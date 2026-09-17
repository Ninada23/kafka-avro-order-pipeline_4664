"""Consumes Avro order messages, aggregates a running average, and routes
messages that fail permanently (bad Avro / invalid data / exhausted retries)
to a Dead Letter Queue topic.

Run from the repository root:
    python -m consumer.consumer
"""
import datetime
import random
import time

from confluent_kafka import Consumer, KafkaException, Producer

from common import config
from common.aggregator import ProductAggregator
from common.avro_utils import deserialize, load_schema, serialize

ORDER_SCHEMA_PATH = "schemas/order.avsc"
DLQ_SCHEMA_PATH = "schemas/dlq.avsc"


class TransientError(Exception):
    """A temporary/recoverable failure (e.g. a downstream service hiccup)."""


def validate_order(order: dict):
    if not order.get("orderId"):
        raise ValueError("orderId is missing/empty")
    if not order.get("product"):
        raise ValueError("product is missing/empty")
    if order.get("price") is None or order["price"] <= 0:
        raise ValueError(f"invalid price: {order.get('price')}")


def process_order(order: dict, aggregator: ProductAggregator):
    """Simulated business logic. Fails transiently at random to exercise the
    retry path (imagine this calling a flaky downstream pricing/inventory
    service)."""
    if random.random() < config.TRANSIENT_FAILURE_PROBABILITY:
        raise TransientError("simulated downstream service timeout")

    overall_avg, product_avg = aggregator.update(order["product"], order["price"])
    print(
        f"[consumer] OK orderId={order['orderId']:<6} product={order['product']:<6} "
        f"price={order['price']:>7.2f} | running avg overall={overall_avg:>7.2f} "
        f"running avg {order['product']}={product_avg:>7.2f}"
    )


def send_to_dlq(dlq_producer: Producer, dlq_schema, msg, error_reason: str, retry_count: int):
    dlq_record = {
        "originalTopic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "errorReason": error_reason,
        "retryCount": retry_count,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "originalPayload": msg.value() or b"",
    }
    dlq_producer.produce(
        config.DLQ_TOPIC,
        key=msg.key(),
        value=serialize(dlq_schema, dlq_record),
    )
    dlq_producer.flush()
    print(f"[consumer] >>> DLQ offset={msg.offset()} reason={error_reason}")


def handle_message(msg, order_schema, dlq_schema, dlq_producer, aggregator):
    # 1. Deserialize. If this fails, the payload isn't valid Avro at all -
    #    retrying won't help, so it's a permanent failure straight to the DLQ.
    try:
        order = deserialize(order_schema, msg.value())
    except Exception as exc:
        send_to_dlq(dlq_producer, dlq_schema, msg, f"avro_deserialize_error: {exc}", retry_count=0)
        return

    # 2. Validate. Bad data is also a permanent failure.
    try:
        validate_order(order)
    except ValueError as exc:
        send_to_dlq(dlq_producer, dlq_schema, msg, f"validation_error: {exc}", retry_count=0)
        return

    # 3. Process, with retry + exponential backoff for transient failures.
    attempt = 0
    while True:
        try:
            process_order(order, aggregator)
            return
        except TransientError as exc:
            attempt += 1
            if attempt > config.MAX_RETRIES:
                send_to_dlq(dlq_producer, dlq_schema, msg, f"max_retries_exceeded: {exc}", retry_count=attempt - 1)
                return
            backoff = config.RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            print(
                f"[consumer] retrying orderId={order['orderId']} "
                f"(attempt {attempt}/{config.MAX_RETRIES}) after {exc}; backoff={backoff:.1f}s"
            )
            time.sleep(backoff)


def main():
    order_schema = load_schema(ORDER_SCHEMA_PATH)
    dlq_schema = load_schema(DLQ_SCHEMA_PATH)

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    dlq_producer = Producer({"bootstrap.servers": config.BOOTSTRAP_SERVERS})
    aggregator = ProductAggregator()

    consumer.subscribe([config.ORDERS_TOPIC])
    print(f"[consumer] subscribed to '{config.ORDERS_TOPIC}' as group '{config.CONSUMER_GROUP}'")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            handle_message(msg, order_schema, dlq_schema, dlq_producer, aggregator)
            # Commit only after the message has been fully handled (processed
            # or dead-lettered), so a crash mid-retry replays the message
            # rather than silently dropping it.
            consumer.commit(msg)
    except KeyboardInterrupt:
        print("[consumer] interrupted")
    finally:
        consumer.close()
        dlq_producer.flush()


if __name__ == "__main__":
    main()
