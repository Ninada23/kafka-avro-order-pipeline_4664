"""Produces random Avro-serialized order messages to Kafka.

Run from the repository root:
    python -m producer.producer
    python -m producer.producer -n 50 -i 0.5
"""
import argparse
import random
import uuid
from time import sleep

from confluent_kafka import Producer

from common import config
from common.avro_utils import load_schema, serialize

SCHEMA_PATH = "schemas/order.avsc"
PRODUCTS = [f"Item{i}" for i in range(1, 6)]


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] delivery FAILED: {err}")
    else:
        print(f"[producer] delivered orderId key={msg.key()} -> {msg.topic()}[{msg.partition()}]@{msg.offset()}")


def make_order(seq: int) -> dict:
    return {
        "orderId": str(1000 + seq),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def make_invalid_order(seq: int) -> dict:
    """A well-formed Avro record but with business-invalid data (negative
    price). Decodes fine, so it's the consumer's validate_order() step -
    not Avro - that must catch it."""
    order = make_order(seq)
    order["price"] = -order["price"]
    return order


def main():
    parser = argparse.ArgumentParser(description="Kafka Avro order producer")
    parser.add_argument("-n", "--num-messages", type=int, default=0,
                         help="Number of messages to send (0 = run forever)")
    parser.add_argument("-i", "--interval", type=float, default=config.PRODUCE_INTERVAL_SECONDS,
                         help="Seconds to sleep between messages")
    args = parser.parse_args()

    schema = load_schema(SCHEMA_PATH)
    producer = Producer({"bootstrap.servers": config.BOOTSTRAP_SERVERS})

    seq = 0
    try:
        while args.num_messages == 0 or seq < args.num_messages:
            seq += 1
            roll = random.random()

            if roll < config.CORRUPT_MESSAGE_PROBABILITY:
                # Simulate a permanently malformed message (e.g. a producer bug
                # or upstream data corruption). This is NOT valid Avro, so the
                # consumer can never successfully decode it -> DLQ, no retries.
                order = make_order(seq)
                payload = f"NOT-AVRO-{uuid.uuid4()}".encode("utf-8")
                print(f"[producer] >>> orderId={order['orderId']} product={order['product']:<6} "
                      f"price={order['price']:>7.2f}  [CORRUPT / not valid Avro]")
            elif roll < config.CORRUPT_MESSAGE_PROBABILITY + config.INVALID_DATA_PROBABILITY:
                # Valid Avro, but business-invalid (negative price). The
                # consumer's validation step - not Avro decoding - must catch
                # this -> DLQ, no retries.
                order = make_invalid_order(seq)
                payload = serialize(schema, order)
                print(f"[producer] >>> orderId={order['orderId']} product={order['product']:<6} "
                      f"price={order['price']:>7.2f}  [INVALID / negative price]")
            else:
                order = make_order(seq)
                payload = serialize(schema, order)
                print(f"[producer] orderId={order['orderId']} product={order['product']:<6} "
                      f"price={order['price']:>7.2f}")

            producer.produce(
                config.ORDERS_TOPIC,
                key=order["orderId"].encode("utf-8"),
                value=payload,
                callback=delivery_report,
            )
            producer.poll(0)
            sleep(args.interval)
    except KeyboardInterrupt:
        print("[producer] interrupted, flushing...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
