"""Small helper consumer that prints the contents of the DLQ topic - handy to
have running in its own terminal during the live demo.

Run from the repository root:
    python -m consumer.dlq_viewer
"""
from confluent_kafka import Consumer, KafkaException

from common import config
from common.avro_utils import deserialize, load_schema

DLQ_SCHEMA_PATH = "schemas/dlq.avsc"


def main():
    schema = load_schema(DLQ_SCHEMA_PATH)
    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": "dlq-viewer",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([config.DLQ_TOPIC])
    print(f"[dlq-viewer] watching '{config.DLQ_TOPIC}'")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            record = deserialize(schema, msg.value())
            print(
                f"[DLQ] original_offset={record['offset']} partition={record['partition']} "
                f"reason={record['errorReason']} retries={record['retryCount']} "
                f"at={record['timestamp']}\n       raw_payload={record['originalPayload'][:80]!r}"
            )
    except KeyboardInterrupt:
        print("[dlq-viewer] interrupted")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
