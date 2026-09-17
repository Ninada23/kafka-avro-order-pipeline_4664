"""Central configuration, overridable via environment variables."""
import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "orders-dlq")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "order-consumer-group")

# Retry behaviour for transient processing failures.
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_BASE_SECONDS = float(os.getenv("RETRY_BACKOFF_BASE_SECONDS", "1"))

# Failure simulation knobs (tune these to make transient/permanent
# failures more or less frequent during a demo).
TRANSIENT_FAILURE_PROBABILITY = float(os.getenv("TRANSIENT_FAILURE_PROBABILITY", "0.3"))
CORRUPT_MESSAGE_PROBABILITY = float(os.getenv("CORRUPT_MESSAGE_PROBABILITY", "0.1"))

PRODUCE_INTERVAL_SECONDS = float(os.getenv("PRODUCE_INTERVAL_SECONDS", "1"))
