# kafka-avro-order-pipeline

Kafka-based order pipeline with **Avro serialization**, **real-time running-average
aggregation**, **retry logic for transient failures**, and a **Dead Letter Queue
(DLQ)** for permanently failed messages.

## Architecture

```
producer.py --(Avro "Order")--> [orders topic] --> consumer.py --> running average
                                                          |
                                                          | (parse error / bad data /
                                                          |  retries exhausted)
                                                          v
                                                [orders-dlq topic] <-- dlq_viewer.py
```

- **Producer** (`producer/producer.py`): generates random orders, serializes them
  with the `order.avsc` schema, and publishes to the `orders` topic. It
  occasionally emits a deliberately corrupt (non-Avro) payload to simulate a
  malformed upstream message.
- **Consumer** (`consumer/consumer.py`): deserializes each message, validates
  it, then "processes" it (a stand-in for calling a downstream service, which
  randomly fails to simulate a transient error). It maintains a running
  average price overall and per-product, retries transient failures with
  exponential backoff, and dead-letters anything that can't be decoded,
  fails validation, or exhausts its retries.
- **DLQ viewer** (`consumer/dlq_viewer.py`): a second consumer that just
  prints whatever lands in `orders-dlq`, for visibility during the demo.

No Schema Registry is used — producer and consumer both read the same
`schemas/order.avsc` file directly, which satisfies the "Avro serialization"
requirement without extra infrastructure. The DLQ envelope (`schemas/dlq.avsc`)
is Avro too, wrapping the original raw bytes plus failure metadata.

## Prerequisites

- Docker Desktop (for Kafka)
- Python 3.10+

## Setup

```powershell
# 1. Start Kafka (single-node, KRaft mode, no Zookeeper)
docker compose up -d

# 2. Create topics explicitly (orders: 3 partitions, orders-dlq: 1 partition)
.\scripts\create-topics.ps1

# 3. Python environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Running the demo

Open three terminals (all from the repo root, with the venv activated):

```powershell
# Terminal 1 - consumer (aggregation + retry + DLQ routing)
python -m consumer.consumer

# Terminal 2 - DLQ viewer
python -m consumer.dlq_viewer

# Terminal 3 - producer
python -m producer.producer
```

You should see:
- Terminal 1 printing `OK orderId=... running avg overall=...` for successful
  messages, occasional `retrying orderId=...` lines for simulated transient
  failures, and `>>> DLQ offset=...` lines when retries are exhausted or a
  message is unprocessable.
- Terminal 2 printing the same dead-lettered messages with their failure
  reason and retry count.

Stop everything with `Ctrl+C` in each terminal, then `docker compose down`
(add `-v` to also wipe the topic data).

### Sending a fixed batch instead of an infinite stream

```powershell
python -m producer.producer -n 50 -i 0.5   # 50 messages, 0.5s apart
```

## How failures are simulated (and why)

| Knob (env var) | Default | Effect |
|---|---|---|
| `CORRUPT_MESSAGE_PROBABILITY` | 0.1 | Producer sends a non-Avro payload -> consumer fails to deserialize -> **permanent failure, straight to DLQ, no retries** |
| `TRANSIENT_FAILURE_PROBABILITY` | 0.3 | Consumer's simulated downstream call randomly throws `TransientError` -> **retried with exponential backoff** up to `MAX_RETRIES`, then DLQ if still failing |
| `MAX_RETRIES` | 3 | Max retry attempts before a transient failure is treated as permanent |
| `RETRY_BACKOFF_BASE_SECONDS` | 1 | Backoff doubles each attempt: 1s, 2s, 4s, ... |

Set these before launching a process, e.g. (PowerShell):

```powershell
$env:TRANSIENT_FAILURE_PROBABILITY = "0.6"
python -m consumer.consumer
```

Validation failures (empty `orderId`/`product`, non-positive `price`) are
also treated as permanent and go straight to the DLQ, since retrying
wouldn't change bad data.

## Project layout

```
schemas/
  order.avsc        # Order record, per the assignment spec
  dlq.avsc           # DLQ envelope (original bytes + failure metadata)
common/
  config.py          # Kafka/topic/retry settings (env-var overridable)
  avro_utils.py       # schemaless Avro serialize/deserialize helpers
  aggregator.py       # RunningAverage / ProductAggregator
producer/
  producer.py         # order producer
consumer/
  consumer.py         # order consumer: retry + aggregation + DLQ
  dlq_viewer.py        # prints DLQ contents
scripts/
  create-topics.ps1 / .sh
docker-compose.yml    # single-node Kafka (KRaft)
```

## Design notes

- **Retry vs. DLQ boundary**: a message is retried only when the failure is
  classified as *transient* (a simulated downstream call). Anything that
  can't be decoded as Avro, or fails field validation, is treated as
  *permanent* and dead-lettered immediately — retrying a structurally bad
  message would just fail the same way every time.
- **At-least-once processing**: the consumer disables auto-commit and only
  commits an offset after the message has been fully handled (processed
  successfully or dead-lettered), so a crash mid-retry replays the message
  instead of silently losing it.
- **Running average**: computed in-memory per consumer process
  (`common/aggregator.py`), both overall and per product. For a
  multi-partition/multi-consumer deployment this would need a shared store
  (e.g. Kafka Streams state store, or an external cache) to merge partial
  averages — out of scope for this assignment but worth mentioning in the
  demo/writeup.
