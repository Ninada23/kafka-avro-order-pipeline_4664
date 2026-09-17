#!/usr/bin/env bash
set -euo pipefail

# On Windows Git Bash, disable MSYS path conversion so container paths like
# /opt/kafka/bin/... aren't rewritten into a Windows host path.
export MSYS_NO_PATHCONV=1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic orders --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic orders-dlq --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
