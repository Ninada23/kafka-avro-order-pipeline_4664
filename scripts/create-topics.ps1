docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists `
  --topic orders --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists `
  --topic orders-dlq --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
