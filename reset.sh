#!/bin/bash
# reset.sh — cleans Kafka topics + Spark checkpoints

set -e

echo "🗑️  Deleting Kafka topics..."
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete --topic uber_topic 2>/dev/null || true

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete --topic uber_output 2>/dev/null || true

echo "🆕 Recreating Kafka topics..."
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic uber_topic --partitions 1 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic uber_output --partitions 1 --replication-factor 1

echo "🧹 Clearing Spark checkpoints..."
rm -rf /tmp/uber_stream_output

echo "✅ Reset complete. Ready for a clean run!"