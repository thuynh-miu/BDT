#!/usr/bin/env bash
# Submit the Spark Structured Streaming job.
# Requires: spark-submit on PATH  (e.g. pip install pyspark==3.5.1)
#           Kafka running at KAFKA_BOOTSTRAP_SERVERS (default localhost:9092)
#           part1/producer.py actively writing to the binance-raw topic

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Kafka connector — must match Spark 3.5.x / Scala 2.12
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

: "${KAFKA_BOOTSTRAP_SERVERS:=localhost:9092}"
: "${KAFKA_TOPIC:=binance-raw}"
: "${WHALE_THRESHOLD_USD:=50000}"
: "${CHECKPOINT_DIR:=/tmp/spark-checkpoints/binance}"

echo "============================================================"
echo "  Binance Kafka → Spark Structured Streaming"
echo "  Kafka brokers : $KAFKA_BOOTSTRAP_SERVERS"
echo "  Topic         : $KAFKA_TOPIC"
echo "  Whale alert   : >= \$$WHALE_THRESHOLD_USD notional"
echo "  Checkpoints   : $CHECKPOINT_DIR"
echo "============================================================"
echo ""

export KAFKA_BOOTSTRAP_SERVERS KAFKA_TOPIC WHALE_THRESHOLD_USD CHECKPOINT_DIR

spark-submit \
  --master "local[*]" \
  --packages "$KAFKA_PKG" \
  --exclude-packages "org.slf4j:slf4j-api,org.xerial.snappy:snappy-java,commons-logging:commons-logging" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.streaming.stopGracefullyOnShutdown=true" \
  --conf "spark.ui.showConsoleProgress=false" \
  "$SCRIPT_DIR/spark_consumer.py"
