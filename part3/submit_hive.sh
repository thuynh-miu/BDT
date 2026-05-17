#!/usr/bin/env bash
# Option B: Spark → Hive (embedded Derby metastore — no extra Docker service)
# Prerequisites:
#   1. part1 Kafka stack running  (cd part1 && docker compose up -d)
#   2. part1 producer running     (python part1/producer.py)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

: "${KAFKA_BOOTSTRAP_SERVERS:=localhost:9092}"
: "${KAFKA_TOPIC:=binance-raw}"
: "${HIVE_WAREHOUSE_DIR:=/tmp/hive-warehouse/binance}"
: "${CHECKPOINT_DIR:=/tmp/spark-checkpoints/binance-hive}"

mkdir -p "$HIVE_WAREHOUSE_DIR"

echo "============================================================"
echo "  Binance Kafka → Hive Sink  (Option B)"
echo "  Kafka     : $KAFKA_BOOTSTRAP_SERVERS  topic=$KAFKA_TOPIC"
echo "  Warehouse : $HIVE_WAREHOUSE_DIR"
echo "============================================================"
echo ""

export KAFKA_BOOTSTRAP_SERVERS KAFKA_TOPIC HIVE_WAREHOUSE_DIR CHECKPOINT_DIR

spark-submit \
  --master "local[*]" \
  --packages "$KAFKA_PKG" \
  --exclude-packages "org.slf4j:slf4j-api,org.xerial.snappy:snappy-java,commons-logging:commons-logging" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.sql.warehouse.dir=$HIVE_WAREHOUSE_DIR" \
  --conf "spark.streaming.stopGracefullyOnShutdown=true" \
  --conf "spark.ui.showConsoleProgress=false" \
  "$SCRIPT_DIR/sink_hive.py"
