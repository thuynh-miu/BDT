#!/usr/bin/env bash
# Option A: Spark → HBase
# Prerequisites:
#   1. part1 Kafka stack running  (cd part1 && docker compose up -d)
#   2. HBase + HDFS running       (cd part3 && docker compose up -d)
#   3. Metadata uploaded to HDFS  (bash part3/upload_metadata_to_hdfs.sh)
#   4. part1 producer running     (python part1/producer.py)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

: "${KAFKA_BOOTSTRAP_SERVERS:=localhost:9092}"
: "${KAFKA_TOPIC:=binance-raw}"
: "${HBASE_HOST:=localhost}"
: "${HBASE_PORT:=9090}"
: "${CHECKPOINT_DIR:=file:///tmp/spark-checkpoints/binance-hbase}"

# Static symbol-metadata CSV stored in HDFS (part3/docker-compose.yml).
# Upload once with:  bash part3/upload_metadata_to_hdfs.sh
# Falls back to the local file:// path if the env var is not set.
_HDFS_METADATA="hdfs://localhost:9000/user/spark/binance/symbol_metadata.csv"
: "${SYMBOL_METADATA_PATH:=$_HDFS_METADATA}"

echo "============================================================"
echo "  Binance Kafka → HBase Sink  (Option A, with enrichment)"
echo "  Kafka    : $KAFKA_BOOTSTRAP_SERVERS  topic=$KAFKA_TOPIC"
echo "  HBase    : $HBASE_HOST:$HBASE_PORT  (Thrift)"
echo "  Metadata : $SYMBOL_METADATA_PATH"
echo "============================================================"
echo ""

export KAFKA_BOOTSTRAP_SERVERS KAFKA_TOPIC HBASE_HOST HBASE_PORT CHECKPOINT_DIR \
       SYMBOL_METADATA_PATH

VENV_PYTHON="$SCRIPT_DIR/../.venv/bin/python"
export PYSPARK_PYTHON="$VENV_PYTHON"
export PYSPARK_DRIVER_PYTHON="$VENV_PYTHON"

spark-submit \
  --master "local[*]" \
  --packages "$KAFKA_PKG" \
  --exclude-packages "org.slf4j:slf4j-api,org.xerial.snappy:snappy-java,commons-logging:commons-logging" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.streaming.stopGracefullyOnShutdown=true" \
  --conf "spark.ui.showConsoleProgress=false" \
  --conf "spark.hadoop.fs.defaultFS=hdfs://localhost:9000" \
  --conf "spark.hadoop.dfs.client.use.datanode.hostname=true" \
  "$SCRIPT_DIR/sink_hbase.py"
