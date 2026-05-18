#!/usr/bin/env bash
# Uploads symbol_metadata.csv into the HDFS namenode container so Spark
# can read it as a static broadcast dataset for the enrichment join.
#
# HDFS is provided by the namenode/datanode services in part3/docker-compose.yml.
# No local Hadoop installation is required – all hdfs commands run inside
# the namenode Docker container via 'docker exec'.
#
# Prerequisites:
#   cd part3 && docker compose up -d   (starts HBase + HDFS)
#
# Usage:
#   bash part3/upload_metadata_to_hdfs.sh
#
# Override the namenode container name:
#   NAMENODE_CONTAINER=my-namenode bash part3/upload_metadata_to_hdfs.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_CSV="$SCRIPT_DIR/symbol_metadata.csv"

: "${NAMENODE_CONTAINER:=namenode}"

HDFS_DIR="/user/spark/binance"
HDFS_DEST="$HDFS_DIR/symbol_metadata.csv"
HDFS_URI="hdfs://localhost:9000$HDFS_DEST"

echo "============================================================"
echo "  Upload symbol metadata CSV → HDFS"
echo "  Source    : $LOCAL_CSV"
echo "  Container : $NAMENODE_CONTAINER"
echo "  HDFS path : $HDFS_URI"
echo "============================================================"

DATANODE_CONTAINER="${DATANODE_CONTAINER:-datanode}"

# ── Verify the containers are running ────────────────────────────────────────
for _c in "$NAMENODE_CONTAINER" "$DATANODE_CONTAINER"; do
  if ! docker inspect -f '{{.State.Running}}' "$_c" 2>/dev/null | grep -q true; then
    echo ""
    echo "ERROR: container '$_c' is not running."
    echo "Start the stack first:  cd part3 && docker compose up -d"
    exit 1
  fi
done

# ── Wait for NameNode to leave safe mode ─────────────────────────────────────
echo ""
echo "Waiting for HDFS NameNode to leave safe mode (may take ~30 s)..."
docker exec "$NAMENODE_CONTAINER" hdfs dfsadmin -safemode wait

# ── Create destination directory (namenode namespace op – no data transfer) ──
docker exec "$NAMENODE_CONTAINER" hdfs dfs -mkdir -p "$HDFS_DIR"

# ── Upload CSV ────────────────────────────────────────────────────────────────
# We run 'hdfs dfs -put' from INSIDE the datanode container because the
# datanode is configured to advertise "localhost" as its hostname.
# From within the datanode, "localhost:9866" resolves to itself, so the
# HDFS client can actually write the block there.
docker cp "$LOCAL_CSV" "$DATANODE_CONTAINER:/tmp/symbol_metadata.csv"
docker exec "$DATANODE_CONTAINER" \
  hdfs dfs -put -f /tmp/symbol_metadata.csv "$HDFS_DEST"

echo ""
echo "Done.  Verify:"
echo "  docker exec $NAMENODE_CONTAINER hdfs dfs -ls $HDFS_DIR"
echo "  docker exec $DATANODE_CONTAINER hdfs dfs -cat $HDFS_DEST"
echo ""
echo "Set this env var before running submit_hbase.sh:"
echo "  export SYMBOL_METADATA_PATH=$HDFS_URI"
