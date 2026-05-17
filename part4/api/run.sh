#!/usr/bin/env bash
# Part 4 – Start the HBase → REST API bridge
# Prerequisites:
#   1. part3 HBase running  (cd part3 && docker compose up -d)
#   2. part3 sink running   (bash part3/submit_hbase.sh)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PYTHON="$SCRIPT_DIR/../../.venv/bin/python"

: "${HBASE_HOST:=localhost}"
: "${HBASE_PORT:=9090}"
: "${PORT:=8000}"

export HBASE_HOST HBASE_PORT

echo "=================================================="
echo "  Binance Dashboard API"
echo "  HBase : $HBASE_HOST:$HBASE_PORT"
echo "  API   : http://localhost:$PORT"
echo "  Docs  : http://localhost:$PORT/docs"
echo "=================================================="
echo ""

"$VENV_PYTHON" -m uvicorn main:app \
  --host 0.0.0.0 \
  --port "$PORT" \
  --reload \
  --app-dir "$SCRIPT_DIR"
