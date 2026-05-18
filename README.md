# Binance Big Data Pipeline

Real-time data pipeline that streams live Binance trade data from WebSocket
through Kafka, processes it with Spark Structured Streaming, persists it in
HBase or Hive, and displays it in a Flutter web dashboard.

```
Binance WebSocket
      │
      ▼
 ┌─────────┐
 │ Part 1  │  Kafka Producer  (Zookeeper + Kafka + Kafka-UI)
 └────┬────┘
      │  topic: binance-raw
      ▼
 ┌─────────┐
 │ Part 3  │  Spark Structured Streaming
 │         │    Option A → HBase  (Thrift API)
 │         │    Option B → Hive   (embedded Derby, no extra Docker)
 └────┬────┘
      │  (Option A only feeds Part 4)
      ▼
 ┌─────────┐
 │ Part 4  │  FastAPI bridge  +  Flutter Web dashboard
 └─────────┘  http://localhost:3000
```

---

## Prerequisites

| Tool | Notes |
|---|---|
| Docker + Docker Compose | Kafka, ZooKeeper, HBase containers |
| Python 3.11 | Shared virtual environment at project root |
| Apache Spark (`spark-submit`) | `pip install pyspark==3.5.1` puts it on PATH |
| Flutter SDK | Only for Part 4 dashboard |

---

## Shared Virtual Environment (one-time)

Run once from the **project root**:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r part1/requirements.txt -r part3/requirements.txt
```

> All shell scripts resolve `.venv` relative to the project root automatically.

---

## Part 1 — Kafka Producer (Binance WebSocket → Kafka)

**What it does:** Opens a combined Binance WebSocket stream for the configured
symbols/streams and forwards every raw JSON frame to the `binance-raw` Kafka
topic. Auto-reconnects on any disconnect.

### Step 1 — Start the Kafka stack

```bash
cd part1
bash install.sh          # docker compose up -d
```

Services started:

| Service | URL |
|---|---|
| Kafka broker | `localhost:9092` |
| ZooKeeper | `localhost:2181` |
| Kafka UI | <http://localhost:8080> |

Wait ~30 s for Kafka to become healthy (check with `docker compose ps`).

### Step 2 — Start the producer

```bash
# from project root, with .venv active
source .venv/bin/activate
python part1/producer.py
```

**Environment variables** (optional, set in a `.env` file or exported):

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `binance-raw` | Target topic |
| `BINANCE_SYMBOLS` | `btcusdt,ethusdt` | Comma-separated symbol list |
| `BINANCE_STREAMS` | `trade` | Stream types: `trade`, `ticker`, `depth5`, `kline_1m` |
| `RECONNECT_DELAY_S` | `5` | Seconds between reconnect attempts |

### Verify messages are flowing

```bash
# Consume from the terminal
bash part1/data.sh

# Or open the Kafka UI
open http://localhost:8080
```

This is a **Binance WebSocket trade event** payload. Here's what each field means:

| Field | Value | Description |
|-------|-------|-------------|
| `e` | `"trade"` | Event type |
| `E` | `1778973680727` | Event timestamp (Unix ms) |
| `s` | `"BTCUSDT"` | Trading pair symbol |
| `t` | `6301153275` | Trade ID |
| `p` | `"78235.14000000"` | Price — BTC traded at **$78,235.14** |
| `q` | `"0.00047000"` | Quantity — **0.00047 BTC** traded |
| `T` | `1778973680727` | Trade timestamp (Unix ms, same as `E` here) |
| `m` | `true` | Whether the buyer is the market maker (i.e. the **sell** side initiated the trade) |
| `M` | `true` | Ignore (legacy field, always `true`) |

In short: a sell of 0.00047 BTC at $78,235.14 occurred on the BTC/USDT pair, triggered by a market sell order hitting a resting buy limit order (maker).

---

## Part 3 — Spark Structured Streaming Sink

**What it does:** Consumes `binance-raw` from Kafka in real time, parses trade
events, and writes two tables (`binance_trades` and `binance_vwap`).
Choose **Option A** (HBase) if you also plan to run Part 4; choose **Option B**
(Hive) for SQL analysis without extra containers.

> **Prerequisite:** Part 1 Kafka stack and producer must be running.

### Option A — Sink to HBase (required for Part 4)

#### Step 1 — Start HBase

```bash
cd part3
bash install.sh          # docker compose up -d
```

HBase ports exposed:

| Port | Purpose |
|---|---|
| `9090` | Thrift API (used by Spark + Part 4 API) |
| `9095` | Thrift UI |
| `16010` | HBase Master Web UI |

Wait ~60 s for HBase to pass its health check (`docker compose ps`).

#### Step 2 — Submit the Spark job

```bash
# from project root
bash part3/submit_hbase.sh
```

**Environment variables** (all optional):

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `KAFKA_TOPIC` | `binance-raw` | |
| `HBASE_HOST` | `localhost` | |
| `HBASE_PORT` | `9090` | Thrift port |
| `CHECKPOINT_DIR` | `/tmp/spark-checkpoints/binance-hbase` | |

HBase tables created automatically:

- **`binance_trades`** — every raw trade event; row key: `{SYMBOL}#{trade_id}`
- **`binance_vwap`** — 1-minute VWAP bars; row key: `{SYMBOL}#{epoch_seconds}`

---

### Option B — Sink to Hive (no extra Docker service)

Spark uses an embedded Derby metastore. No additional containers required.

```bash
# from project root
bash part3/submit_hive.sh
```

**Environment variables** (all optional):

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `KAFKA_TOPIC` | `binance-raw` | |
| `HIVE_WAREHOUSE_DIR` | `/tmp/hive-warehouse/binance` | |
| `CHECKPOINT_DIR` | `/tmp/spark-checkpoints/binance-hive` | |

Hive tables created:

- **`binance_trades`** — partitioned by `symbol`, `trade_date`
- **`binance_vwap`** — partitioned by `symbol`

Sample Spark SQL queries:

```sql
-- Latest BTCUSDT trades
SELECT * FROM binance_trades
WHERE symbol = 'BTCUSDT' AND trade_date = '2026-05-17'
ORDER BY event_ts DESC LIMIT 100;

-- 1-minute VWAP chart data
SELECT window_start, vwap_usd, trade_count
FROM binance_vwap
WHERE symbol = 'ETHUSDT'
ORDER BY window_start DESC LIMIT 60;
```

---

## Part 4 — REST API + Flutter Dashboard

**What it does:** A FastAPI service reads live data from HBase (Part 3 Option A)
and exposes clean JSON endpoints consumed every 5 s by a Flutter Web dashboard.

> **Prerequisites:** Part 1 Kafka stack, Part 1 producer, and Part 3 Option A
> HBase sink must all be running.

### One-time Setup

```bash
cd part4
bash setup.sh
```

This installs the Python API dependencies into the shared `.venv` and
initialises the Flutter web project (skipped on subsequent runs).

### Running the Stack

Open **two terminals**:

**Terminal A — API server**

```bash
bash part4/api/run.sh
```

| Item | Value |
|---|---|
| API base URL | <http://localhost:8000> |
| Interactive docs | <http://localhost:8000/docs> |
| Health check | <http://localhost:8000/health> |

**Terminal B — Flutter dashboard**

```bash
cd part4/dashboard
flutter run -d web-server --web-port 3000
```

Then open **<http://localhost:3000>** in your browser.

### API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness probe; checks HBase connectivity |
| `GET` | `/api/symbols` | List of tracked symbols |
| `GET` | `/api/vwap/{symbol}?minutes=60` | 1-min VWAP bars for last N minutes |
| `GET` | `/api/trades/{symbol}?limit=30` | Most recent raw trades |
| `GET` | `/api/summary` | Latest VWAP bar per symbol |

**Environment variables** (API server):

| Variable | Default | Description |
|---|---|---|
| `HBASE_HOST` | `localhost` | |
| `HBASE_PORT` | `9090` | |
| `SYMBOLS` | `BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT` | Comma-separated |
| `PORT` | `8000` | API listen port |

---

## Full Stack Startup Order

```
1. cd part1 && bash install.sh          # Kafka + ZooKeeper + Kafka-UI
2. cd part3 && bash install.sh          # HBase  (Option A only)
3. source .venv/bin/activate
   python part1/producer.py             # Binance → Kafka
4. bash part3/submit_hbase.sh           # Kafka → HBase  (Option A)
5. bash part4/api/run.sh                # FastAPI bridge
6. cd part4/dashboard && flutter run -d web-server --web-port 3000
7. open http://localhost:3000
```

## Teardown

```bash
cd part1 && docker compose down
cd part3 && docker compose down         # if you ran Option A
```