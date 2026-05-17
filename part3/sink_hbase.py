"""
Spark Structured Streaming → HBase Sink  (Part 3 – Option A)

Reads raw Binance trade events from the 'binance-raw' Kafka topic and
persists two categories of data into HBase via the Thrift API (happybase):

  Table: binance_trades
    Stores every individual trade event for real-time key-value lookups.
    Row key: {SYMBOL}#{trade_id_zero_padded}
    Use case: point lookup "what was the BTCUSDT trade with id X?"

  Table: binance_vwap
    Stores 1-minute VWAP aggregations per symbol for dashboard queries.
    Row key: {SYMBOL}#{window_start_epoch_seconds_zero_padded}
    Use case: range scan "give me all BTCUSDT VWAP bars from T1 to T2"

Row keys are designed to be lexicographically sortable so HBase range
scans over a symbol's history are efficient without secondary indexes.

Run:
  docker compose up -d            # starts HBase (part3/docker-compose.yml)
  bash submit_hbase.sh

Environment variables:
  HBASE_HOST               default: localhost
  HBASE_PORT               default: 9090   (Thrift)
  KAFKA_BOOTSTRAP_SERVERS  default: localhost:9092
  KAFKA_TOPIC              default: binance-raw
  CHECKPOINT_DIR           default: /tmp/spark-checkpoints/binance-hbase
"""

import os

import happybase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

HBASE_HOST: str              = os.getenv("HBASE_HOST", "localhost")
HBASE_PORT: int              = int(os.getenv("HBASE_PORT", "9090"))
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str             = os.getenv("KAFKA_TOPIC", "binance-raw")
CHECKPOINT_BASE: str         = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/binance-hbase")

TABLE_TRADES = "binance_trades"
TABLE_VWAP   = "binance_vwap"
CF           = b"cf"

# ---------------------------------------------------------------------------
# Binance trade-event schema  (price/qty arrive as strings in the API)
# ---------------------------------------------------------------------------

TRADE_SCHEMA = StructType([
    StructField("e", StringType(),  True),
    StructField("E", LongType(),    True),
    StructField("s", StringType(),  True),
    StructField("t", LongType(),    True),
    StructField("p", StringType(),  True),
    StructField("q", StringType(),  True),
    StructField("b", LongType(),    True),
    StructField("a", LongType(),    True),
    StructField("T", LongType(),    True),
    StructField("m", BooleanType(), True),
    StructField("M", BooleanType(), True),
])

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BinanceHBaseSink")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

# ---------------------------------------------------------------------------
# HBase table provisioning
# ---------------------------------------------------------------------------

def ensure_hbase_tables() -> None:
    """Create HBase tables if they do not already exist."""
    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    existing = {t.decode() for t in conn.tables()}

    for table_name in (TABLE_TRADES, TABLE_VWAP):
        if table_name not in existing:
            conn.create_table(table_name, {CF.decode(): {"max_versions": 1}})
            print(f"[HBase] Created table: {table_name}")
        else:
            print(f"[HBase] Table already exists: {table_name}")

    conn.close()

# ---------------------------------------------------------------------------
# Parsing (same logic as part2, case-sensitivity required)
# ---------------------------------------------------------------------------

def parse_trades(raw_df):
    """
    Kafka value bytes → typed trade columns.
    Filter is applied on the struct field before d.* expansion to avoid
    case-collision on columns e/E, t/T, m/M (Spark is case-insensitive by
    default; caseSensitive=true in the session config resolves this).
    """
    return (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("d"))
        .filter(F.col("d.e") == "trade")
        .select("d.*")
        .withColumn("event_ts", (F.col("T") / 1000).cast("timestamp"))
        .withColumn("price_d",  F.col("p").cast(DoubleType()))
        .withColumn("qty_d",    F.col("q").cast(DoubleType()))
        .withColumn("notional", F.col("price_d") * F.col("qty_d"))
        .withColumn("side",     F.when(F.col("m"), "SELL").otherwise("BUY"))
        .drop("p", "q", "M", "E", "b", "a", "e")
        .withWatermark("event_ts", "30 seconds")
    )

# ---------------------------------------------------------------------------
# foreachBatch writer – raw trades → binance_trades
# ---------------------------------------------------------------------------

def write_trades_to_hbase(batch_df, batch_id: int) -> None:
    """
    Writes every trade in the micro-batch to HBase.
    Row key: {SYMBOL}#{trade_id zero-padded to 20 digits}

    Zero-padding ensures lexicographic sort == numeric sort, enabling
    efficient range scans over a symbol's trade history.
    """
    rows = batch_df.select("s", "t", "T", "price_d", "qty_d", "notional", "side").collect()
    if not rows:
        return

    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    table = conn.table(TABLE_TRADES)

    with table.batch(batch_size=500) as hb_batch:
        for row in rows:
            row_key = f"{row.s}#{str(row.t).zfill(20)}".encode()
            hb_batch.put(row_key, {
                b"cf:symbol":   row.s.encode(),
                b"cf:trade_id": str(row.t).encode(),
                b"cf:price":    str(round(row.price_d, 8)).encode(),
                b"cf:qty":      str(round(row.qty_d, 8)).encode(),
                b"cf:notional": str(round(row.notional, 4)).encode(),
                b"cf:side":     row.side.encode(),
                b"cf:trade_ts": str(row.T).encode(),
            })

    conn.close()
    print(f"[HBase] batch {batch_id}: wrote {len(rows)} trades to {TABLE_TRADES}")

# ---------------------------------------------------------------------------
# VWAP aggregation DataFrame
# ---------------------------------------------------------------------------

def compute_vwap(trades_df):
    """Tumbling 1-minute VWAP per symbol."""
    return (
        trades_df
        .groupBy(F.window("event_ts", "1 minute"), "s")
        .agg(
            (F.sum("notional") / F.sum("qty_d")).alias("vwap_usd"),
            F.sum("qty_d").alias("total_volume"),
            F.sum("notional").alias("total_notional_usd"),
            F.count("*").alias("trade_count"),
            F.min("price_d").alias("low_usd"),
            F.max("price_d").alias("high_usd"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("s").alias("symbol"),
            F.round("vwap_usd", 4).alias("vwap_usd"),
            F.round("total_volume", 6).alias("total_volume"),
            F.round("total_notional_usd", 2).alias("total_notional_usd"),
            "trade_count",
            F.round("low_usd", 4).alias("low_usd"),
            F.round("high_usd", 4).alias("high_usd"),
        )
    )

# ---------------------------------------------------------------------------
# foreachBatch writer – VWAP aggregations → binance_vwap
# ---------------------------------------------------------------------------

def write_vwap_to_hbase(batch_df, batch_id: int) -> None:
    """
    Writes 1-minute VWAP window results to HBase.
    Row key: {SYMBOL}#{window_start_epoch_s zero-padded to 15 digits}

    This layout supports efficient range scans such as:
      "Get all BTCUSDT VWAP bars between 18:00 and 19:00"
    by scanning rows between BTCUSDT#000001700000000 and BTCUSDT#000001703599999.
    """
    rows = batch_df.collect()
    if not rows:
        return

    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    table = conn.table(TABLE_VWAP)

    with table.batch() as hb_batch:
        for row in rows:
            win_epoch = int(row.window_start.timestamp())
            row_key = f"{row.symbol}#{str(win_epoch).zfill(15)}".encode()
            hb_batch.put(row_key, {
                b"cf:symbol":           row.symbol.encode(),
                b"cf:window_start":     str(row.window_start).encode(),
                b"cf:window_end":       str(row.window_end).encode(),
                b"cf:vwap_usd":         str(row.vwap_usd).encode(),
                b"cf:total_volume":     str(row.total_volume).encode(),
                b"cf:total_notional":   str(row.total_notional_usd).encode(),
                b"cf:trade_count":      str(row.trade_count).encode(),
                b"cf:low_usd":          str(row.low_usd).encode(),
                b"cf:high_usd":         str(row.high_usd).encode(),
            })

    conn.close()
    print(f"[HBase] batch {batch_id}: wrote {len(rows)} VWAP rows to {TABLE_VWAP}")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print("=== Binance Kafka → HBase Sink ===")
    print(f"  HBase     : {HBASE_HOST}:{HBASE_PORT}")
    print(f"  Kafka     : {KAFKA_BOOTSTRAP_SERVERS}  topic={KAFKA_TOPIC}")
    print(f"  Tables    : {TABLE_TRADES}, {TABLE_VWAP}")

    ensure_hbase_tables()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    trades_df = parse_trades(raw_df)
    vwap_df   = compute_vwap(trades_df)

    # Query 1: individual trades (append – every trade written once)
    q_trades = (
        trades_df.writeStream
        .outputMode("append")
        .foreachBatch(write_trades_to_hbase)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/trades")
        .queryName("hbase_trades")
        .start()
    )

    # Query 2: VWAP windows (update – window emitted when watermark closes it)
    q_vwap = (
        vwap_df.writeStream
        .outputMode("update")
        .foreachBatch(write_vwap_to_hbase)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/vwap")
        .queryName("hbase_vwap")
        .start()
    )

    print(f"\n  [{q_trades.id}] {q_trades.name}  → {TABLE_TRADES}")
    print(f"  [{q_vwap.id}]   {q_vwap.name}    → {TABLE_VWAP}\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
