"""
Spark Structured Streaming → Hive Sink  (Part 3 – Option B)

Reads raw Binance trade events from the 'binance-raw' Kafka topic and
appends structured summaries to two Hive-managed tables using Spark's
built-in Hive support (no external Hive Metastore service required —
Spark uses an embedded Derby metastore stored in ./metastore_db).

  Table: binance_trades
    Every individual trade event, partitioned by symbol and trade date.
    Suitable for ad-hoc SQL analysis over historical trade data.
    Query example:
      SELECT * FROM binance_trades
      WHERE symbol = 'BTCUSDT' AND trade_date = '2026-05-16'
      ORDER BY event_ts DESC LIMIT 100;

  Table: binance_vwap
    1-minute VWAP window summaries per symbol, partitioned by symbol.
    Suitable for time-series charting and summary dashboards.
    Query example:
      SELECT window_start, vwap_usd, trade_count
      FROM binance_vwap
      WHERE symbol = 'ETHUSDT'
      ORDER BY window_start DESC LIMIT 60;

Run:
  bash submit_hive.sh      # no extra Docker services needed

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS  default: localhost:9092
  KAFKA_TOPIC              default: binance-raw
  HIVE_WAREHOUSE_DIR       default: /tmp/hive-warehouse/binance
  CHECKPOINT_DIR           default: /tmp/spark-checkpoints/binance-hive
"""

import os

from pyspark.sql import DataFrame, SparkSession
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

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str             = os.getenv("KAFKA_TOPIC", "binance-raw")
WAREHOUSE_DIR: str           = os.getenv("HIVE_WAREHOUSE_DIR", "/tmp/hive-warehouse/binance")
CHECKPOINT_BASE: str         = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/binance-hive")

TABLE_TRADES = "binance_trades"
TABLE_VWAP   = "binance_vwap"

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
# Spark session with Hive support
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BinanceHiveSink")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # Built-in Hive metastore — no external service required.
        # Data is stored as Parquet under WAREHOUSE_DIR.
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .enableHiveSupport()
        .getOrCreate()
    )

# ---------------------------------------------------------------------------
# Hive table DDL
# ---------------------------------------------------------------------------

def create_hive_tables(spark: SparkSession) -> None:
    """Create Hive tables if they do not exist."""

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_TRADES} (
            trade_id    BIGINT,
            event_ts    TIMESTAMP,
            price_d     DOUBLE,
            qty_d       DOUBLE,
            notional    DOUBLE,
            side        STRING,
            ingested_at TIMESTAMP
        )
        PARTITIONED BY (symbol STRING, trade_date DATE)
        STORED AS PARQUET
        TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """)
    print(f"[Hive] Table ready: {TABLE_TRADES}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_VWAP} (
            window_start       TIMESTAMP,
            window_end         TIMESTAMP,
            vwap_usd           DOUBLE,
            total_volume       DOUBLE,
            total_notional_usd DOUBLE,
            trade_count        BIGINT,
            low_usd            DOUBLE,
            high_usd           DOUBLE,
            ingested_at        TIMESTAMP
        )
        PARTITIONED BY (symbol STRING)
        STORED AS PARQUET
        TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """)
    print(f"[Hive] Table ready: {TABLE_VWAP}")

# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_trades(raw_df):
    """
    Kafka value bytes → typed trade columns.
    caseSensitive=true is required because the Binance schema uses both
    upper- and lower-case single-letter field names (e/E, t/T, m/M).
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

def write_trades_to_hive(batch_df: DataFrame, batch_id: int) -> None:
    """
    Appends a micro-batch of raw trades to the Hive binance_trades table.
    Partitioned by symbol and trade_date so queries that filter on either
    dimension skip unrelated partition files entirely.
    """
    if batch_df.isEmpty():
        return

    # Column order must match the CREATE TABLE DDL:
    # non-partition cols first, then partition cols (symbol, trade_date).
    # insertInto() honours the table's registered HiveFileFormat — no conflict.
    prepared = (
        batch_df
        .select(
            F.col("t").alias("trade_id"),
            "event_ts",
            "price_d",
            "qty_d",
            "notional",
            "side",
            F.current_timestamp().alias("ingested_at"),
            F.col("s").alias("symbol"),                     # partition col 1
            F.to_date("event_ts").alias("trade_date"),      # partition col 2
        )
    )

    prepared.write.mode("append").insertInto(TABLE_TRADES)

    count = prepared.count()
    print(f"[Hive] batch {batch_id}: appended {count} rows → {TABLE_TRADES}")

# ---------------------------------------------------------------------------
# VWAP aggregation
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
# foreachBatch writer – VWAP windows → binance_vwap
# ---------------------------------------------------------------------------

def write_vwap_to_hive(batch_df: DataFrame, batch_id: int) -> None:
    """
    Appends 1-minute VWAP window summaries to the Hive binance_vwap table.

    Output mode is 'update', so a window may appear in multiple micro-batches
    as late data arrives within the 30-second watermark. Each row carries an
    ingested_at timestamp so consumers can always select the latest entry
    per (symbol, window_start):

      SELECT symbol, window_start, vwap_usd
      FROM (
        SELECT *, ROW_NUMBER() OVER
          (PARTITION BY symbol, window_start ORDER BY ingested_at DESC) rn
        FROM binance_vwap
      ) WHERE rn = 1;
    """
    if batch_df.isEmpty():
        return

    # Column order must match CREATE TABLE DDL:
    # non-partition cols first, then partition col (symbol).
    prepared = (
        batch_df
        .select(
            "window_start",
            "window_end",
            "vwap_usd",
            "total_volume",
            "total_notional_usd",
            "trade_count",
            "low_usd",
            "high_usd",
            F.current_timestamp().alias("ingested_at"),
            "symbol",                                       # partition col
        )
    )

    prepared.write.mode("append").insertInto(TABLE_VWAP)

    count = prepared.count()
    print(f"[Hive] batch {batch_id}: appended {count} VWAP rows → {TABLE_VWAP}")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print("=== Binance Kafka → Hive Sink ===")
    print(f"  Kafka     : {KAFKA_BOOTSTRAP_SERVERS}  topic={KAFKA_TOPIC}")
    print(f"  Warehouse : {WAREHOUSE_DIR}")
    print(f"  Tables    : {TABLE_TRADES}, {TABLE_VWAP}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    # Enable dynamic partition insertion without listing all partitions upfront.
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    create_hive_tables(spark)

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

    # Query 1: raw individual trades (append mode — each trade written once)
    q_trades = (
        trades_df.writeStream
        .outputMode("append")
        .foreachBatch(write_trades_to_hive)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/trades")
        .queryName("hive_trades")
        .start()
    )

    # Query 2: VWAP summaries (update mode — emits as watermark closes windows)
    q_vwap = (
        vwap_df.writeStream
        .outputMode("update")
        .foreachBatch(write_vwap_to_hive)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/vwap")
        .queryName("hive_vwap")
        .start()
    )

    print(f"\n  [{q_trades.id}] {q_trades.name}  → {TABLE_TRADES}")
    print(f"  [{q_vwap.id}]   {q_vwap.name}    → {TABLE_VWAP}\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
