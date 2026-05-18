"""
Spark Structured Streaming → HBase Sink  (Part 3 – Option A)

Reads raw Binance trade events from the 'binance-raw' Kafka topic,
enriches each micro-batch with a static symbol-metadata dataset loaded
from HDFS via a Spark SQL join, then persists two categories of data
into HBase via the Thrift API (happybase):

  Static dataset  (HDFS CSV → broadcast DataFrame)
    symbol_metadata.csv: per-symbol metadata (base/quote asset, category,
    market-cap tier, description).  Loaded once at startup, cached in
    memory, and joined with every streaming micro-batch.

  Table: binance_trades
    Stores every individual trade event enriched with metadata columns.
    Row key: {SYMBOL}#{trade_id_zero_padded}
    Extra columns: cf:base_asset, cf:quote_asset, cf:asset_category,
                   cf:market_cap_tier, cf:description
    Use case: point lookup "what was the BTCUSDT trade with id X?"

  Table: binance_vwap
    Stores 1-minute VWAP aggregations enriched with category/tier.
    Row key: {SYMBOL}#{window_start_epoch_seconds_zero_padded}
    Extra columns: cf:asset_category, cf:market_cap_tier
    Use case: range scan "give me all BTCUSDT VWAP bars from T1 to T2"

Row keys are lexicographically sortable so HBase range scans over a
symbol's history are efficient without secondary indexes.

Run:
  # 1. Start HBase
  docker compose up -d                         # part3/docker-compose.yml

  # 2. Upload static metadata to HDFS (once)
  bash part3/upload_metadata_to_hdfs.sh
  export SYMBOL_METADATA_PATH=hdfs:///user/spark/binance/symbol_metadata.csv

  # 3. Start the sink
  bash submit_hbase.sh

Environment variables:
  HBASE_HOST               default: localhost
  HBASE_PORT               default: 9090   (Thrift)
  KAFKA_BOOTSTRAP_SERVERS  default: localhost:9092
  KAFKA_TOPIC              default: binance-raw
  CHECKPOINT_DIR           default: /tmp/spark-checkpoints/binance-hbase
  SYMBOL_METADATA_PATH     default: file://<script_dir>/symbol_metadata.csv
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

# Static symbol-metadata CSV.  Defaults to the local file next to this
# script so the sink works out-of-the-box without HDFS.  Set to an
# hdfs:// URI after running upload_metadata_to_hdfs.sh for production use.
_DEFAULT_METADATA_PATH: str  = (
    "file://" + os.path.join(os.path.dirname(os.path.abspath(__file__)), "symbol_metadata.csv")
)
SYMBOL_METADATA_PATH: str   = os.getenv("SYMBOL_METADATA_PATH", _DEFAULT_METADATA_PATH)

TABLE_TRADES = "binance_trades"
TABLE_VWAP   = "binance_vwap"
CF           = b"cf"

# ---------------------------------------------------------------------------
# Static symbol-metadata schema
# ---------------------------------------------------------------------------

METADATA_SCHEMA = StructType([
    StructField("symbol",          StringType(), False),
    StructField("base_asset",      StringType(), True),
    StructField("quote_asset",     StringType(), True),
    StructField("asset_category",  StringType(), True),
    StructField("market_cap_tier", StringType(), True),
    StructField("description",     StringType(), True),
])

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
# Static dataset loader – symbol metadata from HDFS (or local fallback)
# ---------------------------------------------------------------------------

def load_symbol_metadata(spark: SparkSession):
    """
    Read the static symbol-metadata CSV from HDFS (or a local file:// URI),
    cache it in memory, and return the DataFrame.

    The DataFrame is intentionally kept as a regular (non-streaming)
    DataFrame so it can be referenced directly inside foreachBatch
    closures and joined with each micro-batch using Spark SQL.
    """
    df = (
        spark.read
        .option("header", "true")
        .schema(METADATA_SCHEMA)
        .csv(SYMBOL_METADATA_PATH)
    )
    df.cache()
    count = df.count()
    print(f"[Metadata] Loaded {count} symbol records from: {SYMBOL_METADATA_PATH}")
    df.show(truncate=False)
    return df

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
# foreachBatch writer factories – enrich via Spark SQL join, then → HBase
# ---------------------------------------------------------------------------

def make_trade_writer(symbol_meta_df):
    """
    Returns a foreachBatch function that:
      1. Broadcast-joins the cached metadata DataFrame with each micro-batch.
      2. Writes the enriched rows to the binance_trades HBase table.

    Uses the DataFrame API (not temp views) to avoid Spark session-scope
    mismatches that occur when PySpark wraps the batch DataFrame inside
    foreachBatch with a different session context.

    Row key: {SYMBOL}#{trade_id zero-padded to 20 digits}
    Zero-padding ensures lexicographic sort == numeric sort, enabling
    efficient range scans over a symbol's trade history.
    """
    def write_trades_to_hbase(batch_df, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        # ── Broadcast join enrichment ────────────────────────────────────
        enriched_df = batch_df.join(
            F.broadcast(symbol_meta_df),
            batch_df["s"] == symbol_meta_df["symbol"],
            "left",
        ).select(
            batch_df["s"],
            batch_df["t"],
            batch_df["T"],
            batch_df["price_d"],
            batch_df["qty_d"],
            batch_df["notional"],
            batch_df["side"],
            F.coalesce(symbol_meta_df["base_asset"],      F.lit("UNKNOWN")).alias("base_asset"),
            F.coalesce(symbol_meta_df["quote_asset"],     F.lit("UNKNOWN")).alias("quote_asset"),
            F.coalesce(symbol_meta_df["asset_category"],  F.lit("Unknown")).alias("asset_category"),
            F.coalesce(symbol_meta_df["market_cap_tier"], F.lit("Unknown")).alias("market_cap_tier"),
            F.coalesce(symbol_meta_df["description"],     F.lit("")).alias("description"),
        )

        rows = enriched_df.collect()
        if not rows:
            return

        # ── Write enriched rows to HBase ────────────────────────────────
        conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        table = conn.table(TABLE_TRADES)

        with table.batch(batch_size=500) as hb_batch:
            for row in rows:
                row_key = f"{row.s}#{str(row.t).zfill(20)}".encode()
                hb_batch.put(row_key, {
                    b"cf:symbol":          row.s.encode(),
                    b"cf:trade_id":        str(row.t).encode(),
                    b"cf:price":           str(round(row.price_d, 8)).encode(),
                    b"cf:qty":             str(round(row.qty_d, 8)).encode(),
                    b"cf:notional":        str(round(row.notional, 4)).encode(),
                    b"cf:side":            row.side.encode(),
                    b"cf:trade_ts":        str(row.T).encode(),
                    b"cf:base_asset":      row.base_asset.encode(),
                    b"cf:quote_asset":     row.quote_asset.encode(),
                    b"cf:asset_category":  row.asset_category.encode(),
                    b"cf:market_cap_tier": row.market_cap_tier.encode(),
                    b"cf:description":     row.description.encode(),
                })

        conn.close()
        print(
            f"[HBase] batch {batch_id}: wrote {len(rows)} enriched trades "
            f"to {TABLE_TRADES}"
        )

    return write_trades_to_hbase

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

def make_vwap_writer(symbol_meta_df):
    """
    Returns a foreachBatch function that:
      1. Broadcast-joins the cached metadata DataFrame with each VWAP batch.
      2. Writes the enriched VWAP rows to the binance_vwap HBase table.

    Uses the DataFrame API (not temp views) to avoid Spark session-scope
    mismatches that occur when PySpark wraps the batch DataFrame inside
    foreachBatch with a different session context.

    Row key: {SYMBOL}#{window_start_epoch_s zero-padded to 15 digits}
    This layout supports efficient range scans such as:
      "Get all BTCUSDT VWAP bars between 18:00 and 19:00"
    by scanning rows between BTCUSDT#000001700000000 and BTCUSDT#000001703599999.
    """
    def write_vwap_to_hbase(batch_df, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        # ── Broadcast join enrichment ────────────────────────────────────
        enriched_df = batch_df.join(
            F.broadcast(symbol_meta_df),
            batch_df["symbol"] == symbol_meta_df["symbol"],
            "left",
        ).select(
            batch_df["window_start"],
            batch_df["window_end"],
            batch_df["symbol"],
            batch_df["vwap_usd"],
            batch_df["total_volume"],
            batch_df["total_notional_usd"],
            batch_df["trade_count"],
            batch_df["low_usd"],
            batch_df["high_usd"],
            F.coalesce(symbol_meta_df["asset_category"],  F.lit("Unknown")).alias("asset_category"),
            F.coalesce(symbol_meta_df["market_cap_tier"], F.lit("Unknown")).alias("market_cap_tier"),
        )

        rows = enriched_df.collect()
        if not rows:
            return

        # ── Write enriched rows to HBase ────────────────────────────────
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
                    b"cf:asset_category":   row.asset_category.encode(),
                    b"cf:market_cap_tier":  row.market_cap_tier.encode(),
                })

        conn.close()
        print(
            f"[HBase] batch {batch_id}: wrote {len(rows)} enriched VWAP rows "
            f"to {TABLE_VWAP}"
        )

    return write_vwap_to_hbase

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print("=== Binance Kafka → HBase Sink (with HDFS metadata enrichment) ===")
    print(f"  HBase     : {HBASE_HOST}:{HBASE_PORT}")
    print(f"  Kafka     : {KAFKA_BOOTSTRAP_SERVERS}  topic={KAFKA_TOPIC}")
    print(f"  Tables    : {TABLE_TRADES}, {TABLE_VWAP}")
    print(f"  Metadata  : {SYMBOL_METADATA_PATH}")

    ensure_hbase_tables()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ── Load static symbol-metadata from HDFS (or local file:// fallback) ──
    # This DataFrame is a regular (non-streaming) broadcast-sized table.
    # It is loaded once, cached, and joined with every micro-batch via
    # Spark SQL inside the foreachBatch closures below.
    symbol_meta_df = load_symbol_metadata(spark)

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

    # Build enriched foreachBatch writers via closure factories.
    # Each factory captures symbol_meta_df and uses Spark SQL to LEFT JOIN
    # the live micro-batch with the static HDFS dataset before writing.
    trade_writer = make_trade_writer(symbol_meta_df)
    vwap_writer  = make_vwap_writer(symbol_meta_df)

    # Query 1: individual trades (append – every trade written once)
    q_trades = (
        trades_df.writeStream
        .outputMode("append")
        .foreachBatch(trade_writer)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/trades")
        .queryName("hbase_trades")
        .start()
    )

    # Query 2: VWAP windows (update – window emitted when watermark closes it)
    q_vwap = (
        vwap_df.writeStream
        .outputMode("update")
        .foreachBatch(vwap_writer)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/vwap")
        .queryName("hbase_vwap")
        .start()
    )

    print(f"\n  [{q_trades.id}] {q_trades.name}  → {TABLE_TRADES}")
    print(f"  [{q_vwap.id}]   {q_vwap.name}    → {TABLE_VWAP}\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
