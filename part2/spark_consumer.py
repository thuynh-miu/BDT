"""
Binance Kafka → Spark Structured Streaming Consumer (Part 2)

Reads raw Binance trade events from the 'binance-raw' Kafka topic written
by part1/producer.py and runs four concurrent streaming queries:

  Query 1  vwap_1min
      Tumbling 1-minute VWAP, volume, trade count, and high/low per symbol.
      Enriched via a broadcast join with a static symbol reference table
      (asset class, market-cap rank).

  Query 2  trade_velocity_30s
      Sliding window (1-min window, 30-sec slide) separating buy-side vs
      sell-side aggressor volume. Useful for detecting order-flow imbalances.

  Query 3  whale_alerts
      Append-mode pass-through filter that surfaces individual trades whose
      notional value >= WHALE_THRESHOLD_USD (default $50,000).

  Query 4  moving_avg_price
      Sliding window (2-min window, 10-sec slide) simple moving average of
      price per symbol, plus stddev-based volatility flag.

Run:
  bash submit.sh

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS   default: localhost:9092
  KAFKA_TOPIC               default: binance-raw
  WHALE_THRESHOLD_USD       default: 50000
  CHECKPOINT_DIR            default: /tmp/spark-checkpoints/binance
"""

import os

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

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str             = os.getenv("KAFKA_TOPIC", "binance-raw")
WHALE_THRESHOLD_USD: float   = float(os.getenv("WHALE_THRESHOLD_USD", "50000"))
CHECKPOINT_BASE: str         = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/binance")

# ---------------------------------------------------------------------------
# Binance trade-event JSON schema
# Price and quantity arrive as strings in the Binance WebSocket API.
# ---------------------------------------------------------------------------

TRADE_SCHEMA = StructType([
    StructField("e", StringType(),  True),  # event type ("trade")
    StructField("E", LongType(),    True),  # event time (epoch ms)
    StructField("s", StringType(),  True),  # symbol, e.g. "BTCUSDT"
    StructField("t", LongType(),    True),  # trade ID
    StructField("p", StringType(),  True),  # price (string)
    StructField("q", StringType(),  True),  # quantity / base amount (string)
    StructField("b", LongType(),    True),  # buyer order ID
    StructField("a", LongType(),    True),  # seller order ID
    StructField("T", LongType(),    True),  # trade time (epoch ms)
    StructField("m", BooleanType(), True),  # True → buyer is market maker (sell-side aggressor)
    StructField("M", BooleanType(), True),  # ignore
])

# ---------------------------------------------------------------------------
# Static reference data — broadcast-joined with streaming to enrich trades
# ---------------------------------------------------------------------------

REFERENCE_ROWS = [
    ("BTCUSDT", "BTC", "USDT", 1, "Layer-1"),
    ("ETHUSDT", "ETH", "USDT", 2, "Layer-1"),
    ("BNBUSDT", "BNB", "USDT", 4, "Exchange-token"),
    ("SOLUSDT", "SOL", "USDT", 5, "Layer-1"),
    ("XRPUSDT", "XRP", "USDT", 6, "Payment"),
]

REFERENCE_SCHEMA = StructType([
    StructField("ref_symbol",  StringType(), False),
    StructField("base_asset",  StringType(), False),
    StructField("quote_asset", StringType(), False),
    StructField("mcap_rank",   LongType(),   False),
    StructField("asset_class", StringType(), False),
])

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BinanceKafkaStreaming")
        .config("spark.sql.shuffle.partitions", "4")
        # Required: the Binance schema has case-colliding field pairs (e/E, t/T, m/M).
        # Without this, Spark treats them as the same column after select("d.*").
        .config("spark.sql.caseSensitive", "true")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

# ---------------------------------------------------------------------------
# Shared parsing and enrichment step
# ---------------------------------------------------------------------------

def parse_trades(raw_df):
    """
    Kafka value bytes → parsed, typed trade columns.

    Derived columns added:
      event_ts  – trade timestamp as TimestampType (from field T)
      price_d   – price cast from string to double
      qty_d     – quantity cast from string to double
      notional  – USD value of the trade  (price_d × qty_d)
      side      – "SELL" when m=True (buyer is passive market-maker), else "BUY"

    A 30-second watermark on event_ts allows Spark to bound state and emit
    late-arriving data without holding windows open indefinitely.
    """
    return (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("d"))
        .filter(F.col("d.e") == "trade")   # filter on struct field before expanding
        .select("d.*")
        .withColumn("event_ts", (F.col("T") / 1000).cast("timestamp"))
        .withColumn("price_d",  F.col("p").cast(DoubleType()))
        .withColumn("qty_d",    F.col("q").cast(DoubleType()))
        .withColumn("notional", F.col("price_d") * F.col("qty_d"))
        .withColumn("side",     F.when(F.col("m"), "SELL").otherwise("BUY"))
        .drop("p", "q", "M", "E", "b", "a")
        .withWatermark("event_ts", "30 seconds")
    )

# ---------------------------------------------------------------------------
# Query 1 — 1-minute tumbling VWAP with static reference join
# ---------------------------------------------------------------------------

def start_vwap_query(trades_df, ref_df):
    """
    Tumbling 1-minute window VWAP per symbol.

    VWAP = Σ(price × qty) / Σ(qty)  within the window.

    The static ref_df is broadcast-joined before aggregation to attach
    asset_class and mcap_rank without a shuffle.
    Output mode: update — emits each window result as soon as the watermark
    confirms no more late data can arrive for that window.
    """
    enriched = trades_df.join(
        F.broadcast(ref_df),
        trades_df["s"] == ref_df["ref_symbol"],
        "left",
    )

    agg = (
        enriched
        .groupBy(F.window("event_ts", "1 minute"), "s", "asset_class", "mcap_rank")
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
            "asset_class",
            "mcap_rank",
            F.round("vwap_usd", 4).alias("vwap_usd"),
            F.round("total_volume", 6).alias("total_volume"),
            F.round("total_notional_usd", 2).alias("total_notional_usd"),
            "trade_count",
            F.round("low_usd", 4).alias("low_usd"),
            F.round("high_usd", 4).alias("high_usd"),
        )
    )

    return (
        agg.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/vwap")
        .queryName("vwap_1min")
        .start()
    )

# ---------------------------------------------------------------------------
# Query 2 — Sliding-window buy/sell trade velocity
# ---------------------------------------------------------------------------

def start_velocity_query(trades_df):
    """
    Sliding window (1-min window, 30-sec slide) buy vs. sell pressure.

    Separates trades by aggressor side so the consumer can detect when
    one side is dominating — a leading indicator of short-term price moves.
    Output mode: update.
    """
    velocity = (
        trades_df
        .groupBy(
            F.window("event_ts", "1 minute", "30 seconds"),
            "s",
            "side",
        )
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("qty_d").alias("volume"),
            F.sum("notional").alias("notional_usd"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("s").alias("symbol"),
            "side",
            "trade_count",
            F.round("volume", 6).alias("volume"),
            F.round("notional_usd", 2).alias("notional_usd"),
        )
    )

    return (
        velocity.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 30)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/velocity")
        .queryName("trade_velocity_30s")
        .start()
    )

# ---------------------------------------------------------------------------
# Query 3 — Whale alert filter (large individual trades)
# ---------------------------------------------------------------------------

def start_whale_query(trades_df, threshold_usd: float):
    """
    Stateless append-mode filter that forwards any single trade whose
    notional value >= threshold_usd to the output sink.

    These large-block trades can signal institutional activity and often
    precede significant short-term price moves.
    Output mode: append.
    """
    whales = (
        trades_df
        .filter(F.col("notional") >= threshold_usd)
        .select(
            "event_ts",
            F.col("s").alias("symbol"),
            F.col("t").alias("trade_id"),
            F.round("price_d", 4).alias("price_usd"),
            F.round("qty_d", 8).alias("quantity"),
            F.round("notional", 2).alias("notional_usd"),
            "side",
        )
    )

    return (
        whales.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/whales")
        .queryName("whale_alerts")
        .start()
    )

# ---------------------------------------------------------------------------
# Query 4 — Streaming moving average of price with volatility flag
# ---------------------------------------------------------------------------

def start_moving_avg_query(trades_df):
    """
    Sliding window (2-min window, 10-sec slide) SMA and stddev per symbol.

    Approximates a 120-second simple moving average of the trade price.
    The volatility_flag column marks windows where price stddev / SMA > 0.1%,
    which indicates an unusually choppy micro-structure for that symbol.
    Output mode: update.
    """
    sma = (
        trades_df
        .groupBy(
            F.window("event_ts", "2 minutes", "10 seconds"),
            "s",
        )
        .agg(
            F.avg("price_d").alias("sma_usd"),
            F.stddev("price_d").alias("price_stddev"),
            F.count("*").alias("sample_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("s").alias("symbol"),
            F.round("sma_usd", 4).alias("sma_usd"),
            F.round("price_stddev", 6).alias("price_stddev_usd"),
            "sample_count",
            F.when(
                F.col("price_stddev") / F.col("sma_usd") > 0.001,
                "HIGH-VOLATILITY",
            ).otherwise("NORMAL").alias("volatility_flag"),
        )
    )

    return (
        sma.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/sma")
        .queryName("moving_avg_price")
        .start()
    )

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    ref_df = spark.createDataFrame(REFERENCE_ROWS, schema=REFERENCE_SCHEMA)

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

    q1 = start_vwap_query(trades_df, ref_df)
    q2 = start_velocity_query(trades_df)
    q3 = start_whale_query(trades_df, WHALE_THRESHOLD_USD)
    q4 = start_moving_avg_query(trades_df)

    sep = "=" * 62
    print(f"\n{sep}")
    print("  4 streaming queries active:")
    print(f"  [{q1.id}] {q1.name:30s}  ← 1-min VWAP + OHLCV")
    print(f"  [{q2.id}] {q2.name:30s}  ← Buy/Sell velocity (sliding)")
    print(f"  [{q3.id}] {q3.name:30s}  ← Whale trade alerts")
    print(f"  [{q4.id}] {q4.name:30s}  ← SMA-120s + volatility flag")
    print(f"{sep}\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
