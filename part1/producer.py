"""
Binance WebSocket → Apache Kafka Producer

Connects to the Binance combined public WebSocket stream and forwards
every raw JSON message to a Kafka topic unchanged.

Supported stream types per symbol:
  trade       – real-time trade executions
  ticker      – 24-hour rolling statistics
  depth5      – order-book top-5 snapshot
  kline_1m    – 1-minute candlestick

Usage:
  python producer.py

Environment variables (override defaults via .env):
  KAFKA_BOOTSTRAP_SERVERS  default: localhost:9092
  KAFKA_TOPIC              default: binance-raw
  BINANCE_SYMBOLS          default: btcusdt,ethusdt
  BINANCE_STREAMS          default: trade
  RECONNECT_DELAY_S        default: 5
"""

import asyncio
import logging
import os
import signal
import sys
from typing import NoReturn

import websockets
from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "binance-raw")

BINANCE_WS_BASE: str = "wss://stream.binance.com:9443/ws"
SYMBOLS: list[str] = [s.lower() for s in os.getenv("BINANCE_SYMBOLS", "btcusdt,ethusdt").split(",")]
STREAMS: list[str] = [s.strip() for s in os.getenv("BINANCE_STREAMS", "trade").split(",")]
RECONNECT_DELAY_S: float = float(os.getenv("RECONNECT_DELAY_S", "5"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------

PRODUCER_CONFIG: dict = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "binance-ws-producer",
    # Guarantee every message is acknowledged by the broker leader.
    "acks": "1",
    # Micro-batch up to 10 ms to improve throughput without noticeable latency.
    "linger.ms": 10,
    "batch.size": 32768,
    # Retry transient send failures.
    "retries": 5,
    "retry.backoff.ms": 500,
}


def _delivery_report(err, msg) -> None:
    """Callback fired by librdkafka after each produce attempt."""
    if err:
        log.error("Delivery failed | topic=%s partition=%s error=%s", msg.topic(), msg.partition(), err)
    else:
        log.debug(
            "Delivered | topic=%s partition=%d offset=%d size=%d bytes",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            len(msg.value()),
        )


def build_producer() -> Producer:
    try:
        producer = Producer(PRODUCER_CONFIG)
        log.info("Kafka producer created (brokers=%s)", KAFKA_BOOTSTRAP_SERVERS)
        return producer
    except KafkaException as exc:
        log.critical("Failed to create Kafka producer: %s", exc)
        sys.exit(1)


# ---------------------------------------------------------------------------
# WebSocket stream URL
# ---------------------------------------------------------------------------

def build_stream_url() -> str:
    """
    Binance combined-stream format:
      wss://stream.binance.com:9443/ws/<stream1>/<stream2>/...

    Each stream name is  <symbol>@<streamType>, e.g. btcusdt@trade
    """
    stream_names = [f"{symbol}@{stream}" for symbol in SYMBOLS for stream in STREAMS]
    url = f"{BINANCE_WS_BASE}/{'/'.join(stream_names)}"
    log.info("Binance stream URL: %s", url)
    return url


# ---------------------------------------------------------------------------
# Core streaming loop
# ---------------------------------------------------------------------------

async def stream_to_kafka(producer: Producer, ws_url: str) -> NoReturn:
    """
    Maintains a persistent WebSocket connection to Binance and publishes
    every received frame to Kafka as raw bytes. Reconnects automatically
    on any disconnection or error.
    """
    messages_sent = 0

    while True:
        try:
            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:
                log.info("Connected to Binance WebSocket.")

                async for raw_msg in ws:
                    payload: bytes = (
                        raw_msg.encode("utf-8") if isinstance(raw_msg, str) else raw_msg
                    )

                    producer.produce(
                        topic=KAFKA_TOPIC,
                        value=payload,
                        callback=_delivery_report,
                    )

                    # poll(0) triggers delivery callbacks without blocking.
                    producer.poll(0)

                    messages_sent += 1
                    if messages_sent % 500 == 0:
                        log.info("Messages produced so far: %d", messages_sent)

        except websockets.ConnectionClosedOK:
            log.warning("WebSocket closed cleanly. Reconnecting in %.0fs...", RECONNECT_DELAY_S)
        except websockets.ConnectionClosedError as exc:
            log.warning("WebSocket closed with error: %s. Reconnecting in %.0fs...", exc, RECONNECT_DELAY_S)
        except OSError as exc:
            log.error("Network error: %s. Reconnecting in %.0fs...", exc, RECONNECT_DELAY_S)
        except Exception as exc:  # noqa: BLE001
            log.exception("Unexpected error: %s. Reconnecting in %.0fs...", exc, RECONNECT_DELAY_S)
        finally:
            # Flush any buffered messages before the next reconnect attempt.
            remaining = producer.flush(timeout=10)
            if remaining:
                log.warning("%d message(s) were not delivered before reconnect.", remaining)

        await asyncio.sleep(RECONNECT_DELAY_S)


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

def _shutdown(loop: asyncio.AbstractEventLoop, producer: Producer) -> None:
    log.info("Shutdown signal received – flushing producer...")
    producer.flush(timeout=15)
    log.info("Producer flushed. Stopping event loop.")
    loop.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("Starting Binance → Kafka producer")
    log.info("  Symbols : %s", SYMBOLS)
    log.info("  Streams : %s", STREAMS)
    log.info("  Topic   : %s", KAFKA_TOPIC)
    log.info("  Brokers : %s", KAFKA_BOOTSTRAP_SERVERS)

    producer = build_producer()
    ws_url = build_stream_url()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, loop, producer)

    try:
        loop.run_until_complete(stream_to_kafka(producer, ws_url))
    except RuntimeError:
        # loop.stop() raises RuntimeError("Event loop stopped before Future completed")
        pass
    finally:
        loop.close()
        log.info("Producer shut down cleanly.")


if __name__ == "__main__":
    main()
