"""
Part 4 – REST API bridge: HBase → Flutter dashboard.

Reads live data from the HBase tables populated by part3/sink_hbase.py
and exposes clean JSON endpoints polled every 5 s by the Flutter app.

Endpoints
---------
GET /health                          – liveness probe
GET /api/symbols                     – list of tracked symbols
GET /api/vwap/{symbol}?minutes=60    – VWAP 1-min bars for last N minutes
GET /api/trades/{symbol}?limit=30    – most recent raw trades
GET /api/summary                     – latest VWAP bar per symbol

Run
---
  bash run.sh
  (or)
  uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

import os
import time

import happybase
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

HBASE_HOST: str    = os.getenv("HBASE_HOST", "localhost")
HBASE_PORT: int    = int(os.getenv("HBASE_PORT", "9090"))
SYMBOLS: list[str] = [
    s.upper().strip()
    for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT").split(",")
]

TABLE_TRADES = "binance_trades"
TABLE_VWAP   = "binance_vwap"

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Binance Live Dashboard API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# HBase helpers
# ---------------------------------------------------------------------------

def _connect() -> happybase.Connection:
    return happybase.Connection(HBASE_HOST, port=HBASE_PORT)


def _str(data: dict, key: bytes, default: str = "") -> str:
    raw = data.get(key)
    if raw is None:
        return default
    return raw.decode(errors="replace")


def _float(data: dict, key: bytes, default: float = 0.0) -> float:
    try:
        return float(_str(data, key, str(default)))
    except (ValueError, TypeError):
        return default


def _int(data: dict, key: bytes, default: int = 0) -> int:
    try:
        return int(_str(data, key, str(default)))
    except (ValueError, TypeError):
        return default

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    try:
        conn   = _connect()
        tables = [t.decode() for t in conn.tables()]
        conn.close()
        return {"status": "ok", "hbase": f"{HBASE_HOST}:{HBASE_PORT}", "tables": tables}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/api/symbols")
def get_symbols():
    return {"symbols": SYMBOLS}


@app.get("/api/vwap/{symbol}")
def get_vwap(symbol: str, minutes: int = 60) -> list[dict]:
    symbol    = symbol.upper()
    now       = int(time.time())
    start_ts  = now - minutes * 60
    start_row = f"{symbol}#{str(start_ts).zfill(15)}".encode()
    stop_row  = f"{symbol}#{str(now + 60).zfill(15)}".encode()

    try:
        conn  = _connect()
        table = conn.table(TABLE_VWAP)
        rows: list[dict] = []
        for _, data in table.scan(row_start=start_row, row_stop=stop_row):
            rows.append({
                "symbol":         _str(data,   b"cf:symbol",        symbol),
                "window_start":   _str(data,   b"cf:window_start"),
                "window_end":     _str(data,   b"cf:window_end"),
                "vwap_usd":       _float(data, b"cf:vwap_usd"),
                "total_volume":   _float(data, b"cf:total_volume"),
                "total_notional": _float(data, b"cf:total_notional"),
                "trade_count":    _int(data,   b"cf:trade_count"),
                "low_usd":        _float(data, b"cf:low_usd"),
                "high_usd":       _float(data, b"cf:high_usd"),
            })
        conn.close()
        return rows
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/trades/{symbol}")
def get_trades(symbol: str, limit: int = 30) -> list[dict]:
    symbol    = symbol.upper()
    # Row keys: SYMBOL#<trade_id_padded>  —  scan the symbol prefix
    start_row = f"{symbol}#".encode()
    stop_row  = (symbol + "$").encode()   # '$' (0x24) > '#' (0x23)

    try:
        conn  = _connect()
        table = conn.table(TABLE_TRADES)
        rows: list[dict] = []
        for _, data in table.scan(row_start=start_row, row_stop=stop_row):
            rows.append({
                "symbol":   _str(data,   b"cf:symbol",   symbol),
                "trade_id": _str(data,   b"cf:trade_id"),
                "price":    _float(data, b"cf:price"),
                "qty":      _float(data, b"cf:qty"),
                "notional": _float(data, b"cf:notional"),
                "side":     _str(data,   b"cf:side"),
                "trade_ts": _str(data,   b"cf:trade_ts"),
            })
        conn.close()
        # Trades are sorted by trade_id ASC; take the most recent N
        return rows[-limit:]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/summary")
def get_summary() -> list[dict]:
    """Latest completed VWAP bar for every tracked symbol."""
    now      = int(time.time())
    start_ts = now - 180   # look back 3 min to catch the latest closed bar

    results: list[dict] = []
    try:
        conn  = _connect()
        table = conn.table(TABLE_VWAP)
        for symbol in SYMBOLS:
            start_row = f"{symbol}#{str(start_ts).zfill(15)}".encode()
            stop_row  = f"{symbol}#{str(now + 60).zfill(15)}".encode()

            latest: dict | None = None
            for _, data in table.scan(row_start=start_row, row_stop=stop_row):
                latest = data   # last wins → highest key = most recent bar

            if latest:
                results.append({
                    "symbol":       symbol,
                    "vwap_usd":     _float(latest, b"cf:vwap_usd"),
                    "high_usd":     _float(latest, b"cf:high_usd"),
                    "low_usd":      _float(latest, b"cf:low_usd"),
                    "trade_count":  _int(latest,   b"cf:trade_count"),
                    "total_volume": _float(latest, b"cf:total_volume"),
                    "window_start": _str(latest,   b"cf:window_start"),
                })
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return results
