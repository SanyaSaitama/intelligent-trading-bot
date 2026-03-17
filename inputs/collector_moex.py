from datetime import datetime, timezone
from pathlib import Path
import sqlite3

import pandas as pd

from common.utils import pandas_get_interval

import logging
log = logging.getLogger('moex.collector')


def _resolve_db_path(config: dict) -> Path:
    configured = config.get("moex_db_path") or "moex/moex_quotes.db"
    db_path = Path(configured)
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path
    return db_path


def _read_symbol_quotes(conn: sqlite3.Connection, symbol: str, start_from_dt, freq: str, overlap_rows: int) -> pd.DataFrame:
    # Pull with overlap so rolling features are stable in the tail.
    if start_from_dt is not None:
        overlap = pd.Timedelta(freq) * max(overlap_rows, 1)
        read_from_dt = pd.Timestamp(start_from_dt, tz='UTC') - overlap
    else:
        bootstrap_hours = 24 * 30
        read_from_dt = pd.Timestamp.now(tz='UTC') - pd.Timedelta(hours=bootstrap_hours)

    sql = """
        SELECT
            q.timestamp,
            q.last_price,
            q.open_price,
            q.high_price,
            q.low_price,
            q.volume
        FROM quotes q
        JOIN securities s ON s.security_id = q.security_id
        WHERE s.secid = ?
          AND q.timestamp >= ?
        ORDER BY q.timestamp ASC
    """
    raw = pd.read_sql_query(sql, conn, params=(symbol, read_from_dt.isoformat()))
    if raw.empty:
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    raw['timestamp'] = pd.to_datetime(raw['timestamp'], utc=True, errors='coerce')
    raw = raw.dropna(subset=['timestamp'])
    if raw.empty:
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    # Fall back to last_price if OHLC snapshots are partially empty.
    px = raw['last_price'].astype(float)
    raw['open'] = raw['open_price'].astype(float).fillna(px)
    raw['high'] = raw['high_price'].astype(float).fillna(px)
    raw['low'] = raw['low_price'].astype(float).fillna(px)
    raw['close'] = px
    raw['volume'] = raw['volume'].astype(float).fillna(0.0)

    # Convert snapshots to regular bars.
    bars = (
        raw[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        .set_index('timestamp')
        .resample(freq)
        .agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'last',
        })
        .dropna(subset=['close'])
    )

    # Exclude currently forming interval.
    current_interval_start_ms, _ = pandas_get_interval(freq)
    current_interval_start = pd.to_datetime(current_interval_start_ms, unit='ms', utc=True)
    bars = bars[bars.index < current_interval_start]

    bars = bars.reset_index()
    bars = bars.astype({
        'open': 'float64',
        'high': 'float64',
        'low': 'float64',
        'close': 'float64',
        'volume': 'float64',
    })
    bars = bars.set_index('timestamp', inplace=False, drop=False)

    return bars


async def fetch_klines(config: dict, start_from_dt) -> dict[str, pd.DataFrame] | None:
    data_sources = config.get('data_sources', [])
    symbols = [x.get('folder') for x in data_sources if x.get('folder')]
    if not symbols:
        symbols = [config.get('symbol')]

    freq = config['freq']
    overlap_rows = int(config.get('append_overlap_records', 2))
    db_path = _resolve_db_path(config)

    if not db_path.exists():
        log.error(f"MOEX DB file does not exist: {db_path}")
        return None

    results: dict[str, pd.DataFrame] = {}
    with sqlite3.connect(str(db_path)) as conn:
        for symbol in symbols:
            df = _read_symbol_quotes(conn, symbol, start_from_dt, freq, overlap_rows)
            df.name = symbol
            results[symbol] = df
            log.info(f"MOEX collector: {symbol} -> {len(df)} bars")

    return results


async def health_check() -> int:
    try:
        from service.App import App
        config = getattr(App, 'config', {}) or {}
    except Exception:
        config = {}

    db_path = _resolve_db_path(config)
    if not db_path.exists():
        log.error(f"MOEX DB file does not exist: {db_path}")
        return 1

    try:
        with sqlite3.connect(str(db_path)) as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='quotes'")
            row = c.fetchone()
            if row is None:
                log.error("MOEX DB does not contain quotes table")
                return 1
    except Exception as ex:
        log.error(f"MOEX DB health check failed: {ex}")
        return 1

    return 0
