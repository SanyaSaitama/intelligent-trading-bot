from datetime import datetime
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


def _ensure_detection_table(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pattern_detections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            pattern_name TEXT NOT NULL,
            confidence REAL NOT NULL,
            source TEXT NOT NULL DEFAULT 'rule',
            created_at DATETIME NOT NULL,
            UNIQUE(symbol, timeframe, timestamp, pattern_name)
        )
        """
    )


def _persist_patterns(df: pd.DataFrame, config: dict, patterns: list[str], last_rows: int):
    store_config = config.get('pattern_store', {})
    runtime_config = config.get('_runtime_config', {})
    if not store_config.get('enabled', True):
        return

    db_path = store_config.get('db_path') or runtime_config.get('moex_db_path') or 'moex/moex_quotes.db'
    db_path = Path(db_path)
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path

    symbol = runtime_config.get('symbol', '')
    timeframe = runtime_config.get('freq', '')

    rows_df = df.tail(last_rows) if last_rows else df
    if rows_df.empty:
        return

    now_iso = datetime.utcnow().isoformat()
    inserts = []

    for idx, row in rows_df.iterrows():
        ts = pd.Timestamp(idx).isoformat()
        for pattern in patterns:
            flag = row.get(pattern, 0.0)
            if pd.isna(flag) or float(flag) <= 0.0:
                continue

            conf_col = pattern.replace('pattern_', 'pattern_confidence_')
            confidence = row.get(conf_col, 1.0)
            confidence = 1.0 if pd.isna(confidence) else float(confidence)

            inserts.append((symbol, timeframe, ts, pattern, confidence, 'rule', now_iso))

    if not inserts:
        return

    with sqlite3.connect(str(db_path)) as conn:
        _ensure_detection_table(conn)
        conn.executemany(
            """
            INSERT OR REPLACE INTO pattern_detections
            (symbol, timeframe, timestamp, pattern_name, confidence, source, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            inserts,
        )
        conn.commit()


def generate_patterns_ohlc(df: pd.DataFrame, config: dict, last_rows: int = 0):
    required = ['open', 'high', 'low', 'close']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Pattern generator requires columns: {missing}")

    doji_ratio = float(config.get('doji_body_to_range_max', 0.1))
    long_shadow_ratio = float(config.get('long_shadow_to_body_min', 2.0))
    breakout_window = int(config.get('breakout_window', 20))
    breakout_pct = float(config.get('breakout_pct', 0.0)) / 100.0

    if last_rows:
        work_rows = min(len(df), last_rows + breakout_window + 5)
        work_df = df.tail(work_rows).copy()
    else:
        work_df = df

    o = work_df['open'].astype(float)
    h = work_df['high'].astype(float)
    l = work_df['low'].astype(float)
    c = work_df['close'].astype(float)

    body = (c - o).abs()
    candle_range = (h - l).replace(0, np.nan)
    upper_shadow = h - np.maximum(o, c)
    lower_shadow = np.minimum(o, c) - l

    prev_o = o.shift(1)
    prev_c = c.shift(1)

    work_df['pattern_bullish_engulfing'] = (
        (c > o)
        & (prev_c < prev_o)
        & (o <= prev_c)
        & (c >= prev_o)
    ).astype(float)

    work_df['pattern_bearish_engulfing'] = (
        (c < o)
        & (prev_c > prev_o)
        & (o >= prev_c)
        & (c <= prev_o)
    ).astype(float)

    work_df['pattern_doji'] = ((body / candle_range) <= doji_ratio).fillna(False).astype(float)

    work_df['pattern_hammer'] = (
        (lower_shadow >= long_shadow_ratio * body)
        & (upper_shadow <= body)
        & (c > o)
    ).fillna(False).astype(float)

    work_df['pattern_shooting_star'] = (
        (upper_shadow >= long_shadow_ratio * body)
        & (lower_shadow <= body)
        & (c < o)
    ).fillna(False).astype(float)

    rolling_high_prev = h.shift(1).rolling(window=breakout_window, min_periods=breakout_window).max()
    rolling_low_prev = l.shift(1).rolling(window=breakout_window, min_periods=breakout_window).min()

    work_df['pattern_breakout_up'] = (c > (rolling_high_prev * (1.0 + breakout_pct))).fillna(False).astype(float)
    work_df['pattern_breakout_down'] = (c < (rolling_low_prev * (1.0 - breakout_pct))).fillna(False).astype(float)

    pattern_columns = [
        'pattern_bullish_engulfing',
        'pattern_bearish_engulfing',
        'pattern_doji',
        'pattern_hammer',
        'pattern_shooting_star',
        'pattern_breakout_up',
        'pattern_breakout_down',
    ]

    # Confidence in [0, 1].
    base_conf = (body / candle_range).replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(0.0, 1.0)
    for col in pattern_columns:
        conf_col = col.replace('pattern_', 'pattern_confidence_')
        work_df[conf_col] = np.where(work_df[col] > 0.0, np.maximum(base_conf, 0.55), 0.0)

    confidence_columns = [c.replace('pattern_', 'pattern_confidence_') for c in pattern_columns]
    features = pattern_columns + confidence_columns

    df.loc[work_df.index, features] = work_df[features]

    persist_enabled = config.get('pattern_store', {}).get('enabled', True)
    if persist_enabled:
        _persist_patterns(df, config, pattern_columns, last_rows)

    return features
