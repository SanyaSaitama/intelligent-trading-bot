import base64
from datetime import datetime
import hashlib
import importlib
import io
from pathlib import Path
import sqlite3

import pandas as pd

from common.model_store import ModelStore
from outputs.notifier_diagram import generate_chart

import logging
log = logging.getLogger('notifier.patterns')


def _resolve_store_db_path(config: dict) -> Path:
    db_path = config.get('pattern_alert_db_path') or config.get('moex_db_path') or 'moex/moex_quotes.db'
    db_path = Path(db_path)
    if not db_path.is_absolute():
        db_path = Path.cwd() / db_path
    return db_path


def _ensure_notifier_tables(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pattern_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            candle_timestamp DATETIME NOT NULL,
            pattern_name TEXT NOT NULL,
            confidence REAL NOT NULL,
            sent_at DATETIME NOT NULL,
            UNIQUE(symbol, timeframe, candle_timestamp, pattern_name)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pattern_llm_cache (
            cache_key TEXT PRIMARY KEY,
            response_text TEXT NOT NULL,
            created_at DATETIME NOT NULL
        )
        """
    )


def _llm_cache_get(conn: sqlite3.Connection, cache_key: str) -> str:
    cur = conn.cursor()
    cur.execute("SELECT response_text FROM pattern_llm_cache WHERE cache_key = ?", (cache_key,))
    row = cur.fetchone()
    return row[0] if row else ''


def _llm_cache_put(conn: sqlite3.Connection, cache_key: str, response_text: str):
    conn.execute(
        """
        INSERT OR REPLACE INTO pattern_llm_cache (cache_key, response_text, created_at)
        VALUES (?, ?, ?)
        """,
        (cache_key, response_text, datetime.utcnow().isoformat()),
    )


def _already_sent(conn: sqlite3.Connection, symbol: str, timeframe: str, candle_ts: str, pattern_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1 FROM pattern_alerts
        WHERE symbol = ? AND timeframe = ? AND candle_timestamp = ? AND pattern_name = ?
        """,
        (symbol, timeframe, candle_ts, pattern_name),
    )
    return cur.fetchone() is not None


def _store_sent(conn: sqlite3.Connection, symbol: str, timeframe: str, candle_ts: str, matched: list[tuple[str, float]]):
    now_iso = datetime.utcnow().isoformat()
    rows = [(symbol, timeframe, candle_ts, name, conf, now_iso) for name, conf in matched]
    conn.executemany(
        """
        INSERT OR REPLACE INTO pattern_alerts
        (symbol, timeframe, candle_timestamp, pattern_name, confidence, sent_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def _load_template_labels(template_folder: str) -> list[str]:
    if not template_folder:
        return []

    folder = Path(template_folder)
    if not folder.exists() or not folder.is_dir():
        return []

    labels = set()
    for p in folder.glob('*'):
        if p.suffix.lower() not in ['.png', '.jpg', '.jpeg', '.webp']:
            continue
        labels.add(p.stem.split('__')[0])

    return sorted(labels)


def _llm_fallback(model_cfg: dict, app_config: dict, chart_bytes: bytes, patterns: list[str], candidates: list[str], cache_get_fn) -> str:
    try:
        requests = importlib.import_module('requests')
    except Exception:
        return ''

    llm_cfg = model_cfg.get('llm', {})
    if not llm_cfg.get('enabled', False):
        return ''

    endpoint = llm_cfg.get('endpoint')
    api_key = llm_cfg.get('api_key') or app_config.get('llm_api_key')
    model_name = llm_cfg.get('model')
    if not endpoint or not api_key or not model_name:
        return ''

    image_b64 = base64.b64encode(chart_bytes).decode('ascii')
    cache_key = hashlib.sha256(
        (image_b64 + '|' + '|'.join(sorted(patterns)) + '|' + '|'.join(sorted(candidates))).encode('utf-8')
    ).hexdigest()

    cached = cache_get_fn(cache_key)
    if cached:
        return cached

    prompt = (
        'Classify technical chart pattern using candidate labels first. '
        f'Candidates: {candidates}. Rule-based detections: {patterns}. '
        'Return short answer: best_label, confidence_0_to_1, rationale.'
    )

    payload = {
        'model': model_name,
        'messages': [
            {
                'role': 'user',
                'content': [
                    {'type': 'text', 'text': prompt},
                    {'type': 'image_url', 'image_url': {'url': f'data:image/png;base64,{image_b64}'}},
                ],
            }
        ],
        'temperature': 0.1,
        'max_tokens': int(llm_cfg.get('max_tokens', 120)),
    }
    headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}

    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        body = response.json()
        choice = (body.get('choices') or [{}])[0]
        message = choice.get('message', {})
        content = message.get('content', '')
        result = content if isinstance(content, str) else str(content)
        return cache_key + '||' + result
    except Exception as ex:
        log.warning(f"LLM fallback request failed: {ex}")
        return ''


async def send_pattern_alerts(df, model: dict, config: dict, model_store: ModelStore):
    try:
        requests = importlib.import_module('requests')
    except Exception:
        log.warning('Pattern alert skipped: requests package is not available')
        return

    if df.empty:
        return

    enabled = model.get('pattern_notification', True)
    if not enabled:
        return

    pattern_prefix = model.get('pattern_prefix', 'pattern_')
    confidence_prefix = model.get('confidence_prefix', 'pattern_confidence_')
    min_confidence = float(model.get('min_confidence', 0.55))

    pattern_columns = [
        c for c in df.columns
        if c.startswith(pattern_prefix) and not c.startswith(confidence_prefix)
    ]
    if not pattern_columns:
        return

    latest = df.iloc[-1]
    matched = []
    for col in pattern_columns:
        if float(latest.get(col, 0.0) or 0.0) <= 0.0:
            continue

        conf_col = col.replace(pattern_prefix, confidence_prefix)
        conf = float(latest.get(conf_col, 0.0) or 0.0)
        if conf < min_confidence:
            continue
        matched.append((col, conf))

    if not matched:
        return

    matched = sorted(matched, key=lambda x: x[1], reverse=True)
    symbol = config.get('symbol', '')
    timeframe = config.get('freq', '')
    latest_ts = pd.Timestamp(df.index[-1]).isoformat() if len(df.index) > 0 else ''

    store_db = _resolve_store_db_path(config)
    with sqlite3.connect(str(store_db)) as conn:
        _ensure_notifier_tables(conn)
        # Idempotent batch mode: skip if top pattern for this candle was already sent.
        if _already_sent(conn, symbol, timeframe, latest_ts, matched[0][0]):
            return

    nrows = int(model.get('nrows', 120))
    window_df = df.tail(nrows).copy()
    if 'timestamp' not in window_df.columns:
        window_df['timestamp'] = window_df.index

    top_conf_col = matched[0][0].replace(pattern_prefix, confidence_prefix)
    thresholds = [min_confidence]

    title = f"{config.get('symbol', '')} patterns"
    fig = generate_chart(
        window_df,
        title,
        buy_signal_column=None,
        sell_signal_column=None,
        score_column=[top_conf_col] if top_conf_col in window_df.columns else None,
        thresholds=thresholds,
    )

    with io.BytesIO() as buf:
        fig.savefig(buf, format='png', bbox_inches='tight', pad_inches=0.1)
        chart_bytes = buf.getvalue()

    candidate_labels = _load_template_labels(model.get('template_folder', ''))
    uncertain = any(conf < float(model.get('llm_threshold', 0.75)) for _, conf in matched)
    llm_summary = ''
    if uncertain:
        with sqlite3.connect(str(store_db)) as conn:
            _ensure_notifier_tables(conn)

            def _cache_get(k: str) -> str:
                return _llm_cache_get(conn, k)

            cached_or_tagged = _llm_fallback(model, config, chart_bytes, [x[0] for x in matched], candidate_labels, _cache_get)
            if '||' in cached_or_tagged:
                cache_key, llm_summary = cached_or_tagged.split('||', 1)
                _llm_cache_put(conn, cache_key, llm_summary)
                conn.commit()
            else:
                llm_summary = cached_or_tagged

    summary = ', '.join([f"{name} ({conf:.2f})" for name, conf in matched[:5]])
    caption_lines = [
        f"Pattern alert: {config.get('symbol', '')} {config.get('freq', '')}",
        summary,
    ]
    if llm_summary:
        caption_lines.append(f"LLM: {llm_summary}")

    bot_token = config.get('telegram_bot_token')
    chat_id = config.get('telegram_chat_id')
    if not bot_token or not chat_id:
        log.warning('Pattern alert skipped: telegram credentials are not configured')
        return

    payload = {
        'chat_id': chat_id,
        'caption': '\n'.join(caption_lines),
        'parse_mode': 'markdown',
    }

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
        response = requests.post(url=url, data=payload, files={'photo': chart_bytes}, timeout=20)
        response.raise_for_status()

        with sqlite3.connect(str(store_db)) as conn:
            _ensure_notifier_tables(conn)
            _store_sent(conn, symbol, timeframe, latest_ts, matched)
            conn.commit()
    except Exception as ex:
        log.error(f"Error sending pattern alert: {ex}")
