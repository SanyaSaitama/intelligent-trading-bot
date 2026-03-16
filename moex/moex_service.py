#!/usr/bin/env python3
"""
MOEX Quotes Service

A service that periodically fetches quotes/prices from MOEX ISS API
and stores them in a SQLite database.

Usage:
    python moex_service.py

Configuration:
    Configuration is loaded from moex_config.json
    - db_path: Path to SQLite database file
    - securities: List of securities to monitor
    - interval: Fetch interval in seconds
    - engines: Dictionary of engine to list of markets (e.g., {"stock": ["shares"], "currency": ["selt"]})
"""

import argparse
import time
import logging
import sqlite3
import json
import re
from datetime import datetime
try:
    # When run as a script from the moex directory
    from iss_simple_client import Config, MicexAuth, MicexISSClient, MicexISSDataHandler
except ImportError:
    # When imported as a package (e.g., `import moex.moex_service`)
    from .iss_simple_client import Config, MicexAuth, MicexISSClient, MicexISSDataHandler

# Load configuration
with open('moex_config.json', 'r') as f:
    config = json.load(f)

DB_PATH = config['db_path']
SECURITIES = config['securities']
INTERVAL = config['interval']
PAGE_SIZE = config.get('page_size', 500)
ENGINE_MARKETS = [(engine, market) for engine, markets in config['engines'].items() for market in markets]
TIMEOUT = config.get('timeout', 3600)  # Default to 1 hour timeout for API calls

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('moex_service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Columns explicitly excluded from the dynamic securities schema.
EXCLUDED_SECURITY_COLUMNS = {
    'raw_json', 'accruedint', 'annualhigh', 'annuallow', 'assetcode', 'auctionprice',
    'auctiontype', 'auctionnext', 'auctnextstop', 'boardid', 'boardname', 'bondsybtype',
    'bondsubtype', 'bondtype', 'buybackdate', 'buybackprice', 'buysell', 'buyselldescr',
    'buysellfee', 'calcmode', 'calloptiondate', 'centralstrike', 'commodityname',
    'couponpercent', 'couponperiod', 'couponvalue', 'dateyieldfromissuer', 'decimals',
    'delivarybasisname', 'deliverybasisname', 'deliverybasisshortname', 'exerciesefee',
    'exercisefee', 'faceunit', 'facevalue', 'facevalueonsettledate', 'firsttradeddate',
    'fosetypeid', 'forpricetype', 'highlimit', 'imbuy', 'imnp', 'imp', 'imtime',
    'initialmargin', 'instrid', 'isqualifiedinvestors', 'issuesize', 'issuesizeplaced',
    'lastdeldate', 'lastsettleprice', 'lasttradeddate', 'lotdivider', 'lotvalue',
    'lotvolume', 'lowlimit', 'maxprice', 'name', 'negotiationedfee', 'nextcoupon',
    'notes', 'offerdate', 'optiontype', 'prevdate', 'prevlast', 'prevlegalcloseprice',
    'prevopenpostition', 'prevopenposition', 'prevprice', 'prevsettleprice',
    'prevtradedate', 'prevwaprice', 'pricemvtlimit', 'primarydist', 'putcall',
    'putoptiondate', 'remarks', 'repo2price', 'scaleperfee', 'sectorid', 'sectorname',
    'settledate2', 'stepprice', 'strike', 'type', 'underlyingasset',
    'underlyingassetprice', 'underlyingsettleprice', 'underlyingtype', 'unit',
    'yieldatprevwaprice'
}


class QuotesDataHandler(MicexISSDataHandler):
    """Handler for processing quotes data."""

    def __init__(self, container):
        super().__init__(container)

    def do(self, market_data):
        # Support both container types:
        # - list: extend directly
        # - custom container with `.history` attribute
        if hasattr(self.data, 'history'):
            self.data.history.extend(market_data)
        else:
            self.data.extend(market_data)


class MOEXQuotesService:
    """ Service to fetch and store MOEX quotes """

    def __init__(self, db_path=DB_PATH, auto_init=True):
        self.db_path = db_path
        self.config = Config(user='', password='')  # Public data doesn't require auth
        self.auth = None  # No authentication for public data
        self.client = MicexISSClient(self.config, self.auth, QuotesDataHandler, list)
        self.security_columns = []
        # MOEX ISS context - list of (engine, market) tuples
        self.engine_markets = ENGINE_MARKETS

        if auto_init:
            if self._is_db_initialized():
                self.security_columns = self._load_security_columns_from_db()
                logger.info(f"Using existing initialized database at {self.db_path}")
            else:
                self.init_db()

    def _is_db_initialized(self):
        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            c.execute("SELECT name FROM sqlite_master WHERE type='table'")
            existing = {row[0] for row in c.fetchall()}
            required = {'engines', 'markets', 'durations', 'securities', 'quotes'}
            return required.issubset(existing)
        finally:
            conn.close()

    def _load_security_columns_from_db(self):
        conn = sqlite3.connect(self.db_path)
        try:
            c = conn.cursor()
            c.execute("PRAGMA table_info(securities)")
            columns = [row[1] for row in c.fetchall()]
            excluded = {
                'security_id', 'engine', 'market', 'uploaded_at',
                'updated_at', 'upload_source'
            }
            result = [col for col in columns if col not in excluded]
            if 'secid' not in result:
                result.insert(0, 'secid')
            return result
        finally:
            conn.close()

    def init_db(self):
          """Initialize SQLite database, create schema, and load reference/init data."""
          conn = sqlite3.connect(self.db_path)
          c = conn.cursor()

          c.execute("PRAGMA foreign_keys = OFF")
          c.execute("DROP TABLE IF EXISTS quotes")
          c.execute("DROP TABLE IF EXISTS securities")
          c.execute("DROP TABLE IF EXISTS markets")
          c.execute("DROP TABLE IF EXISTS engines")
          c.execute("DROP TABLE IF EXISTS durations")
          c.execute("DROP TABLE IF EXISTS securitytypes")
          c.execute("DROP TABLE IF EXISTS securitygroups")
          c.execute("DROP TABLE IF EXISTS exchanges")

          # ISS reference tables (filled from /iss/index.json)
          c.execute('''CREATE TABLE engines (
                                id INTEGER PRIMARY KEY,
                                name TEXT NOT NULL UNIQUE,
                                title TEXT
                            )''')

          c.execute('''CREATE TABLE markets (
                                market_id INTEGER PRIMARY KEY,
                                engine_id INTEGER NOT NULL,
                                engine TEXT NOT NULL,
                                market TEXT NOT NULL,
                                market_title TEXT,
                                marketplace TEXT,
                                board_order INTEGER,
                                UNIQUE(engine, market),
                                FOREIGN KEY (engine_id) REFERENCES engines(id)
                            )''')

          c.execute('''CREATE TABLE durations (
                                interval INTEGER PRIMARY KEY,
                                duration INTEGER,
                                days INTEGER,
                                title TEXT,
                                hint TEXT
                            )''')

          self._populate_reference_tables(conn)
          self.security_columns = self._discover_security_columns(conn)
          self._create_securities_table(conn, self.security_columns)

          # Quote snapshots
          c.execute('''CREATE TABLE quotes (
                                quote_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                security_id INTEGER NOT NULL,
                                timestamp DATETIME NOT NULL,
                                page INTEGER NOT NULL DEFAULT 0,
                                last_price REAL,
                                last_change REAL,
                                open_price REAL,
                                high_price REAL,
                                low_price REAL,
                                volume INTEGER,
                                value REAL,
                                UNIQUE(security_id, timestamp, page),
                                FOREIGN KEY (security_id) REFERENCES securities(security_id)
                            )''')

          self._populate_initial_securities(conn)
          c.execute("PRAGMA foreign_keys = ON")
          conn.commit()
          conn.close()
          logger.info(f"Database initialized at {self.db_path}")

    def _populate_reference_tables(self, conn):
        """Fetch /iss/index.json and upsert engines, markets, durations."""
        try:
            cfg = Config()
            client = MicexISSClient(cfg)
            index_data = client.get_index()
            if not index_data:
                logger.warning("Could not fetch ISS index — reference tables will be empty")
                return

            c = conn.cursor()

            if 'engines' in index_data:
                cols = index_data['engines']['columns']
                for row in index_data['engines']['data']:
                    r = dict(zip(cols, row))
                    c.execute(
                        "INSERT OR REPLACE INTO engines (id, name, title) VALUES (?, ?, ?)",
                        (r.get('id'), r.get('name'), r.get('title')),
                    )

            if 'markets' in index_data:
                cols = index_data['markets']['columns']
                for row in index_data['markets']['data']:
                    r = dict(zip(cols, row))
                    # ISS uses trade_engine_id / trade_engine_name / market_name
                    engine_id = r.get('trade_engine_id') or r.get('engine_id')
                    engine_name = r.get('trade_engine_name') or r.get('engine')
                    market_name = r.get('market_name') or r.get('market')
                    c.execute(
                        "INSERT OR REPLACE INTO markets "
                        "(market_id, engine_id, engine, market, market_title, marketplace, board_order) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (r.get('id'), engine_id, engine_name, market_name,
                         r.get('market_title'), r.get('marketplace'), r.get('board_order')),
                    )

            if 'durations' in index_data:
                cols = index_data['durations']['columns']
                for row in index_data['durations']['data']:
                    r = dict(zip(cols, row))
                    c.execute(
                        "INSERT OR REPLACE INTO durations (interval, duration, days, title, hint) VALUES (?, ?, ?, ?, ?)",
                        (r.get('interval'), r.get('duration'), r.get('days'), r.get('title'), r.get('hint')),
                    )

            conn.commit()
            logger.info("Reference tables populated from ISS index")
        except Exception as e:
            logger.warning(f"Could not populate reference tables: {e}")

    def _sanitize_column_name(self, name):
        col = re.sub(r'[^a-zA-Z0-9]+', '_', str(name)).strip('_').lower()
        if not col:
            col = 'field'
        if col[0].isdigit():
            col = f'c_{col}'
        return col

    def _get_engine_market_pairs(self, conn):
        # Restrict to configured engines/markets only.
        return ENGINE_MARKETS

    def _discover_security_columns(self, conn):
        cols = {'secid'}
        for engine, market in self._get_engine_market_pairs(conn):
            try:
                response = self.client.get_current_securities(engine, market)
                if not response or 'securities' not in response:
                    continue
                for col in response['securities'].get('columns', []):
                    normalized = self._sanitize_column_name(col)
                    if normalized not in EXCLUDED_SECURITY_COLUMNS:
                        cols.add(normalized)
            except Exception as e:
                logger.warning(f"Could not discover columns for {engine}/{market}: {e}")
        return sorted(cols)

    def _create_securities_table(self, conn, columns):
        c = conn.cursor()
        dynamic_cols = [
            col for col in columns
            if col not in {'security_id', 'engine', 'market', 'uploaded_at', 'updated_at', 'upload_source', 'raw_json'}
            and col not in EXCLUDED_SECURITY_COLUMNS
        ]
        if 'secid' not in dynamic_cols:
            dynamic_cols.insert(0, 'secid')

        quoted_dynamic = ',\n                        '.join([f'"{col}" TEXT' for col in dynamic_cols])
        create_sql = f'''CREATE TABLE securities (
                        security_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        engine TEXT NOT NULL,
                        market TEXT NOT NULL,
                        uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        upload_source TEXT,
                        {quoted_dynamic},
                        UNIQUE(secid, engine, market)
                     )'''
        c.execute(create_sql)

    def _upsert_security_row(self, conn, engine, market, row_map, upload_source='current_securities'):
        c = conn.cursor()
        normalized = {self._sanitize_column_name(k): v for k, v in row_map.items()}
        secid = normalized.get('secid')
        if not secid:
            return None

        c.execute(
            "SELECT security_id FROM securities WHERE secid = ? AND engine = ? AND market = ?",
            (secid, engine, market),
        )
        row = c.fetchone()

        payload = {
            'engine': engine,
            'market': market,
            'upload_source': upload_source,
        }
        for col in self.security_columns:
            if col in normalized:
                payload[col] = normalized[col]

        if row:
            security_id = row[0]
            update_cols = [k for k in payload.keys() if k not in {'engine', 'market'}]
            update_stmt = ', '.join([f'"{col}" = ?' for col in update_cols])
            sql = f"UPDATE securities SET {update_stmt}, updated_at = CURRENT_TIMESTAMP WHERE security_id = ?"
            params = [payload[col] for col in update_cols] + [security_id]
            c.execute(sql, params)
            return security_id

        insert_cols = list(payload.keys())
        placeholders = ', '.join(['?'] * len(insert_cols))
        sql = f"INSERT INTO securities ({', '.join([f'\"{col}\"' for col in insert_cols])}) VALUES ({placeholders})"
        params = [payload[col] for col in insert_cols]
        c.execute(sql, params)
        return c.lastrowid

    def _populate_initial_securities(self, conn):
        """Fill securities table using current_securities across all engine/market pairs from reference data."""
        pairs = self._get_engine_market_pairs(conn)
        loaded = 0
        for engine, market in pairs:
            response = self.client.get_current_securities(engine, market)
            if not response or 'securities' not in response:
                logger.warning(f"No current securities for {engine}/{market}")
                continue

            columns = response['securities'].get('columns', [])
            rows = response['securities'].get('data', [])
            for row in rows:
                row_map = {columns[i]: row[i] for i in range(len(columns))}
                if self._upsert_security_row(conn, engine, market, row_map, upload_source='init_load'):
                    loaded += 1
            conn.commit()

        logger.info(f"Initial securities loaded: {loaded}")

    def _store_quote(self, conn, security_id, data, timestamp=None, page=0):
        c = conn.cursor()
        if timestamp is None:
            timestamp = datetime.utcnow().replace(microsecond=0)
        c.execute(
            "INSERT OR REPLACE INTO quotes (security_id, timestamp, page, last_price, last_change, open_price, high_price, low_price, volume, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                security_id,
                timestamp,
                page,
                data.get('last_price'),
                data.get('last_change'),
                data.get('open_price'),
                data.get('high_price'),
                data.get('low_price'),
                data.get('volume', 0),
                data.get('value', 0),
            )
        )

    def _resolve_security_context(self, secid, engine=None, market=None):
        if engine and market:
            return engine, market

        for configured_engine, configured_market in self.engine_markets:
            engine_securities = SECURITIES.get(configured_engine, [])
            if secid in engine_securities:
                return configured_engine, configured_market

        return None, None

    def _get_last_loaded_page(self, conn, security_id):
        c = conn.cursor()
        c.execute("SELECT MAX(page) FROM quotes WHERE security_id = ?", (security_id,))
        row = c.fetchone()
        if not row or row[0] is None:
            return None
        return int(row[0])

    def _store_candles_page(self, conn, security_id, rows, columns, page):
        stored_count = 0
        for row in rows:
            row_map = {columns[i]: row[i] for i in range(len(columns))}
            ts_str = row_map.get('begin') or row_map.get('BEGIN') or row_map.get('end') or row_map.get('END')
            timestamp = datetime.fromisoformat(ts_str) if ts_str else datetime.utcnow().replace(microsecond=0)

            market_data = {
                'last_price': row_map.get('close') if 'close' in row_map else row_map.get('CLOSE'),
                'open_price': row_map.get('open') if 'open' in row_map else row_map.get('OPEN'),
                'high_price': row_map.get('high') if 'high' in row_map else row_map.get('HIGH'),
                'low_price': row_map.get('low') if 'low' in row_map else row_map.get('LOW'),
                'volume': row_map.get('volume') if 'volume' in row_map else row_map.get('VOLUME'),
                'value': row_map.get('value') if 'value' in row_map else row_map.get('VALUE'),
            }

            if market_data.get('last_price') is None:
                continue

            self._store_quote(conn, security_id, market_data, timestamp=timestamp, page=page)
            stored_count += 1

        return stored_count

    def _get_security_id(self, conn, secid, engine, market):
        c = conn.cursor()
        c.execute("SELECT security_id FROM securities WHERE secid = ? AND engine = ? AND market = ?", (secid, engine, market))
        row = c.fetchone()
        return row[0] if row else None

    def quota_load(self, secid, engine=None, market=None, interval=INTERVAL, page_size=PAGE_SIZE, page=None, allow_init=True):
        """Load one page of candles for secid, continuing from the last loaded page by default."""
        try:
            engine, market = self._resolve_security_context(secid, engine, market)
            if not engine or not market:
                logger.error(f"Could not resolve engine/market for {secid}")
                return {'status': 'error', 'secid': secid, 'page': page, 'rows': 0}

            conn = sqlite3.connect(self.db_path)
            try:
                security_id = self._get_security_id(conn, secid, engine, market)
                if security_id is None:
                    security_id = self._upsert_security_row(conn, engine, market, {'SECID': secid}, upload_source='runtime')
                    conn.commit()

                if page is None:
                    last_page = self._get_last_loaded_page(conn, security_id)
                    if last_page is None:
                        if allow_init:
                            return self.quota_init(secid, engine=engine, market=market, interval=interval, page_size=page_size)
                        return {'status': 'no_data', 'secid': secid, 'page': None, 'rows': 0}
                    page = last_page + 1

                position = page * page_size
                candles_data = self.client.get_security_candles(engine, market, secid, interval, position)
                if not candles_data or 'candles' not in candles_data:
                    logger.error(f"No candles data received for {secid} (page={page}, start={position})")
                    return {'status': 'error', 'secid': secid, 'page': page, 'rows': 0}

                rows = candles_data['candles'].get('data', [])
                columns = candles_data['candles'].get('columns', [])
                if not rows:
                    logger.info(f"EOF for {secid} at page={page} (start={position})")
                    return {'status': 'eof', 'secid': secid, 'page': page, 'rows': 0}

                stored_count = self._store_candles_page(conn, security_id, rows, columns, page)
                conn.commit()

                logger.info(
                    f"Loaded {stored_count} rows for {secid} (engine={engine}, market={market}, page={page}, start={position})"
                )
                return {'status': 'ok', 'secid': secid, 'page': page, 'rows': stored_count}
            finally:
                conn.close()

        except Exception as e:
            logger.error(f"Error loading quotes for {secid}: {e}")
            return {'status': 'error', 'secid': secid, 'page': page, 'rows': 0}

    def quota_init(self, secid, engine=None, market=None, interval=INTERVAL, page_size=PAGE_SIZE):
        """Initial load for secid: load all pages from 0 until endpoint EOF."""
        current_page = 0
        total_rows = 0
        while True:
            result = self.quota_load(
                secid,
                engine=engine,
                market=market,
                interval=interval,
                page_size=page_size,
                page=current_page,
                allow_init=False,
            )

            status = result.get('status')
            if status == 'ok':
                total_rows += result.get('rows', 0)
                current_page += 1
                continue

            if status == 'eof':
                logger.info(f"Initial load complete for {secid}: {total_rows} rows")
                return {'status': 'ok', 'secid': secid, 'rows': total_rows}

            logger.error(f"Initial load failed for {secid} at page={current_page}")
            return {'status': 'error', 'secid': secid, 'rows': total_rows}

    def fetch_security_data(self, secid, engine=None, market=None, interval=INTERVAL, position=0):
        """Backward-compatible wrapper that now uses the paged quota loader."""
        page = int(position // PAGE_SIZE)
        return self.quota_load(secid, engine=engine, market=market, interval=interval, page_size=PAGE_SIZE, page=page)

    def run(self):
        """ Main service loop """
        logger.info("Starting MOEX Quotes Service")
        logger.info(f"Engine-market combinations: {self.engine_markets}")
        logger.info(f"Update interval: {INTERVAL} seconds")

        while True:
            try:
                for engine, market in self.engine_markets:
                    logger.info(f"Fetching data for engine={engine}, market={market}")
                    engine_securities = SECURITIES.get(engine, [])
                    logger.info(f"Securities for {engine}: {engine_securities}")
                    for secid in engine_securities:
                        self.quota_load(secid, engine=engine, market=market, interval=INTERVAL, page_size=PAGE_SIZE)
                        time.sleep(1)  # Rate limiting between securities

                logger.info(f"Completed data fetch cycle. Sleeping for {INTERVAL} seconds...")
                time.sleep(INTERVAL)

            except KeyboardInterrupt:
                logger.info("Service stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(TIMEOUT)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='MOEX quotes service')
    parser.add_argument('--init', action='store_true', help='Initialize database and exit')
    parser.add_argument('--fetch', nargs='+', help='Fetch current data for listed securities (e.g., --fetch SBER GAZP)')
    parser.add_argument('--quota-init', action='store_true', help='Load initial candles history for all configured securities')
    parser.add_argument('--run', action='store_true', help='Run continuous service loop')

    args = parser.parse_args()

    service = MOEXQuotesService(auto_init=not args.init)
    if args.init:
        service.init_db()
        logger.info('Database forcefully reinitialized and ready.')
        return

    if args.fetch:
        for sec in args.fetch:
            for engine, market in ENGINE_MARKETS:
                engine_securities = SECURITIES.get(engine, [])
                if sec in engine_securities:
                    service.quota_load(sec, engine=engine, market=market, interval=INTERVAL, page_size=PAGE_SIZE)
        return

    if args.quota_init:
        for engine, market in ENGINE_MARKETS:
            for sec in SECURITIES.get(engine, []):
                service.quota_init(sec, engine=engine, market=market, interval=INTERVAL, page_size=PAGE_SIZE)
        return

    # Default behavior: run continuously
    service.run()


if __name__ == '__main__':
    main()