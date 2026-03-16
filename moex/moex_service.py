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
ENGINE_MARKETS = [(engine, market) for engine, markets in config['engines'].items() for market in markets]

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

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.init_db()
        self.config = Config(user='', password='')  # Public data doesn't require auth
        self.auth = None  # No authentication for public data
        self.client = MicexISSClient(self.config, self.auth, QuotesDataHandler, list)

        # MOEX ISS context - list of (engine, market) tuples
        self.engine_markets = ENGINE_MARKETS

    def init_db(self):
        """Initialize SQLite database with required tables."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # ISS reference tables (populated from /iss/index.json on first init)
        c.execute('''CREATE TABLE IF NOT EXISTS engines (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL UNIQUE,
                        title TEXT
                     )''')

        c.execute('''CREATE TABLE IF NOT EXISTS markets (
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

        c.execute('''CREATE TABLE IF NOT EXISTS durations (
                        interval INTEGER PRIMARY KEY,
                        duration INTEGER,
                        days INTEGER,
                        title TEXT,
                        hint TEXT
                     )''')

        c.execute('''CREATE TABLE IF NOT EXISTS securitytypes (
                        id INTEGER PRIMARY KEY,
                        security_group_name TEXT,
                        name TEXT NOT NULL,
                        title TEXT,
                        comment TEXT,
                        is_ordered INTEGER
                     )''')

        c.execute('''CREATE TABLE IF NOT EXISTS securitygroups (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL UNIQUE,
                        title TEXT,
                        is_ordered INTEGER
                     )''')

        # Securities master table
        c.execute('''CREATE TABLE IF NOT EXISTS securities (
                        security_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        secid TEXT NOT NULL UNIQUE,
                        isin TEXT,
                        short_name TEXT,
                        long_name TEXT,
                        lot_size INTEGER,
                        currency TEXT,
                        board_id TEXT,
                        market_id INTEGER,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (market_id) REFERENCES markets(market_id)
                     )''')

        # Quote snapshots
        c.execute('''CREATE TABLE IF NOT EXISTS quotes (
                        quote_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        security_id INTEGER NOT NULL,
                        timestamp DATETIME NOT NULL,
                        last_price REAL,
                        last_change REAL,
                        open_price REAL,
                        high_price REAL,
                        low_price REAL,
                        volume INTEGER,
                        value REAL,
                        source TEXT,
                        raw_json TEXT,
                        UNIQUE(security_id, timestamp, source),
                        FOREIGN KEY (security_id) REFERENCES securities(security_id)
                     )''')

        conn.commit()
        self._populate_reference_tables(conn)
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")

    def _populate_reference_tables(self, conn):
        """Fetch /iss/index.json and upsert engines, markets, durations, securitytypes, securitygroups."""
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

            if 'securitytypes' in index_data:
                cols = index_data['securitytypes']['columns']
                for row in index_data['securitytypes']['data']:
                    r = dict(zip(cols, row))
                    c.execute(
                        "INSERT OR REPLACE INTO securitytypes "
                        "(id, security_group_name, name, title, comment, is_ordered) VALUES (?, ?, ?, ?, ?, ?)",
                        (r.get('id'), r.get('security_group_name'), r.get('name'),
                         r.get('title'), r.get('comment'), r.get('is_ordered')),
                    )

            if 'securitygroups' in index_data:
                cols = index_data['securitygroups']['columns']
                for row in index_data['securitygroups']['data']:
                    r = dict(zip(cols, row))
                    c.execute(
                        "INSERT OR REPLACE INTO securitygroups (id, name, title, is_ordered) VALUES (?, ?, ?, ?)",
                        (r.get('id'), r.get('name'), r.get('title'), r.get('is_ordered')),
                    )

            conn.commit()
            logger.info("Reference tables populated from ISS index")
        except Exception as e:
            logger.warning(f"Could not populate reference tables: {e}")

    def _get_market_id(self, conn, engine, market):
        c = conn.cursor()
        c.execute("SELECT market_id FROM markets WHERE engine = ? AND market = ?", (engine, market))
        row = c.fetchone()
        return row[0] if row else None

    def _upsert_security(self, conn, secid, metadata, engine, market):
        """Insert or update a security master row and return its security_id."""
        c = conn.cursor()
        c.execute("SELECT security_id FROM securities WHERE secid = ?", (secid,))
        row = c.fetchone()
        market_id = self._get_market_id(conn, engine, market)

        # Normalize fields from metadata
        isin = metadata.get('ISIN')
        short_name = metadata.get('SHORTNAME') or metadata.get('SHORTNAME')
        long_name = metadata.get('NAME') or metadata.get('LONGNAME')
        lot_size = metadata.get('LOTSIZE')
        currency = metadata.get('CURRENCY')
        board_id = metadata.get('BOARDID')

        if row:
            security_id = row[0]
            c.execute(
                "UPDATE securities SET isin = ?, short_name = ?, long_name = ?, lot_size = ?, currency = ?, board_id = ?, market_id = ?, updated_at = CURRENT_TIMESTAMP WHERE security_id = ?",
                (isin, short_name, long_name, lot_size, currency, board_id, market_id, security_id)
            )
            return security_id

        c.execute(
            "INSERT INTO securities (secid, isin, short_name, long_name, lot_size, currency, board_id, market_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (secid, isin, short_name, long_name, lot_size, currency, board_id, market_id)
        )
        return c.lastrowid

    def _store_quote(self, conn, security_id, data, source, raw_json=None, timestamp=None):
        c = conn.cursor()
        if timestamp is None:
            timestamp = datetime.utcnow().replace(microsecond=0)
        c.execute(
            "INSERT OR REPLACE INTO quotes (security_id, timestamp, last_price, last_change, open_price, high_price, low_price, volume, value, source, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                security_id,
                timestamp,
                data.get('last_price'),
                data.get('last_change'),
                data.get('open_price'),
                data.get('high_price'),
                data.get('low_price'),
                data.get('volume', 0),
                data.get('value', 0),
                source,
                json.dumps(raw_json) if raw_json is not None else None,
            )
        )

    def _get_security_id(self, conn, secid):
        c = conn.cursor()
        c.execute("SELECT security_id FROM securities WHERE secid = ?", (secid,))
        row = c.fetchone()
        return row[0] if row else None

    def fetch_security_data(self, secid, engine, market):
        """Fetch data for a specific security and store it in the database."""
        try:
            # Get securities data (this is public)
            securities_data = self.client.get_current_securities(engine, market)
            if securities_data and 'securities' in securities_data:
                data = securities_data['securities']['data']
                columns = securities_data['securities']['columns']

                conn = sqlite3.connect(self.db_path)
                try:
                    # Find the security
                    secid_idx = columns.index('SECID')
                    for row in data:
                        if row[secid_idx] == secid:
                            row_map = {columns[i]: row[i] for i in range(len(columns))}

                            security_id = self._upsert_security(conn, secid, row_map, engine, market)

                            market_data = {}
                            if 'LAST' in row_map:
                                market_data['last_price'] = row_map['LAST']
                            if 'CHANGE' in row_map:
                                market_data['last_change'] = row_map['CHANGE']
                            if 'OPEN' in row_map:
                                market_data['open_price'] = row_map['OPEN']
                            if 'HIGH' in row_map:
                                market_data['high_price'] = row_map['HIGH']
                            if 'LOW' in row_map:
                                market_data['low_price'] = row_map['LOW']
                            if 'VOLTODAY' in row_map:
                                market_data['volume'] = row_map['VOLTODAY']
                            if 'VALTODAY' in row_map:
                                market_data['value'] = row_map['VALTODAY']

                            if market_data.get('last_price') is not None:
                                self._store_quote(conn, security_id, market_data, 'current_securities', raw_json=row_map)
                                logger.info(f"Stored market data for {secid}: {market_data.get('last_price')}")
                            else:
                                logger.warning(f"No price data available for {secid}")
                            break

                    conn.commit()
                finally:
                    conn.close()
            else:
                logger.error(f"No securities data received")

        except Exception as e:
            logger.error(f"Error fetching data for {secid}: {e}")

    def load_history(self, date_str, board='TQBR', engine=None, market=None):
        """Fetch historical data for a single date and store it.

        Args:
            date_str: Date string in YYYY-MM-DD format.
            board: Market board (default 'TQBR').
            engine: MOEX engine (if None, uses all configured engines).
            market: MOEX market (if None, uses all configured markets).
        """
        if engine is None or market is None:
            # Load for all engine-market combinations
            for eng, mkt in self.engine_markets:
                self._load_history_single(date_str, board, eng, mkt)
        else:
            self._load_history_single(date_str, board, engine, market)

    def _load_history_single(self, date_str, board, engine, market):
        """Fetch historical data for a single date and engine-market combination."""
        logger.info(f"Loading history for {date_str} (board={board}, engine={engine}, market={market})")
        client = MicexISSClient(self.config, self.auth)
        hist_data = client.get_history_securities(engine, market, board, date_str)

        if not hist_data or 'history' not in hist_data:
            logger.warning(f"No history data returned for {date_str} (engine={engine}, market={market})")
            return

        cols = hist_data['history']['columns']
        rows = hist_data['history']['data']
        if not rows:
            logger.warning(f"No history rows for {date_str} (engine={engine}, market={market})")
            return

        secid_idx = cols.index('SECID')
        close_idx = cols.index('CLOSE') if 'CLOSE' in cols else None
        numtrades_idx = cols.index('NUMTRADES') if 'NUMTRADES' in cols else None

        date_ts = datetime.fromisoformat(date_str)
        conn = sqlite3.connect(self.db_path)
        try:
            for row in rows:
                secid = row[secid_idx]
                engine_securities = SECURITIES.get(engine, [])
                if secid in engine_securities:
                    close_price = row[close_idx] if close_idx is not None else None
                    num_trades = row[numtrades_idx] if numtrades_idx is not None else None
                    security_id = self._upsert_security(conn, secid, {'SECID': secid}, engine, market)
                    self._store_quote(
                        conn,
                        security_id,
                        {'last_price': close_price, 'value': num_trades},
                        source=f'history_{date_str}',
                        raw_json={'secid': secid, 'close': close_price, 'trades': num_trades},
                        timestamp=date_ts,
                    )
            conn.commit()
        finally:
            conn.close()

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
                        self.fetch_security_data(secid, engine, market)
                        time.sleep(1)  # Rate limiting between securities

                logger.info(f"Completed data fetch cycle. Sleeping for {INTERVAL} seconds...")
                time.sleep(INTERVAL)

            except KeyboardInterrupt:
                logger.info("Service stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(INTERVAL)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='MOEX quotes service')
    parser.add_argument('--init', action='store_true', help='Initialize database and exit')
    parser.add_argument('--fetch', nargs='+', help='Fetch current data for listed securities (e.g., --fetch SBER GAZP)')
    parser.add_argument('--history', help='Fetch historical data for a given date (YYYY-MM-DD)')
    parser.add_argument('--history-board', default='TQBR', help='MOEX board for historical data (default: TQBR)')
    parser.add_argument('--run', action='store_true', help='Run continuous service loop')

    args = parser.parse_args()

    service = MOEXQuotesService()
    if args.init:
        # init_db already called in constructor
        logger.info('Database initialized and ready.')
        return

    if args.history:
        service.load_history(args.history, board=args.history_board)
        return

    if args.fetch:
        for sec in args.fetch:
            for engine, market in ENGINE_MARKETS:
                engine_securities = SECURITIES.get(engine, [])
                if sec in engine_securities:
                    service.fetch_security_data(sec, engine, market)
        return

    # Default behavior: run continuously
    service.run()


if __name__ == '__main__':
    main()