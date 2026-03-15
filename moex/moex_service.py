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

        # Exchange/market metadata
        c.execute('''CREATE TABLE IF NOT EXISTS exchanges (
                        exchange_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL UNIQUE,
                        url TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                     )''')

        c.execute('''CREATE TABLE IF NOT EXISTS markets (
                        market_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        exchange_id INTEGER NOT NULL,
                        engine TEXT NOT NULL,
                        market TEXT NOT NULL,
                        board TEXT,
                        UNIQUE(exchange_id, engine, market, board),
                        FOREIGN KEY (exchange_id) REFERENCES exchanges(exchange_id)
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

        # Metadata for API responses (columns, etc.)
        c.execute('''CREATE TABLE IF NOT EXISTS security_metadata (
                        meta_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        market_id INTEGER NOT NULL,
                        response_type TEXT NOT NULL,
                        columns_json TEXT NOT NULL,
                        fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
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

        # Orderbook snapshots and levels
        c.execute('''CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                        orderbook_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        security_id INTEGER NOT NULL,
                        timestamp DATETIME NOT NULL,
                        source TEXT,
                        raw_json TEXT,
                        FOREIGN KEY (security_id) REFERENCES securities(security_id)
                     )''')

        c.execute('''CREATE TABLE IF NOT EXISTS orderbook_levels (
                        level_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        orderbook_id INTEGER NOT NULL,
                        side TEXT NOT NULL,
                        level INTEGER NOT NULL,
                        price REAL,
                        quantity INTEGER,
                        FOREIGN KEY (orderbook_id) REFERENCES orderbook_snapshots(orderbook_id)
                     )''')

        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")

    def _get_or_create_exchange(self, conn, name='MOEX', url='https://iss.moex.com'):
        c = conn.cursor()
        c.execute("SELECT exchange_id FROM exchanges WHERE name = ?", (name,))
        row = c.fetchone()
        if row:
            return row[0]
        c.execute("INSERT INTO exchanges (name, url) VALUES (?, ?)", (name, url))
        return c.lastrowid

    def _get_or_create_market(self, conn, engine, market):
        c = conn.cursor()
        exchange_id = self._get_or_create_exchange(conn)
        c.execute(
            "SELECT market_id FROM markets WHERE exchange_id = ? AND engine = ? AND market = ?",
            (exchange_id, engine, market)
        )
        row = c.fetchone()
        if row:
            return row[0]
        c.execute(
            "INSERT INTO markets (exchange_id, engine, market) VALUES (?, ?, ?)",
            (exchange_id, engine, market)
        )
        return c.lastrowid

    def _upsert_security(self, conn, secid, metadata, engine, market):
        """Insert or update a security master row and return its security_id."""
        c = conn.cursor()
        c.execute("SELECT security_id FROM securities WHERE secid = ?", (secid,))
        row = c.fetchone()
        market_id = self._get_or_create_market(conn, engine, market)

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

    def _store_metadata(self, conn, response_type, columns, engine, market):
        """Store the column metadata for a given response type."""
        market_id = self._get_or_create_market(conn, engine, market)
        c = conn.cursor()
        c.execute(
            "INSERT INTO security_metadata (market_id, response_type, columns_json) VALUES (?, ?, ?)",
            (market_id, response_type, json.dumps(columns))
        )

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

    def _store_orderbook(self, conn, security_id, bids, asks, source, raw_json=None):
        if not bids and not asks:
            return
        c = conn.cursor()
        timestamp = datetime.utcnow().replace(microsecond=0)
        c.execute(
            "INSERT INTO orderbook_snapshots (security_id, timestamp, source, raw_json) VALUES (?, ?, ?, ?)",
            (security_id, timestamp, source, json.dumps(raw_json) if raw_json is not None else None)
        )
        orderbook_id = c.lastrowid

        # store each level for bid and ask
        for side, rows in (('bid', bids), ('ask', asks)):
            for idx, level in enumerate(rows, start=1):
                price, qty = level if len(level) >= 2 else (None, None)
                c.execute(
                    "INSERT INTO orderbook_levels (orderbook_id, side, level, price, quantity) VALUES (?, ?, ?, ?, ?)",
                    (orderbook_id, side, idx, price, qty)
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
                    # Store column metadata for future reference
                    self._store_metadata(conn, 'current_securities', columns, engine, market)

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

            # Try orderbook (may require auth)
            try:
                orderbook = self.client.get_current_orderbook(engine, market, secid)
                if orderbook and 'orderbook' in orderbook:
                    bids = orderbook['orderbook'].get('b', [])
                    asks = orderbook['orderbook'].get('a', [])
                    if bids or asks:
                        conn = sqlite3.connect(self.db_path)
                        try:
                            security_id = self._get_security_id(conn, secid)
                            if security_id:
                                self._store_orderbook(conn, security_id, bids, asks, 'current_orderbook', raw_json=orderbook)
                                logger.info(f"Stored orderbook for {secid}")
                            else:
                                logger.warning(f"Skipping orderbook for {secid}: security not found")
                            conn.commit()
                        finally:
                            conn.close()
                    else:
                        logger.debug(f"No orderbook data for {secid}")
                else:
                    logger.debug(f"No orderbook response for {secid}")
            except Exception as e:
                logger.debug(f"Orderbook not available for {secid}: {e}")

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
        client = MicexISSClient(self.config, self.auth, QuotesDataHandler, list)
        client.get_history_securities(engine, market, board, date_str)

        history = getattr(client.handler, 'data', None)
        if not history:
            logger.warning(f"No history data returned for {date_str} (engine={engine}, market={market})")
            return

        # Store the historical snapshot with timestamp set to the date.
        date_ts = datetime.fromisoformat(date_str)
        conn = sqlite3.connect(self.db_path)
        try:
            for secid, close_price, num_trades in history:
                # Only store if this security belongs to this engine
                engine_securities = SECURITIES.get(engine, [])
                if secid in engine_securities:
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