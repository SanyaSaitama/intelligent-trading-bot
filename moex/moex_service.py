#!/usr/bin/env python3
"""
MOEX Quotes Service

A service that periodically fetches quotes/prices from MOEX ISS API
and stores them in a SQLite database.

Usage:
    python moex_service.py

Configuration:
    - DB_PATH: Path to SQLite database file
    - SECURITIES: List of securities to monitor
    - INTERVAL: Fetch interval in seconds
"""

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

# Configuration
DB_PATH = 'moex_quotes.db'
SECURITIES = ['SBER', 'GAZP', 'LKOH', 'ROSN', 'VTBR', 'TATN', 'MGNT', 'NVTK', 'YNDX', 'POLY']
INTERVAL = 60  # seconds

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
    """ Handler for processing quotes data """
    def __init__(self, container):
        super().__init__(container)

    def do(self, market_data):
        self.data.history.extend(market_data)


class MOEXQuotesService:
    """ Service to fetch and store MOEX quotes """

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.init_db()
        self.config = Config(user='', password='')  # Public data doesn't require auth
        self.auth = None  # No authentication for public data
        self.client = MicexISSClient(self.config, self.auth, QuotesDataHandler, list)

        # MOEX ISS context (hardcoded for now)
        self.engine = 'stock'
        self.market = 'shares'

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

    def _get_or_create_market(self, conn):
        c = conn.cursor()
        exchange_id = self._get_or_create_exchange(conn)
        c.execute(
            "SELECT market_id FROM markets WHERE exchange_id = ? AND engine = ? AND market = ?",
            (exchange_id, self.engine, self.market)
        )
        row = c.fetchone()
        if row:
            return row[0]
        c.execute(
            "INSERT INTO markets (exchange_id, engine, market) VALUES (?, ?, ?)",
            (exchange_id, self.engine, self.market)
        )
        return c.lastrowid

    def _upsert_security(self, conn, secid, metadata):
        """Insert or update a security master row and return its security_id."""
        c = conn.cursor()
        c.execute("SELECT security_id FROM securities WHERE secid = ?", (secid,))
        row = c.fetchone()
        market_id = self._get_or_create_market(conn)

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

    def _store_metadata(self, conn, response_type, columns):
        """Store the column metadata for a given response type."""
        market_id = self._get_or_create_market(conn)
        c = conn.cursor()
        c.execute(
            "INSERT INTO security_metadata (market_id, response_type, columns_json) VALUES (?, ?, ?)",
            (market_id, response_type, json.dumps(columns))
        )

    def _store_quote(self, conn, security_id, data, source, raw_json=None):
        c = conn.cursor()
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

    def fetch_security_data(self, secid):
        """Fetch data for a specific security and store it in the database."""
        try:
            # Get securities data (this is public)
            securities_data = self.client.get_current_securities(self.engine, self.market)
            if securities_data and 'securities' in securities_data:
                data = securities_data['securities']['data']
                columns = securities_data['securities']['columns']

                conn = sqlite3.connect(self.db_path)
                try:
                    # Store column metadata for future reference
                    self._store_metadata(conn, 'current_securities', columns)

                    # Find the security
                    secid_idx = columns.index('SECID')
                    for row in data:
                        if row[secid_idx] == secid:
                            row_map = {columns[i]: row[i] for i in range(len(columns))}

                            security_id = self._upsert_security(conn, secid, row_map)

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
                orderbook = self.client.get_current_orderbook(self.engine, self.market, secid)
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

    def run(self):
        """ Main service loop """
        logger.info("Starting MOEX Quotes Service")
        logger.info(f"Monitoring securities: {', '.join(SECURITIES)}")
        logger.info(f"Update interval: {INTERVAL} seconds")

        while True:
            try:
                for secid in SECURITIES:
                    self.fetch_security_data(secid)
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
    """ Main entry point """
    service = MOEXQuotesService()
    service.run()


if __name__ == '__main__':
    main()