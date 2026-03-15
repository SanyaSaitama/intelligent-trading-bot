#!/usr/bin/env python3
"""
    Small example of interaction with Moscow Exchange ISS server.

    Version: 2.0 (Updated for Python 3)
    Developed for Python 3.x

    Requires iss_simple_client.py library.
    Note that the valid username and password for the MOEX ISS account
    are required in order to perform the given request for historical data.

    @copyright: 2016 by MOEX, updated 2024
"""

import sys
import sqlite3
import time
from datetime import datetime
from iss_simple_client import Config
from iss_simple_client import MicexAuth
from iss_simple_client import MicexISSClient
from iss_simple_client import MicexISSDataHandler


class MyData:
    """ Container that will be used by the handler to store data.
    Kept separately from the handler for scalability purposes: in order
    to differentiate storage and output from the processing.
    """
    def __init__(self):
        self.history = []

    def print_history(self):
        print("=" * 49)
        print("|%15s|%15s|%15s|" % ("SECID", "CLOSE", "TRADES"))
        print("=" * 49)
        for sec in self.history:
            print("|%15s|%15.2f|%15d|" % (sec[0], sec[1], sec[2]))
        print("=" * 49)


class MyDataHandler(MicexISSDataHandler):
    """ This handler will be receiving pieces of data from the ISS client.
    """
    def do(self, market_data):
        """ Just as an example we add all the chunks to one list.
        In real application other options should be considered because some
        server replies may be too big to be kept in memory.
        """
        self.data.history = self.data.history + market_data


class MOEXService:
    """ Service to fetch and store MOEX quotes/prices """

    def __init__(self, db_path='moex_quotes.db'):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        """ Initialize SQLite database """
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS securities (
                        id INTEGER PRIMARY KEY,
                        secid TEXT,
                        timestamp DATETIME,
                        price REAL,
                        volume INTEGER
                     )''')
        c.execute('''CREATE TABLE IF NOT EXISTS orderbook (
                        id INTEGER PRIMARY KEY,
                        secid TEXT,
                        timestamp DATETIME,
                        bid_price REAL,
                        bid_quantity INTEGER,
                        ask_price REAL,
                        ask_quantity INTEGER
                     )''')
        conn.commit()
        conn.close()

    def store_security_data(self, secid, price, volume):
        """ Store security price data """
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("INSERT INTO securities (secid, timestamp, price, volume) VALUES (?, ?, ?, ?)",
                  (secid, datetime.now(), price, volume))
        conn.commit()
        conn.close()

    def store_orderbook_data(self, secid, bids, asks):
        """ Store orderbook data """
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        timestamp = datetime.now()
        # Store top bid and ask
        if bids and asks:
            bid_price = bids[0][0] if bids[0] else None
            bid_qty = bids[0][1] if bids[0] else None
            ask_price = asks[0][0] if asks[0] else None
            ask_qty = asks[0][1] if asks[0] else None
            c.execute("INSERT INTO orderbook (secid, timestamp, bid_price, bid_quantity, ask_price, ask_quantity) VALUES (?, ?, ?, ?, ?, ?)",
                      (secid, timestamp, bid_price, bid_qty, ask_price, ask_qty))
        conn.commit()
        conn.close()

    def fetch_and_store_quotes(self, securities=['SBER', 'GAZP', 'LKOH']):
        """ Fetch current quotes and store in database """
        config = Config(user='', password='')  # No auth needed for basic data
        auth = MicexAuth(config)
        client = MicexISSClient(config, auth, MyDataHandler, MyData)

        for secid in securities:
            print(f"Fetching data for {secid}")
            # Get orderbook
            orderbook = client.get_current_orderbook('stock', 'shares', secid)
            if orderbook and 'orderbook' in orderbook:
                bids = orderbook['orderbook'].get('b', [])
                asks = orderbook['orderbook'].get('a', [])
                self.store_orderbook_data(secid, bids, asks)
                print(f"Stored orderbook for {secid}")

            # Get securities data
            securities_data = client.get_current_securities('stock', 'shares')
            if securities_data and 'securities' in securities_data:
                data = securities_data['securities']['data']
                columns = securities_data['securities']['columns']
                secid_idx = columns.index('SECID')
                price_idx = columns.index('PREVPRICE') if 'PREVPRICE' in columns else None
                volume_idx = columns.index('VOLTODAY') if 'VOLTODAY' in columns else None

                for row in data:
                    if row[secid_idx] == secid:
                        price = row[price_idx] if price_idx else None
                        volume = row[volume_idx] if volume_idx else None
                        if price:
                            self.store_security_data(secid, price, volume or 0)
                            print(f"Stored price data for {secid}: {price}")
                        break

            time.sleep(1)  # Rate limiting


def main():
    # Example usage
    my_config = Config(user='username', password='password', proxy_url='')
    my_auth = MicexAuth(my_config)
    if my_auth.is_real_time():
        iss = MicexISSClient(my_config, my_auth, MyDataHandler, MyData)
        iss.get_history_securities('stock',
                                   'shares',
                                   'eqne',
                                   '2010-04-29')
        iss.handler.data.print_history()

    # New service functionality
    service = MOEXService()
    service.fetch_and_store_quotes()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"Sorry: {type(e).__name__}: {e}")
    my_config = Config(user='username', password='password', proxy_url='')
    my_auth = MicexAuth(my_config)
    if my_auth.is_real_time():
        iss = MicexISSClient(my_config, my_auth, MyDataHandler, MyData)
        iss.get_history_securities('stock',
                                   'shares',
                                   'eqne',
                                   '2010-04-29')
        iss.handler.data.print_history()

if __name__ == '__main__':
    try:
        main()
    except:
        print "Sorry:", sys.exc_type, ":", sys.exc_value
