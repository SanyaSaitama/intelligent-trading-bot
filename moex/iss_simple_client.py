#!/usr/bin/env python3
"""
    Small example of a library implementing interaction with Moscow Exchange ISS server.

    Version: 2.0 (Updated for Python 3)
    Developed for Python 3.x

    @copyright: 2016 by MOEX, updated 2024
"""

import urllib.request
import urllib.parse
import http.cookiejar
import json
import ssl
import base64


requests = {
    'history_secs': 'https://iss.moex.com/iss/history/engines/%(engine)s/markets/%(market)s/boards/%(board)s/securities.json?date=%(date)s',
    'current_orderbook': 'https://iss.moex.com/iss/engines/%(engine)s/markets/%(market)s/securities/%(security)s/orderbook.json',
    'current_securities': 'https://iss.moex.com/iss/engines/%(engine)s/markets/%(market)s/securities.json'
}


class Config:
    def __init__(self, user='', password='', proxy_url='', debug_level=0):
        """ Container for all the configuration options:
            user: username in MOEX Passport to access real-time data and history
            password: password for this user
            proxy_url: proxy URL if any is used, specified as http://proxy:port
            debug_level: 0 - no output, 1 - send debug info to stdout
        """
        self.debug_level = debug_level  
        self.proxy_url = proxy_url
        self.user = user
        self.password = password
        self.auth_url = "https://passport.moex.com/authenticate"


class MicexAuth:
    """ user authentication data and functions
    """

    def __init__(self, config):
        self.config = config
        self.cookie_jar = http.cookiejar.CookieJar()
        self.auth()

    def auth(self):
        """ one attempt to authenticate
        """
        # opener for https authorization
        if self.config.proxy_url:
            opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({"https": self.config.proxy_url}),
                urllib.request.HTTPCookieProcessor(self.cookie_jar),
                urllib.request.HTTPSHandler(context=ssl.create_default_context())
            )
        else:
            opener = urllib.request.build_opener(
                urllib.request.HTTPCookieProcessor(self.cookie_jar),
                urllib.request.HTTPSHandler(context=ssl.create_default_context())
            )

        # Create auth string
        auth_string = f"{self.config.user}:{self.config.password}"
        auth_bytes = auth_string.encode('utf-8')
        encoded_auth = base64.b64encode(auth_bytes).decode('utf-8')

        opener.addheaders = [('Authorization', f'Basic {encoded_auth}')]
        try:
            get_cert = opener.open(self.config.auth_url)
        except Exception as e:
            print(f"Authentication failed: {e}")
            return

        # we only need a cookie with MOEX Passport (certificate)
        self.passport = None
        for cookie in self.cookie_jar:
            if cookie.name == 'MicexPassportCert':
                self.passport = cookie
                break
        if self.passport is None:
            print("Cookie not found!")

    def is_real_time(self):
        """ repeat auth request if failed last time or cookie expired
        """
        if not self.passport or (self.passport and self.passport.is_expired()):
            self.auth()
        if self.passport and not self.passport.is_expired():
            return True
        return False


class MicexISSDataHandler:
    """ Data handler which will be called
    by the ISS client to handle downloaded data.
    """
    def __init__(self, container):
        """ The handler will have a container to store received data.
        """
        self.data = container()

    def do(self, market_data):
        """ This handler method should be overridden to perform
        the processing of data returned by the server.
        """
        pass


class MicexISSClient:
    """ Methods for interacting with the MICEX ISS server.
    """

    def __init__(self, config, auth=None, handler=None, container=None):
        """ Create opener for a connection with authorization cookie.
        It's not possible to reuse the opener used to authenticate because
        there's no method in opener to remove auth data.
            config: instance of the Config class with configuration options
            auth: instance of the MicexAuth class with authentication info (optional for public data)
            handler: user's handler class inherited from MicexISSDataHandler
            container: user's container class
        """
        if auth and auth.passport:
            # Use authenticated opener if auth is provided and valid
            if config.proxy_url:
                self.opener = urllib.request.build_opener(
                    urllib.request.ProxyHandler({"https": config.proxy_url}),
                    urllib.request.HTTPCookieProcessor(auth.cookie_jar),
                    urllib.request.HTTPSHandler(context=ssl.create_default_context())
                )
            else:
                self.opener = urllib.request.build_opener(
                    urllib.request.HTTPCookieProcessor(auth.cookie_jar),
                    urllib.request.HTTPSHandler(context=ssl.create_default_context())
                )
        else:
            # Use basic opener for public data
            if config.proxy_url:
                self.opener = urllib.request.build_opener(
                    urllib.request.ProxyHandler({"https": config.proxy_url}),
                    urllib.request.HTTPSHandler(context=ssl.create_default_context())
                )
            else:
                self.opener = urllib.request.build_opener(
                    urllib.request.HTTPSHandler(context=ssl.create_default_context())
                )

        if handler and container:
            self.handler = handler(container)
        else:
            self.handler = None

    def get_history_securities(self, engine, market, board, date):
        """ Get and parse historical data on all the securities at the
        given engine, market, board
        """
        url = requests['history_secs'] % {'engine': engine,
                                          'market': market,
                                          'board': board,
                                          'date': date}

        # always remember about the 'start' argument to get long replies
        start = 0
        cnt = 1
        while cnt > 0:
            try:
                res = self.opener.open(url + '&start=' + str(start))
                jres = json.loads(res.read().decode('utf-8'))
            except Exception as e:
                print(f"Error opening URL: {e}")
                break

            # the following is also just a simple example
            # it is recommended to keep metadata separately

            # root node with historical data
            jhist = jres['history']

            # node with actual data
            jdata = jhist['data']

            # node with the list of column IDs in 'data' in correct order;
            # it's also possible to use the iss.json=extended argument instead
            # to get all the IDs together with data (leads to more traffic)
            jcols = jhist['columns']
            secIdx = jcols.index('SECID')
            closeIdx = jcols.index('LEGALCLOSEPRICE')
            tradesIdx = jcols.index('NUMTRADES')

            result = []
            for sec in jdata:
                result.append((sec[secIdx],
                               del_null(sec[closeIdx]),
                               del_null(sec[tradesIdx])))
            # we return pieces of received data on each iteration
            # in order to be able to handle large volumes of data
            # and to start data processing without waiting for
            # the complete reply
            self.handler.do(result)
            cnt = len(jdata)
            start = start + cnt
        return True

    def get_current_orderbook(self, engine, market, security):
        """ Get current orderbook (quotes) for a specific security """
        url = requests['current_orderbook'] % {'engine': engine,
                                               'market': market,
                                               'security': security}
        try:
            res = self.opener.open(url)
            jres = json.loads(res.read().decode('utf-8'))
            return jres
        except Exception as e:
            print(f"Error getting orderbook: {e}")
            return None

    def get_current_securities(self, engine, market):
        """ Get current securities data for a market """
        url = requests['current_securities'] % {'engine': engine,
                                                'market': market}
        try:
            res = self.opener.open(url)
            jres = json.loads(res.read().decode('utf-8'))
            return jres
        except Exception as e:
            print(f"Error getting securities: {e}")
            return None


def del_null(num):
    """ replace null string with zero
    """
    return 0 if num is None else num