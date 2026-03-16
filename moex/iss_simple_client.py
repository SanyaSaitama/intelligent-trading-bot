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

try:
    # When run as a script from the moex directory
    from logger import LoggedOpener
except ImportError:
    # When imported as a package (e.g., `import moex.iss_simple_client`)
    from .logger import LoggedOpener


requests = {
    'current_securities': 'https://iss.moex.com/iss/engines/%(engine)s/markets/%(market)s/securities.json',
    'security_candles': 'https://iss.moex.com/iss/engines/%(engine)s/markets/%(market)s/securities/%(security)s/candles.json?interval=%(interval)s&start=%(position)s',
    'index': 'https://iss.moex.com/iss/index.json',
    'security_spec': 'https://iss.moex.com/iss/securities/%(security)s.json',
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

        self.opener = LoggedOpener(self.opener)

        if handler and container:
            self.handler = handler(container)
        else:
            self.handler = None

    def get_index(self):
        """ Get global ISS reference data (engines, markets, durations, securitytypes, securitygroups) """
        url = requests['index']
        try:
            res = self.opener.open(url)
            return json.loads(res.read().decode('utf-8'))
        except Exception as e:
            print(f"Error getting index: {e}")
            return None

    def get_current_securities(self, engine, market):
        """ Get current trading data for all securities in a given engine/market """
        url = requests['current_securities'] % {'engine': engine, 'market': market}
        try:
            res = self.opener.open(url)
            return json.loads(res.read().decode('utf-8'))
        except Exception as e:
            print(f"Error getting current securities for {engine}/{market}: {e}")
            return None

    def get_security_candles(self, engine, market, security, interval, position=0):
        """ Get OHLCV candles for a security """
        url = requests['security_candles'] % {
            'engine': engine, 'market': market,
            'security': security, 'interval': interval, 'position': position,
        }
        try:
            res = self.opener.open(url)
            return json.loads(res.read().decode('utf-8'))
        except Exception as e:
            print(f"Error getting candles for {security}: {e}")
            return None

    def get_security_spec(self, security):
        """ Get specification for a security """
        url = requests['security_spec'] % {'security': security}
        try:
            res = self.opener.open(url)
            return json.loads(res.read().decode('utf-8'))
        except Exception as e:
            print(f"Error getting security spec for {security}: {e}")
            return None


def del_null(num):
    """ replace null string with zero
    """
    return 0 if num is None else num