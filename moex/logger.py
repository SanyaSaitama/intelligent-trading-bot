import logging
import json

logging.basicConfig(filename='log.log', level=logging.INFO, format='%(asctime)s - %(message)s')

class LoggedResponse:
    def __init__(self, response):
        self.response = response
        self._data = None

    def read(self):
        if self._data is None:
            self._data = self.response.read()
            try:
                jres = json.loads(self._data.decode('utf-8'))
                num_fields = 0
                for key in ['history', 'orderbook', 'securities']:
                    if key in jres and 'data' in jres[key]:
                        num_fields = len(jres[key]['data'])
                        break
                logging.info(f"Number of fields returned: {num_fields}")
            except:
                pass  # if not json or error, skip logging
        return self._data

    def __getattr__(self, name):
        return getattr(self.response, name)

class LoggedOpener:
    def __init__(self, opener):
        self.opener = opener

    def open(self, url):
        logging.info(f"Request sent: {url}")
        response = self.opener.open(url)
        return LoggedResponse(response)