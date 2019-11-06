import json
import logging
from pprint import pformat
from urllib.parse import quote as urlquote

import httpx
from sseclient import SSEClient

from .db import DB


class OrbitDbAPI ():
    def __init__ (self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.__config = kwargs
        self.__base_url = self.__config.get('base_url')
        self.__use_db_cache = self.__config.get('use_db_cache', True)
        self.__timeout = self.__config.get('timeout', 30)
        self.__headers = httpx.Headers(self.__config.get('headers', {}))
        self.__session = httpx.Client(
            headers=self.__headers,
            timeout=self.__timeout
            )
        self.logger.debug(f'Base url: {self.__base_url}')
        self.logger.debug(f'Headers: {self.__headers.items()}')

    @property
    def session(self):
        return self.__session

    @property
    def base_url(self):
        return self.__base_url

    @property
    def use_db_cache(self):
        return self.__use_db_cache

    def close(self):
        self.__session.close()

    def _do_request(self, *args, **kwargs):
        self.logger.log(15, json.dumps([args, kwargs]))
        #kwargs['timeout'] = kwargs.get('timeout', self.__timeout)
        try:
            return self.__session.request(*args, **kwargs)
        except:
            self.logger.exception('Exception during api call')
            raise

    def _call_raw(self, method, endpoint, **kwargs):
        url = '/'.join([self.__base_url, endpoint])
        return self._do_request(method, url, **kwargs)

    def _call(self, method, endpoint,  **kwargs):
        res = self._call_raw(method, endpoint, **kwargs)
        try:
            result = res.json()
        except:
            self.logger.warning('Json decode error', exc_info=True)
            self.logger.log(15, res.text)
            raise
        try:
            res.raise_for_status()
        except:
            self.logger.exception('Server Error')
            self.logger.error(pformat(result))
            raise
        return result

    def list_dbs(self):
        return self._call('GET', 'dbs')

    def db(self, dbname, local_options=None, **kwargs):
        if local_options is None: local_options = {}
        return DB(self, self.open_db(dbname, **kwargs), **{**self.__config, **local_options})

    def open_db(self, dbname, **kwargs):
        endpoint = '/'.join(['db', urlquote(dbname, safe='')])
        return self._call('POST', endpoint, **kwargs)

    def searches(self):
        endpoint = '/'.join(['peers', 'searches'])
        return self._call('GET', endpoint)

    def events(self, eventnames):
        endpoint = '/'.join(['events', urlquote(eventnames, safe='')])
        res = self._call_raw('GET', endpoint, stream=True)
        res.raise_for_status()
        for event in SSEClient(res.stream()).events():
            event.data = json.loads(event.data)
            yield event
