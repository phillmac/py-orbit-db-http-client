import json
import logging
from pprint import pformat
from urllib.parse import quote as urlquote
import uuid

import httpx
from sseclient import SSEClient

from .asyncDB import DB


class OrbitDbAPI ():
    def __init__ (self, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.__config = kwargs
        self.__base_url = self.__config.get('base_url')
        self.__use_db_cache = self.__config.get('use_db_cache', True)
        self.__timeout = self.__config.get('timeout', 30)
        self.__headers = httpx.Headers(self.__config.get('headers', {}))
        self.__client = httpx.AsyncClient(
            headers=self.__headers,
            timeout=self.__timeout
        )
        self.__session = None
        self.__sseClients = []
        self.__dbs = []
        self.logger.debug(f'Base url: {self.__base_url}')
        self.logger.debug(f'Headers: {self.__headers.items()}')

    def __enter__(self):
        if not self.__session:
            self.create_session()
        return self

    def __exit__(self, type, value, tb):
        self.destroy_session()

    @property
    def raw_client(self):
        return self.__client

    @property
    def base_url(self):
        return self.__base_url

    @property
    def use_db_cache(self):
        return self.__use_db_cache

    def create_session(self):
        if self.__session:
            raise Exception('Session already registered')
        session = uuid.uuid4()
        endpoint = '/'.join(['sessions', session])
        self._call('POST', endpoint)
        self.__sesssion = session

    def destroy_session(self):
        endpoint = '/'.join(['sessions', self.__session])
        self._call('DELETE', endpoint)
        self.__session = None

    def close(self):
        for db in self.__dbs:
            db.close()
        for sseClient in self.__sseClients:
            sseClient.close()
        return self.__client.close()

    def _remove_db(self, db):
        del self.__dbs[self.__dbs.index(db)]

    def _do_request(self, *args, **kwargs):
        self.logger.log(15, json.dumps([args, kwargs]))
        #kwargs['timeout'] = kwargs.get('timeout', self.__timeout)
        try:
            return self.__client.request(*args, **kwargs)
        except:
            self.logger.exception('Exception during api call')
            raise

    def _call_raw(self, method, endpoint, **kwargs):
        url = '/'.join([self.__base_url, endpoint])
        return self._do_request(method, url, **kwargs)

    async def _call(self, method, endpoint,  **kwargs):
        res = await self._call_raw(method, endpoint, **kwargs)
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

    async def db(self, dbname, local_options=None, **kwargs):
        if local_options is None: local_options = {}
        db = DB(self, await self.open_db(dbname, **kwargs), **{**self.__config, **local_options})
        self.__dbs.append(db)
        return db

    async def open_db(self, dbname, **kwargs):
        endpoint = '/'.join(['db', urlquote(dbname, safe='')])
        return await self._call('POST', endpoint, **kwargs)

    def async_open_db(self, dbname, **kwargs):
        json = kwargs.get('json', {})
        json['awaitOpen'] = False
        endpoint = '/'.join(['db', urlquote(dbname, safe='')])
        events = self.events('open,ready,load')
        dbInfo = self._call('POST', endpoint, **kwargs)
        if (not 'ready' in dbInfo) or (not dbInfo['ready']):
            self.logger.info('Waiting for db to be ready...')

    def searches(self):
        endpoint = '/'.join(['peers', 'searches'])
        return self._call('GET', endpoint)

    def events(self, eventnames):
        endpoint = '/'.join(['events', urlquote(eventnames, safe='')])
        res = self._call_raw('GET', endpoint, stream=True)
        res.raise_for_status()
        sseClient = SSEClient(res.stream())
        self.__sseClients.append(sseClient)
        for event in sseClient.events():
            event.json = json.loads(event.data)
            yield event
        del self.__sseClients[self.__sseClients.index(sseClient)]


class SSEventStream(SSEClient):
    def __init__(self, parent, res, char_enc='utf-8'):
        super().__init__(self.eventStream(), char_enc)
        self.__parent = parent
        self.__res = res
        self.__complete = False
        self.logger = logging.getLogger(f'{__name__}.{__class__}')

    @property
    def complete(self):
        return self.__complete

    def eventStream(self):
        try:
            for b in self.__res.stream():
                if self.__complete:
                    raise StopIteration()
                yield b

        except StopIteration:
            self.__res.close()
            raise

    def __enter__(self):
        self.__parent._addSSEClient(self)
        return self

    def __exit__(self, type, value, tb):
            self.__complete = True
            self.__parent._removeSSEClient(self)

    # def awaitEvent(self, event_name):
    #     for event in events:
    #         if event.event == event_name:
    #             logger
