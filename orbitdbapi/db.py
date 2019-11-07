import json
import logging
from collections.abc import Hashable, Iterable
from copy import deepcopy
from urllib.parse import quote as urlquote

from sseclient import SSEClient


class DB ():
    def __init__(self, client, params, **kwargs):
        self.__cache = {}
        self.__client = client
        self.__params = params
        self.__db_options = params.get('options', {})
        self.__dbname = params['dbname']
        self.__id = params['id']
        self.__id_safe = urlquote(self.__id, safe='')
        self.__type = params['type']
        self.__use_cache = kwargs.get('use_db_cache', client.use_db_cache)
        self.__enforce_caps = kwargs.get('enforce_caps', True)
        self.__enforce_indexby = kwargs.get('enforce_indexby', True)
        self.__index_by = self.__db_options.get('indexBy')
        self.__sseClients = []
        self.logger = logging.getLogger(__name__)


    def clear_cache(self):
        self.__cache = {}

    def cache_get(self, item):
        item = str(item)
        return deepcopy(self.__cache.get(item))

    def cache_remove(self, item):
        item = str(item)
        if item in self.__cache:
            del self.__cache[item]

    @property
    def cached(self):
        return self.__use_cache

    @property
    def index_by(self):
        return self.__index_by

    @property
    def cache(self):
        return deepcopy(self.__cache)

    @property
    def params(self):
        return deepcopy(self.__params)

    @property
    def dbname(self):
        return self.__dbname

    @property
    def id(self):
        return self.__id

    @property
    def dbtype(self):
        return self.__type

    @property
    def capabilities(self):
        return deepcopy(self.__params.get('capabilities', []))

    @property
    def queryable(self):
        return 'query' in self.__params.get('capabilities', [])

    @property
    def putable(self):
        return 'put' in self.__params.get('capabilities', [])

    @property
    def removeable(self):
        return 'remove' in self.__params.get('capabilities', [])

    @property
    def iterable(self):
        return 'iterator' in self.__params.get('capabilities', [])

    @property
    def addable(self):
        return 'add' in self.__params.get('capabilities', [])

    @property
    def valuable(self):
        return 'value' in self.__params.get('capabilities', [])

    @property
    def incrementable(self):
        return 'inc' in self.__params.get('capabilities', [])

    @property
    def indexed(self):
        return 'indexBy' in self.__db_options

    @property
    def can_append(self):
        return self.__params.get('canAppend')

    @property
    def write_access(self):
        return deepcopy(self.__params.get('write'))

    def close(self):
        for sseClient in self.__sseClients:
            sseClient.close()
        self.__client._remove_db(self)

    def info(self):
        endpoint = '/'.join(['db', self.__id_safe])
        return self.__client._call('GET', endpoint)

    def get(self, item, cache=None, unpack=False):
        if cache is None: cache = self.__use_cache
        item = str(item)
        if cache and item in self.__cache:
            result = self.__cache[item]
        else:
            endpoint = '/'.join(['db', self.__id_safe, item])
            result = self.__client._call('GET', endpoint)
            if cache: self.__cache[item] = result
        if isinstance(result, Hashable): return deepcopy(result)
        if isinstance(result, Iterable): return deepcopy(result)
        if unpack:
            if isinstance(result, Iterable): return deepcopy(next(result, {}))
            if isinstance(result, list): return deepcopy(next(iter(result), {}))
        return result

    def get_raw(self, item):
        endpoint = '/'.join(['db', self.__id_safe, 'raw', str(item)])
        return (self.__client._call('GET', endpoint))

    def put(self,  item, cache=None):
        if self.__enforce_caps and not self.putable:
            raise CapabilityError(f'Db {self.__dbname} does not have put capability')
        if self.indexed and (not self.__index_by in item) and self.__enforce_indexby:
            raise MissingIndexError(f"The provided document {item} doesn't contain field '{self.__index_by}'")

        if cache is None: cache = self.__use_cache
        if cache:
            if self.indexed and hasattr(item, self.__index_by):
                    index_val = getattr(item, self.__index_by)
            else:
                index_val = item.get('key')
            if index_val:
                self.__cache[index_val] = item
        endpoint = '/'.join(['db', self.__id_safe, 'put'])
        entry_hash = self.__client._call('POST', endpoint, json=item).get('hash')
        if cache and entry_hash: self.__cache[entry_hash] = item
        return entry_hash

    def add(self, item, cache=None):
        if self.__enforce_caps and not self.addable:
            raise CapabilityError(f'Db {self.__dbname} does not have add capability')
        if cache is None: cache = self.__use_cache
        endpoint = '/'.join(['db', self.__id_safe, 'add'])
        entry_hash = self.__client._call('POST', endpoint, json=item).get('hash')
        if cache and entry_hash: self.__cache[entry_hash] = item
        return entry_hash

    def inc(self, val):
        val = int(val)
        endpoint = '/'.join(['db', self.__id_safe, 'inc'])
        return self.__client._call('POST', endpoint, json={'val':val})

    def value(self):
        endpoint = '/'.join(['db', self.__id_safe, 'value'])
        return self.__client._call('GET', endpoint)

    def iterator_raw(self, **kwargs):
        if self.__enforce_caps and not self.iterable:
            raise CapabilityError(f'Db {self.__dbname} does not have iterator capability')
        endpoint =  '/'.join(['db', self.__id_safe, 'rawiterator'])
        return self.__client._call('GET', endpoint, json=kwargs)

    def iterator(self, **kwargs):
        if self.__enforce_caps and not self.iterable:
            raise CapabilityError(f'Db {self.__dbname} does not have iterator capability')
        endpoint =  '/'.join(['db', self.__id_safe, 'iterator'])
        return self.__client._call('GET', endpoint, json=kwargs)

    def index(self):
        endpoint = '/'.join(['db', self.__id_safe, 'index'])
        result = self.__client._call('GET', endpoint)
        return result

    def all(self):
        endpoint = '/'.join(['db', self.__id_safe, 'all'])
        result = self.__client._call('GET', endpoint)
        if isinstance(result, Hashable):
            self.__cache = result
        return result

    def remove(self, item):
        if self.__enforce_caps and not self.removeable:
            raise CapabilityError(f'Db {self.__dbname} does not have remove capability')
        item = str(item)
        endpoint = '/'.join(['db', self.__id_safe, item])
        return self.__client._call('DELETE', endpoint)

    def unload(self):
        self.close()
        endpoint = '/'.join(['db', self.__id_safe])
        return self.__client._call('DELETE', endpoint)

    def events(self, eventnames):
        endpoint = '/'.join(['db', self.__id_safe, 'events', urlquote(eventnames, safe='')])
        res = self.__client._call_raw('GET', endpoint, stream=True)
        res.raise_for_status()
        sseClient = SSEClient(res.stream())
        self.__sseClients.append(sseClient)
        for event in sseClient.events():
            event.json = json.loads(event.data)
            yield event
        del self.__sseClients[self.__sseClients.index(sseClient)]

    def findPeers(self, **kwargs):
        endpoint = '/'.join(['peers','searches','db', self.__id_safe])
        return self.__client._call('POST', endpoint, json=kwargs)

    def getPeers(self):
        endpoint = '/'.join(['db', self.__id_safe, 'peers'])
        return self.__client._call('GET', endpoint)


class CapabilityError(Exception):
    pass

class MissingIndexError(Exception):
    pass
