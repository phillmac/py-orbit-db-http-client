#!/usr/bin/env python
import json
import logging
import os
import random
import string
import sys
import unittest
from pprint import pformat
from time import sleep

from orbitdbapi.client import OrbitDbAPI

base_url = os.environ.get('ORBIT_DB_HTTP_API_URL')
timeout = int(os.environ.get('ORBIT_DB_HTTP_API_TIMEOUT', 120))


def randString(k=5, lowercase=False, both=False):
    if both:
        return ''.join(random.choices(string.ascii_letters + string.digits, k=k))
    if lowercase:
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=k))
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=k))


class CapabilitiesTestCase(unittest.TestCase):
    def setUp(self):
        self.client = OrbitDbAPI(
            base_url=base_url,
            # TODO: See https://github.com/encode/httpx/issues/96
            headers={'connection': 'close'},
            timeout=timeout
        )
        self.kevalue_test = self.client.db('keyvalue_cap_test', json={
                                           'create': True, 'type': 'keyvalue'})
        self.feed_test = self.client.db(
            'feed_cap_test', json={'create': True, 'type': 'feed'})
        self.event_test = self.client.db(
            'event_cap_test', json={'create': True, 'type': 'eventlog'})
        self.docstore_test = self.client.db('docstore_cap_test', json={
                                            'create': True, 'type': 'docstore'})
        self.counter_test = self.client.db('counter_cap_test', json={
                                           'create': True, 'type': 'counter'})

    def runTest(self):
        self.assertEqual(set(['get', 'put', 'remove']),
                         set(self.kevalue_test.capabilities))
        self.assertEqual(set(['add', 'get', 'iterator', 'remove']), set(
            self.feed_test.capabilities))
        self.assertEqual(set(['add', 'get', 'iterator']),
                         set(self.event_test.capabilities))
        self.assertEqual(set(['get', 'put', 'putAll', 'query', 'remove']), set(
            self.docstore_test.capabilities))
        self.assertEqual(set(['inc', 'value']), set(
            self.counter_test.capabilities))

    def tearDown(self):
        self.kevalue_test.unload()
        self.feed_test.unload()
        self.event_test.unload()
        self.docstore_test.unload()
        self.counter_test.unload()
        self.client.close()


class CounterIncrementTestCase(unittest.TestCase):
    def setUp(self):
        self.client = OrbitDbAPI(
            base_url=base_url,
            # TODO: See https://github.com/encode/httpx/issues/96
            headers={'connection': 'close'},
            timeout=timeout
        )
        self.counter_test = self.client.db('counter_store_test', json={
                                           'create': True, 'type': 'counter'})

    def runTest(self):
        localVal = self.counter_test.value()
        self.assertEqual(localVal, self.counter_test.value())
        for _c in range(1, 100):
            incVal = random.randrange(1, 100)
            localVal += incVal
            self.counter_test.inc(incVal)
            self.assertEqual(localVal, self.counter_test.value())

    def tearDown(self):
        self.counter_test.unload()


class KVStoreTestCase(unittest.TestCase):
    def setUp(self):
        self.client = OrbitDbAPI(
            base_url=base_url,
            use_db_cache=False,
            # TODO: See https://github.com/encode/httpx/issues/96
            headers={'connection': 'close'},
            timeout=timeout
        )
        self.kevalue_test = self.client.db('keyvalue_store_test', json={
                                           'create': True, 'type': 'keyvalue'})

    def runTest(self):
        self.assertFalse(self.kevalue_test.cached)
        localKV = {}
        for _c in range(1, 100):
            k = randString()
            v = randString(k=100, both=True)
            localKV[k] = v
            self.kevalue_test.put({'key': k, 'value': v})
            self.assertEqual(localKV.get(k), self.kevalue_test.get(k))

        self.assertDictContainsSubset(localKV, self.kevalue_test.all())

        DeletedKeys = []
        for _c in range(1, 75):
            delk = random.choice(
                [k for k in localKV.keys() if not k in DeletedKeys])
            DeletedKeys.append(delk)
            self.kevalue_test.remove(delk)
        remoteKeys = self.kevalue_test.all().keys()
        self.assertTrue(all(k not in remoteKeys for k in DeletedKeys))

    def tearDown(self):
        self.kevalue_test.unload()
        self.client.close()


class DocStoreTestCase(unittest.TestCase):
    def setUp(self):
        self.client = OrbitDbAPI(
            base_url=base_url,
            use_db_cache=False,
            # TODO: See https://github.com/encode/httpx/issues/96
            headers={'connection': 'close'},
            timeout=timeout
        )
        self.docstore_test = self.client.db('docstore_store_test', json={
                                            'create': True, 'type': 'docstore'})

    def runTest(self):
        self.assertFalse(self.docstore_test.cached)
        localDocs = []
        for _c in range(1, 100):
            k = randString()
            v = randString(k=100, both=True)
            item = {'_id': k, 'value': v}
            localDocs.append(item)
            self.docstore_test.put(item)
            self.assertDictContainsSubset(item, self.docstore_test.get(k)[0])

        remoteDocs = self.docstore_test.all()
        self.assertTrue(all(item in remoteDocs for item in localDocs))

        DeletedDocs = []
        for _c in range(1, 75):
            item = random.choice(
                [d for d in localDocs if not d in DeletedDocs])
            DeletedDocs.append(item)
            self.docstore_test.remove(item['_id'])
        remoteDocs = self.docstore_test.all()
        self.assertTrue(all(d not in remoteDocs for d in DeletedDocs))

    def tearDown(self):
        self.docstore_test.unload()
        self.client.close()


if __name__ == '__main__':
    loglvl = int(os.environ.get('LOG_LEVEL', 15))
    print(f'Log level: {loglvl}')
    print(f'Timeout: {timeout} seconds')
    logfmt = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=logfmt, stream=sys.stdout, level=loglvl)
    unittest.main()
