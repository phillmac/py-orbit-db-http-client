#!/usr/bin/env python
import json
import logging
import os
import random
import string
import sys
import unittest
from time import sleep

from orbitdbapi.client import OrbitDbAPI

base_url=os.environ.get('ORBIT_DB_HTTP_API_URL')

def randString(k=5, lowercase=False, both=False):
    if both:
        return ''.join(random.choices(string.ascii_letters + string.digits, k=k))
    if lowercase:
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=k))
    return  ''.join(random.choices(string.ascii_uppercase + string.digits, k=k))

class CapabilitiesTestCase(unittest.TestCase):
    def setUp(self):
        client = OrbitDbAPI(base_url=base_url)
        self.kevalue_test = client.db('keyvalue_test', json={'create':True, 'type': 'keyvalue'})
        self.feed_test = client.db('feed_test', json={'create':True, 'type': 'feed'})
        self.event_test = client.db('event_test', json={'create':True, 'type': 'eventlog'})
        self.docstore_test = client.db('docstore_test', json={'create':True, 'type': 'docstore'})
        self.counter_test = client.db('counter_test', json={'create':True, 'type': 'counter'})

    def runTest(self):
        self.assertEqual(set(['get', 'put', 'remove']), set(self.kevalue_test.capabilities))
        self.assertEqual(set(['add', 'get', 'iterator', 'remove']), set(self.feed_test.capabilities))
        self.assertEqual(set(['add', 'get', 'iterator']), set(self.event_test.capabilities))
        self.assertEqual(set(['get', 'put', 'query', 'remove']), set(self.docstore_test.capabilities))
        self.assertEqual(set(['inc', 'value']), set(self.counter_test.capabilities))

    def tearDown(self):
        self.kevalue_test.unload()
        self.feed_test.unload()
        self.event_test.unload()
        self.docstore_test.unload()
        self.counter_test.unload()

class CounterIncrementTestCase(unittest.TestCase):
    def setUp(self):
        client = OrbitDbAPI(base_url=base_url)
        self.counter_test = client.db('counter_test', json={'create':True, 'type': 'counter'})

    def runTest(self):
        localVal = self.counter_test.value()
        self.assertEqual(localVal, self.counter_test.value())
        for _c in range(1,100):
            incVal = random.randrange(1,100)
            localVal += incVal
            self.counter_test.inc(incVal)
            self.assertEqual(localVal, self.counter_test.value())

    def tearDown(self):
        self.counter_test.unload()


class KVStoreGetPutTestCase(unittest.TestCase):
    def setUp(self):
        client = OrbitDbAPI(base_url=base_url, use_db_cache=False)
        self.kevalue_test = client.db('keyvalue_test', json={'create':True, 'type': 'keyvalue'})

    def runTest(self):
        self.assertFalse(self.kevalue_test.cached)
        localKV = {}
        for _c in range(1,100):
            k = randString()
            v = randString(k=100, both=True)
            localKV[k] = v
            self.kevalue_test.put({'key':k, 'value':v})
            self.assertEqual(localKV.get(k), self.kevalue_test.get(k))
            self.assertDictContainsSubset(localKV, self.kevalue_test.all())

    def tearDown(self):
        self.kevalue_test.unload()

class DocStoreGetPutTestCase(unittest.TestCase):
    def setUp(self):
        client = OrbitDbAPI(base_url=base_url, use_db_cache=False)
        self.docstore_test = client.db('docstore_test', json={'create':True, 'type': 'docstore'})

    def runTest(self):
        self.assertFalse(self.docstore_test.cached)
        localDocs = []
        for _c in range(1,100):
            k = randString()
            v = randString(k=100, both=True)
            item = {'_id':k, 'value':v}
            localDocs.append(item)
            self.docstore_test.put(item)
            self.assertDictContainsSubset(item, self.docstore_test.get(k)[0])
        self.assertTrue(all(item in self.docstore_test.all() for item in localDocs))

    def tearDown(self):
        self.docstore_test.unload()


class SearchesTestCase(unittest.TestCase):
    def setUp(self):
        self.client = OrbitDbAPI(base_url=base_url)
        self.kevalue_test = self.client.db('keyvalue_test', json={'create':True, 'type': 'keyvalue'})


    def runTest(self):
        self.kevalue_test.findPeers()
        searches = self.client.searches()
        self.assertGreater(len(searches), 0)
        self.assertGreater(len([s for s in searches if s.get('searchID') == self.kevalue_test.id]), 0)

    def tearDown(self):
        self.kevalue_test.unload()

class SearchPeersTestCase(unittest.TestCase):
    def setUp(self):
        self.client = OrbitDbAPI(base_url=base_url)
        self.kevalue_test = self.client.db('zdpuAuSAkDDRm9KTciShAcph2epSZsNmfPeLQmxw6b5mdLmq5/keyvalue_test')

    def runTest(self):
        self.kevalue_test.findPeers(useCustomFindProvs=True)
        dbPeers = []
        count = 0
        while len(dbPeers) < 1:
            sleep(5)
            dbPeers = self.kevalue_test.getPeers()
            if count > 60: break
        self.assertGreater(len(dbPeers), 0)


#     def tearDown(self):
#         self.kevalue_test.unload()



if __name__ == '__main__':
    loglvl = int(os.environ.get('LOG_LEVEL',15))
    print(f'Log level: {loglvl}')
    logfmt = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=logfmt, stream=sys.stdout, level=loglvl)
    unittest.main()
