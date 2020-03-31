#!/usr/bin/env python
import json
import logging
import os
import asyncio
import random
import string
import sys
import unittest
from pprint import pformat
from time import sleep

from orbitdbapi.asyncClient import OrbitDbAPI

base_url=os.environ.get('ORBIT_DB_HTTP_API_URL')
timeout = int(os.environ.get('ORBIT_DB_HTTP_API_TIMEOUT', 120))

def randString(k=5, lowercase=False, both=False):
    if both:
        return ''.join(random.choices(string.ascii_letters + string.digits, k=k))
    if lowercase:
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=k))




class KVStoreTestCase(unittest.TestCase):
    def flushPendingTasks(self, tasks):
        self.loop.run_until_complete(
            asyncio.gather(*tasks)
        )

    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.flushPendingTasks([
            self.loop.create_task(self.asyncSetUp())
        ])

    async def asyncSetUp(self):
        self.client = OrbitDbAPI(
            base_url=base_url,
            use_db_cache=False,
            headers={'connection':'close'}, #TODO: See https://github.com/encode/httpx/issues/96
            timeout=timeout
        )
        self.kevalue_test = await self.client.db('keyvalue_test', json={'create':True, 'type': 'keyvalue'})

    def runTest(self):
        self.assertFalse(self.kevalue_test.cached)
        self.localKV = {}
        putTasks = []
        for _c in range(1,100):
            k = randString()
            v = randString(k=100, both=True)
            putTasks.append(self.loop.create_task(self.put(k,v)))
        self.flushPendingTasks(putTasks)

        self.flushPendingTasks([
            self.loop.create_task(self.verifyContents())
        ])

        delTasks = []
        self.deletedKeys = []
        for _c in range(1,75):
            delk = random.choice([k for k in self.localKV.keys() if not k in self.deletedKeys])
            self.deletedKeys.append(delk)
            delTasks.append(self.loop.create_task(self.remove(delk)))
        self.flushPendingTasks(delTasks)

        self.flushPendingTasks([
            self.loop.create_task(self.verifyDelete())
        ])


    async def put(self, k, v):
        self.localKV[k] = v
        await self.kevalue_test.put({'key':k, 'value':v})
        self.assertEqual(self.localKV.get(k), await self.kevalue_test.get(k))

    async def verifyContents(self):
        self.assertDictContainsSubset(self.localKV, await self.kevalue_test.all())

    async def verifyDelete(self):
        remoteKeys = await self.kevalue_test.all().keys()
        self.assertTrue(all(k not in remoteKeys for k in self.deletedKeys))

    async def remove(self, delk):
        await self.kevalue_test.remove(delk)

    def tearDown(self):
        self.flushPendingTasks([
            self.loop.create_task(self.asyncTearDown())
        ])

    async def asyncTearDown(self):
        await self.kevalue_test.unload()
        await self.client.close()



def main():
    loglvl = int(os.environ.get('LOG_LEVEL',15))
    print(f'Log level: {loglvl}')
    print(f'Timeout: {timeout} seconds')
    logfmt = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=logfmt, stream=sys.stdout, level=loglvl)
    unittest.main()

if __name__ == '__main__':
    main()