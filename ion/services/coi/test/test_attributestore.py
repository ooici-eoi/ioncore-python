#!/usr/bin/env python

"""
@file ion/services/coi/test/test_datastore.py
@author Michael Meisinger
@brief test datastore service
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.core import base_process
from ion.services.coi.attributestore import AttributeStoreService, AttributeStoreClient
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu


class AttrStoreServiceTest(IonTestCase):
    """
    Testing service classes of attribute store
    """
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_put_seperate_backend(self):
        # Test with seperate store backends
        services = [
            {'name':'attstore1','module':'ion.services.coi.attributestore','class':'AttributeStoreService','spawnargs':{'servicename':'as1','backend_class':'ion.data.store.Store','backend_args':{}}},
            {'name':'attstore2','module':'ion.services.coi.attributestore','class':'AttributeStoreService','spawnargs':{'servicename':'as2','backend_class':'ion.data.store.Store','backend_args':{}}},
        ]

        sup = yield self._spawn_processes(services)

        asc1 = AttributeStoreClient(proc=sup, targetname='as1')

        res1 = yield asc1.put('key1','value1')
        logging.info('Result1 put: '+str(res1))

        res2 = yield asc1.get('key1')
        logging.info('Result2 get: '+str(res2))
        self.assertEqual(res2, 'value1')

        res3 = yield asc1.put('key1','value2')

        res4 = yield asc1.get('key1')
        self.assertEqual(res4, 'value2')

        res5 = yield asc1.get('non_existing')
        self.assertEqual(res5, None)

        asc2 = AttributeStoreClient(proc=sup, targetname='as2')

        resx1 = yield asc2.get('key1')
        self.assertEqual(resx1, None)
        
        
        
    @defer.inlineCallbacks
    def test_put_common_backend(self):
        # Test with cassandra store backend where both services can access common values!
        services = [
            {'name':'attstore1',
             'module':'ion.services.coi.attributestore',
             'class':'AttributeStoreService',
             'spawnargs':{'servicename':'as1',
                            'backend_class':'ion.data.backends.cassandra.CassandraStore',
                            'backend_args':{'cass_host_list':['amoeba.ucsd.edu:9160'],
                                        'keyspace':'Datastore',
                                        'colfamily':'DS1',
                                        'cf_super':True,
                                        'namespace':None,
                                        }}},
            {'name':'attstore2',
            'module':'ion.services.coi.attributestore',
            'class':'AttributeStoreService',
            'spawnargs':{'servicename':'as2',
                        'backend_class':'ion.data.backends.cassandra.CassandraStore',
                        'backend_args':{'cass_host_list':['amoeba.ucsd.edu:9160'],
                                        'keyspace':'Datastore',
                                        'colfamily':'DS1',
                                        'cf_super':True,
                                        'namespace':None,
                                        }}}
                    ]

        sup = yield self._spawn_processes(services)

        asc1 = AttributeStoreClient(proc=sup, targetname='as1')

        res1 = yield asc1.put('key1','value1')
        logging.info('Result1 put: '+str(res1))

        res2 = yield asc1.get('key1')
        logging.info('Result2 get: '+str(res2))
        self.assertEqual(res2, 'value1')

        res3 = yield asc1.put('key1','value2')

        res4 = yield asc1.get('key1')
        self.assertEqual(res4, 'value2')

        res5 = yield asc1.get('non_existing')
        self.assertEqual(res5, None)

        asc2 = AttributeStoreClient(proc=sup, targetname='as2')

        resx1 = yield asc2.get('key1')
        self.assertEqual(resx1, 'value2')
