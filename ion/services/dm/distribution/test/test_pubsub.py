#!/usr/bin/env python

"""
@file ion/services/dm/test/test_pubsub.py
@author Michael Meisinger
@author David Stuebe
@brief test service for registering topics for data pub sub
"""

import logging
logging = logging.getLogger(__name__)
import time
from twisted.internet import defer
from twisted.trial import unittest
from magnet.container import Container
from magnet.spawnable import Receiver
from magnet.spawnable import spawn

from ion.core.base_process import ProtocolFactory
from ion.core import bootstrap
from ion.core.base_process import BaseProcess
from ion.services.dm.distribution.pubsub_service import DataPubsubClient
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.data import dataobject
from ion.resources.dm_resource_descriptions import Publication, PublisherResource, PubSubTopicResource, SubscriptionResource, DAPMessageObject

from ion.services.dm.util import dap_tools

from ion.services.dm.distribution import base_consumer
from ion.services.dm.distribution.consumers import forwarding_consumer
from ion.services.dm.distribution.consumers import logging_consumer
from ion.services.dm.distribution.consumers import example_consumer

import numpy

from ion.services.dm.util import dap_tools


class PubSubTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {'name':'pubsub_registry','module':'ion.services.dm.distribution.pubsub_registry','class':'DataPubSubRegistryService'},
            {'name':'pubsub_service','module':'ion.services.dm.distribution.pubsub_service','class':'DataPubsubService'}
            ]

        self.sup = yield self._spawn_processes(services)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_pubsub(self):

        dpsc = DataPubsubClient(self.sup)
        
        # Create and Register a topic
        topic = PubSubTopicResource.create('Davids Topic',"oceans, oil spill, fun things to do")        
        topic = yield dpsc.define_topic(topic)
        logging.info('Defined Topic: '+str(topic))


        #Create two test queues - don't use topics to test the consumer
        # To be replaced when the subscription service is ready
        queue1=dataobject.create_unique_identity()
        queue_properties = {queue1:{'name_type':'fanout', 'args':{'scope':'global'}}}
        yield bootstrap.declare_messaging(queue_properties)

        queue2=dataobject.create_unique_identity()
        queue_properties = {queue2:{'name_type':'fanout', 'args':{'scope':'global'}}}
        yield bootstrap.declare_messaging(queue_properties)

    
        #Create and register self.sup as a publisher
        print 'SUP',self.sup,self.test_sup
        
        publisher = PublisherResource.create('Test Publisher', self.sup, topic, 'DataObject')
        publisher = yield dpsc.define_publisher(publisher)

        logging.info('Defined Publisher: '+str(publisher))


        pd1={'name':'example_consumer_1',
                 'module':'ion.services.dm.distribution.consumers.forwarding_consumer',
                 'procclass':'ForwardingConsumer',
                 'spawnargs':{'attach':topic.queue.name,\
                              'Process Parameters':\
                              {'queues':[queue1,queue2]}}\
                    }
        child1 = base_consumer.ConsumerDesc(**pd1)

        child1_id = yield self.test_sup.spawn_child(child1)


        # Create and send a data message
        data = {'Data':'in a dictionary'}
        result = yield dpsc.publish(self.sup, topic.reference(), data)
        if result:
            logging.info('Published Message')
        else:
            logging.info('Failed to Published Message')

        # Need to await the delivery of data messages into the (separate) consumers
        yield pu.asleep(1)

        msg_cnt = yield child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent.get(queue1),1)
        self.assertEqual(sent.get(queue2),1)
        self.assertEqual(received.get(topic.queue.name),1)

        pd2={'name':'example_consumer_2',
                 'module':'ion.services.dm.distribution.consumers.logging_consumer',
                 'procclass':'LoggingConsumer',
                 'spawnargs':{'attach':queue1,\
                              'Process Parameters':{}}\
                    }
        child2 = base_consumer.ConsumerDesc(**pd2)

        child2_id = yield self.test_sup.spawn_child(child2)

        # Send the simple message again
        result = yield dpsc.publish(self.sup, topic.reference(), data)
        
        # Need to await the delivery of data messages into the (separate) consumers
        yield pu.asleep(1)

        msg_cnt = yield child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent.get(queue1),2)
        self.assertEqual(sent.get(queue2),2)
        self.assertEqual(received.get(topic.queue.name),2)

        msg_cnt = yield child2.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent,{})
        self.assertEqual(received.get(queue1),1)

        '''
        # Create and send a data message - a simple string
        data = 'a string of data' # usually not a great idea!
        result = yield dpsc.publish(self.sup, topic.reference(), data)
        if result:
            logging.info('Published Message')
        else:
            logging.info('Failed to Published Message')

        # Need to await the delivery of data messages into the (separate) consumers
        yield pu.asleep(1)

        self.assertEqual(dc1.receive_cnt, 3)
        self.assertEqual(dc2.receive_cnt, 2)
        '''



    @defer.inlineCallbacks
    def test_chainprocess(self):
        # This test covers a chain of three data consumer processes on three
        # topics. One process is an event-detector and data-filter, sending
        # event messages to an event queue and a new data message to a different
        # data queue


        dpsc = DataPubsubClient(self.sup)
        
        #Create and register 3 topics!
        topic_raw = PubSubTopicResource.create("topic_raw","oceans, oil spill, fun things to do") 
        topic_raw = yield dpsc.define_topic(topic_raw)

        topic_qc = PubSubTopicResource.create("topic_qc","oceans_qc, oil spill") 
        topic_qc = yield dpsc.define_topic(topic_qc)
        
        topic_evt = PubSubTopicResource.create("topic_evt", "spill events")
        topic_evt = yield dpsc.define_topic(topic_evt)


        #Create and register self.sup as a publisher
        publisher = PublisherResource.create('Test Publisher', self.sup, topic_raw, 'DataObject')
        publisher = yield dpsc.define_publisher(publisher)

        logging.info('Defined Publisher: '+str(publisher))

        pd1={'name':'example_consumer_1',
                 'module':'ion.services.dm.distribution.consumers.forwarding_consumer',
                 'procclass':'ForwardingConsumer',
                 'spawnargs':{'attach':topic.queue.name,\
                              'Process Parameters':\
                              {'event_queue':evt_queue,\
                               'processed_queue':pr_queue}}\
                    }

        dc1 = DataConsumer()
        dc1_id = yield dc1.spawn()
        # Use subscribe - does not exist yet
        yield dc1.attach(topic_raw)
        
        dc1.set_ondata(e_process,topic_qc,topic_evt)
        

        dc2 = DataConsumer()
        dc2_id = yield dc2.spawn()
        # Use subscribe - does not exist yet
        yield dc2.attach(topic_qc)

        dc3 = DataConsumer()
        dc3_id = yield dc3.spawn()
        # Use subscribe - does not exist yet
        yield dc3.attach(topic_evt)

        # Create an example data message with time
        dmsg = dap_tools.simple_datamessage(\
            {'DataSet Name':'Simple Data','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':(101,102,103,104,105,106,107,108,109,110), \
            'height':(5,2,4,5,-1,9,3,888,3,4)})
        
        result = yield dpsc.publish(self.sup, topic_raw.reference(), dmsg)
        if result:
            logging.info('Published Message')
        else:
            logging.info('Failed to Published Message')


        # Need to await the delivery of data messages into the consumers
        yield pu.asleep(2)

        # asser that the correct number of message have been received
        self.assertEqual(dc1.receive_cnt, 1)
        self.assertEqual(dc2.receive_cnt, 1)
        self.assertEqual(dc3.receive_cnt, 2)

        # Publish a second message with different data
        dmsg = dap_tools.simple_datamessage(\
            {'DataSet Name':'Simple Data','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':(111,112,123,114,115,116,117,118,119,120), \
            'height':(8,6,4,-2,-1,5,3,1,4,5)})
        
        result = yield dpsc.publish(self.sup, topic_raw.reference(), dmsg)

        # Need to await the delivery of data messages into the consumers
        yield pu.asleep(2)


        # Assert that the correct number of message have been received
        self.assertEqual(dc1.receive_cnt, 2)
        self.assertEqual(dc2.receive_cnt, 2)
        self.assertEqual(dc3.receive_cnt, 4)

