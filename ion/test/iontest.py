#!/usr/bin/env python

"""
@file ion/test/iontest.py
@author Michael Meisinger
@brief test case for ION integration and system test cases (and some unit tests)
"""

from twisted.trial import unittest
from twisted.internet import defer, reactor

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import bootstrap, ioninit
from ion.core import ioninit
from ion.core.cc import service
from ion.core.cc import container
from ion.core.cc.container import Id, Container
from ion.core.messaging.receiver import Receiver
from ion.core.process import process
from ion.core.process.process import IProcess, Process
from ion.data.store import Store
import ion.util.procutils as pu
import os

from ion.resources import description_utility

from ion.core.process.process import request

# The following modules must be imported here, because they load config
# files. If done while in test, it does not work!

CONF = ioninit.config(__name__)

class IonTestCase(unittest.TestCase):
    """
    Extension of python unittest.TestCase and trial unittest.TestCase for the
    purposes of supporting ION tests with a container/AMQP based execution
    environment.
    Use this as a base case for your unit tests, e.g.
     class DatastoreTest(IonTestCase):
    """

    # Set timeout for Trial tests
    timeout = 20
    procRegistry = process.procRegistry
    container = None
    twisted_container_service = None #hack

    @defer.inlineCallbacks
    def _start_container(self, sysname=None):
        """
        Starting and initialzing the container with a connection to a broker.
        """
        #mopt = {}
        mopt = service.Options()
        if os.environ.has_key("ION_TEST_CASE_BROKER_HOST"):
            host = os.environ["ION_TEST_CASE_BROKER_HOST"]
            log.debug("Using environment set ION_TEST_CASE_BROKER_HOST (%s)" % host)
            mopt["broker_host"] = host
        else:
            mopt['broker_host'] = CONF['broker_host']
        mopt['broker_port'] = CONF['broker_port']
        mopt['broker_vhost'] = CONF['broker_vhost']
        mopt['broker_heartbeat'] = CONF['broker_heartbeat']
        mopt['no_shell'] = True
        mopt['scripts'] = [CONF['start_app']]

        # Little trick to have no consecutive failures if previous setUp() failed
        if Container._started:
            log.error("PROBLEM: Previous test did not stop container. Fixing...")
            yield self._stop_container()

        #self.container = container.create_new_container()
        #yield self.container.initialize(mopt)
        #yield self.container.activate()
        twisted_container_service = service.CapabilityContainer(mopt)
        yield twisted_container_service.startService()
        self.twisted_container_service = twisted_container_service

        # Manually perform some ioncore initializations
        if sysname == None and os.environ.has_key("ION_TEST_CASE_SYSNAME"):
            sysname = os.environ["ION_TEST_CASE_SYSNAME"]

        # save off the old container args
        self._old_container_args = None
        self._reset_container_args = False
        if sysname != None:
            log.info("Setting _start_container sysname to %s" % sysname)
            self._old_container_args = Container.args
            self._reset_container_args = True
            curargs = ''
            if Container.args:
                curargs = Container.args + ' '

            Container.args = curargs + "sysname=%s" % sysname       # making this last will override sysname if it's set twice due to parsing

        yield bootstrap.init_ioncore()      # calls set_container_args

        self.procRegistry = process.procRegistry
        self.test_sup = yield bootstrap.create_supervisor()

        log.info("============ %s ===" % self.container)

    @defer.inlineCallbacks
    def create_message(self, MessageContentTypeID, MessageName='', **kwargs):
        """
        @Brief This is a convienence method for creating message instances in a
        test case. The message instance is an envelope for application specific
        messages. The IonTest uses the message client built into the test
        supervisor process.
        @param MessageContentTypeID is the type of message to go in the message instance
        @param MessageName is a depricated concept in the architecture. Do not use it.
        @param kwargs The IonTest method allows key word arguments which will
        attempt to set fields in the message object. The message client does not
        provide this capability, but it could be added if it is helpful.
        """
        msg_instance = yield self.test_sup.message_client.create_instance(MessageContentTypeID, MessageName)
        for k,v in kwargs.items():
            setattr(msg_instance,k,v)
        defer.returnValue(msg_instance)

    @defer.inlineCallbacks
    def _start_core_services(self):
        sup = yield bootstrap.spawn_processes(bootstrap.ion_core_services,
                                              self.test_sup)
        log.info("============Core ION services started============")
        defer.returnValue(sup)

    @defer.inlineCallbacks
    def _stop_container(self):
        """
        Taking down the container's connection to the broker an preparing for
        reinitialization.
        """
        log.info("Closing ION container")
        if self.twisted_container_service: #hack
            yield self.twisted_container_service.stopService()

        self.test_sup = None
        # Cancel any delayed calls, such as timeouts, looping calls etc.
        dcs = reactor.getDelayedCalls()
        if len(dcs) > 0:
            log.debug("Cancelling %s delayed reactor calls!" % len(dcs))
        for dc in dcs:
            # Cancel the registered delayed call (this is non-async)
            dc.cancel()

        # The following is waiting a bit for any currently consumed messages
        # It also prevents the arrival of new messages
        Receiver.rec_shutoff = True
        msgstr = ""
        if len(Receiver.rec_messages) > 0:
            for msg in Receiver.rec_messages.values():
                msgstr += str(msg.payload) + ", \n"
            #log.warn("Content rec_messages: "+str(Receiver.rec_messages))
            log.warn("%s messages still being processed: %s" % (len(Receiver.rec_messages), msgstr))

        num_wait = 0
        while len(Receiver.rec_messages) > 0 and num_wait<10:
            yield pu.asleep(0.2)
            num_wait += 1

        #if self.container:
        #    yield self.container.terminate()
        #elif ioninit.container_instance:
        #    yield ioninit.container_instance.terminate()

        # fix Container args if we messed with them
        if self._reset_container_args:
            Container.args = self._old_container_args
            self._old_container_args = None
            self._reset_container_args = False
            # bootstrap.reset_container() will kill the stuff that bootstrap._set_container_args (called by bootstrap.init_ioncore) will set

        # Reset static module values back to initial state for next test case
        bootstrap.reset_container()

        log.info("============ION container closed============")

        if msgstr:
            raise RuntimeError("Unexpected message processed during container shutdown: "+msgstr)

    def _shutdown_processes(self, proc=None):
        """
        Shuts down spawned test processes.
        """
        if proc:
            return proc.shutdown()
        else:
            return self.test_sup.shutdown()

    def _declare_messaging(self, messaging):
        return bootstrap.declare_messaging(messaging)

    def _spawn_processes(self, procs, sup=None):
        sup = sup if sup else self.test_sup
        return bootstrap.spawn_processes(procs, sup)

    def _spawn_process(self, process):
        return process.spawn()

    def _get_procid(self, name):
        """
        @param name  process instance label given when spawning
        @retval process id of the process (locally) identified by name
        """
        return self.procRegistry.get(name)

    def _get_procinstance(self, pid):
        """
        @param pid  process id
        @retval Process instance for process id
        """
        process = ioninit.container_instance.proc_manager.process_registry.kvs.get(pid, None)
        return process
                
    def run(self, result):
        # TODO This is where we can inject user id and expiry into test case
        # stack to allow running of tests within the policy enforcement environment
        # as well as force certain policy enforcement failures, etc.
        #request.user_id = 'MYUSERID'
        #request.expiry = '999999999'
        unittest.TestCase.run(self, result)        


class ReceiverProcess(Process):
    """
    A simple process that can send messages and tracks all received
    messages
    """
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)
        self.inbox = defer.DeferredQueue()
        self.inbox_count = 0

    def _dispatch_message(self, payload, msg, conv):
        """
        Dispatch of messages to operations within this process instance. The
        default behavior is to dispatch to 'op_*' functions, where * is the
        'op' message attribute.
        @retval deferred
        """
        log.info('ReceiverProcess: Received message op=%s from sender=%s' %
                     (msg.payload['op'], msg.payload['sender']))
        self.inbox.put(msg)
        self.inbox_count += 1
        return defer.succeed(True)

    def await_message(self):
        """
        @retval Deferred for arriving message
        """
        return self.inbox.get()

# Stuff for testing: Stubs, mock objects
fakeStore = Store()

class FakeMessage(object):
    """Instances of this object are given to receive functions and handlers
    by test cases, in lieu of carrot BaseMessage instances. Production code
    detects these and no send is done.
    """
    def __init__(self, payload=None):
        self.payload = payload

    @defer.inlineCallbacks
    def send(self, to, msg):
        self.sendto = to
        self.sendmsg = msg
        # Need to be a generator
        yield fakeStore.put('fake','fake')

class FakeSpawnable(object):
    def __init__(self, id=None):
        self.id = id or Id('fakec','fakep')

class FakeReceiver(object):
    """Instances of this object are given to send/spawn functions
    by test cases, in lieu of ion.core.messaging.receiver.Receiver instances.
    Production code detects these and no send is done.
    """
    def __init__(self, id=None):
        self.payload = None
        self.process = FakeSpawnable()
        self.group = 'fake'

    @defer.inlineCallbacks
    def send(self, to, msg):
        self.sendto = to
        self.sendmsg = msg
        # Need to be a generator
        yield fakeStore.put('fake','fake')

class ItvTestCase(IonTestCase):
    """
    Integration testing base class for use with trial/itv_trial.

    Tests a fully spawned system, either via CEI bootstrapping, or a locally spawned system via itv_trial.

    Read more at:
        https://confluence.oceanobservatories.org/display/CIDev/ITV+R1C3+Integration+Test+Framework

    To use, derive your test from ion.test.ItvTestCase and fill in the services class
    attribute with a list of apps your test needs. Apps are relative to the current working
    directory and typically reside in the res/apps subdir of ioncore-python.

    Example:

        class AttributeStoreTest(ItvTestCase):
            services = ["res/apps/attributestore.app"]  # start these apps prior to testing.

            @defer.inlineCallbacks
            def setUp(self):
                yield self._start_container()

            @defer.inlineCallbacks
            def tearDown(self):
                yield self._stop_container()

            @defer.inlineCallbacks
            def test_set_attr(self):
                asc = AttributeStoreClient()
                yield asc.put("hi", "hellothere")

                res = yield asc.get("hi")
                self.failUnless(res == "hellothere")

            @defer.inlineCallbacks
            def test_set_attr2(self):
                # "hi" is still set here, but only if test_set_attr is run first, be careful
                asc = AttributeStoreClient()
                res = yield asc.get("hi")
                self.failUnless(res == "hellothere")

    Important points:
    - The sysname parameter is required to get all the services and tests running on the same
      system. itv_trial takes care of this for you, but if you want to deploy these tests vs 
      a CEI spawned environment, you must set the environment variable ION_TEST_CASE_SYSNAME
      to be the same as the sysname the CEI environment was spawned with.

    """
    services = []


