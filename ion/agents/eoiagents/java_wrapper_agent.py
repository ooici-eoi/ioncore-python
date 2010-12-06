#!/usr/bin/env python

"""
@file:   ion/agents/eoiagents/java_wrapper_agent.py
@author: Chris Mueller
@author: Tim LaRocque
@brief:  EOI JavaWrapperAgent and JavaWrapperAgentClient class definitions
"""

import subprocess
import ion.util.ionlog
import ion.util.procutils as pu

from twisted.internet import defer
from ion.core.process.process import Process, ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient


log = ion.util.ionlog.getLogger(__name__)


class JavaWrapperAgent(ServiceProcess):
    """
    Class designed to facilitate tight interaction with ION in leu of a complete Java CC, including:
    Agent registration, process lifecycle, and reactivity to other core ION services
    """
    
    
    declare = ServiceProcess.service_declare(name='java_wrapper_agent',
                                             version='0.1.0',
                                             dependencies=[]) # no dependencies

        
    def __init__(self, *args, **kwargs):
        '''
        TODO: document this
        '''
        # Step 1: Delegate initialization to parent "ServiceProcess"
        log.info('Initializing class instance')
        log.info('proc-name: ' + __name__ + '.JavaWraperAgent')
        #ServiceProcess.__init__(self, spawnargs={'proc-name': __name__ + '.JavaWrapperAgent'}, *args, **kwargs)
        ServiceProcess.__init__(self, *args, **kwargs)
        
        # Step 2: Create class attributes (is this needed?)
        self.__agent_phandle = None
        self.__agent_binding = None
        self.__agent_updt_op = None
        self.__agent_term_op = None
        self.__agent_spawn_args = None
        
        # Step 3: Setup the dataset context dictionary (to simulate acquiring context from the dataset registry)
        self.__dataset_context_dict = {"sos_station_st":{"id":"SOS",
                                              "callback":"data_message_callback",
                                              "base_url":"http://sdf.ndbc.noaa.gov/sos/server.php?",
                                              "start_time":"2008-08-01T00:00:00Z",
                                              "end_time":"2008-08-02T00:00:00Z",
                                              "property":"sea_water_temperature",
                                              "stationId":"41012"},
                                        "sos_station_sal":{"id":"SOS",
                                              "callback":"data_message_callback",
                                              "base_url":"http://sdf.ndbc.noaa.gov/sos/server.php?",
                                              "start_time":"2008-08-01T00:00:00Z",
                                              "end_time":"2008-08-02T00:00:00Z",
                                              "property":"salinity",
                                              "stationId":"41012"},
                                        "sos_glider_st":{"id":"SOS",
                                              "callback":"data_message_callback",
                                              "base_url":"http://sdf.ndbc.noaa.gov/sos/server.php?",
                                              "start_time":"2010-07-26T00:00:00Z",
                                              "end_time":"2010-07-27T00:00:00Z",
                                              "property":"sea_water_temperature",
                                              "stationId":"48900"},
                                        "sos_glider_sal":{"id":"SOS",
                                              "callback":"data_message_callback",
                                              "base_url":"http://sdf.ndbc.noaa.gov/sos/server.php?",
                                              "start_time":"2010-07-26T00:00:00Z",
                                              "end_time":"2010-07-27T00:00:00Z",
                                              "property":"salinity",
                                              "stationId":"48900"},
                                        "usgs":{"id":"USGS"}}
        
    @defer.inlineCallbacks
    def slc_init(self):
        '''
        TODO: document this
        '''
        # Step 1: Delegate initialization to parent class
        yield defer.maybeDeferred(ServiceProcess.slc_init, self)
        
        # Step 2: Spawn the associated dataset agent (if not already done)
        yield self._spawn_dataset_agent()
    
    @defer.inlineCallbacks
    def slc_terminate(self):
        '''
        TODO: document this
        '''
        # Step 1: Terminate the underlying dataset agent
        yield self._terminate_dataset_agent()
        
        # Step 2: Finish termination by delegating to parent
        yield defer.maybeDeferred(ServiceProcess.slc_terminate, self)
    
    @defer.inlineCallbacks
    def op_update_request(self, content, headers, msg):
        '''
        scheduler requests an update, provides the resource_id
        depending on state, may or may not call:
        spawn_dataset_agent
        then will always call:
        get_context
        get_update
        '''
        log.debug("Entered op_update_request(datasetID=%s)" % (str(content)))
        
        if (not self.is_agent_active()):
            yield self.reply_err(msg, "Dataset agent is not yet active.  Cannot fulfill update request for: " + str(content))
            defer.returnValue(None)
            
        # Step 1: Grab the context for the given dataset ID
        try:
            context = self._get_dataset_context(str(content))
        except KeyError, ex:
            yield self.reply_err(msg, "Could not grab the current context for the dataset with id: " + str(content))
        
        # Step 2: Perform the dataset update as a RPC
        # @todo: this should ultimately be an RPC send which replies when the update is complete, just before data is pushed back
        log.info("@@@--->>> Sending update request to Dataset Agent with context...")
        log.info("..." + str(context))
        yield self.send(self.agent_binding, self.agent_update_op, context)
        
        # @todo: change reply based on response of the RPC send
        yield self.reply_ok(msg, {"value":"Successfully dispatched update request"}, {})
    
    @defer.inlineCallbacks
    def op_result(self, content, headers, msg):
        '''
        Temporary op to catch RPC reply messages which are not well-formed
        '''
        log.debug("<<<---@@@ Incoming RPC reply from Dataset Agent...")
        log.debug("...Content:\t" + str(content))
        log.debug("...Headers\t" + str(headers))
        log.debug("...Message\t" + str(msg))
        defer.returnValue("ok")
    
    def op_binding_key_callback(self, content, headers, msg):
        log.info("<<<---@@@ Incoming callback with binding key message")
        log.debug("...Content:\t" + str(content))
        log.debug("...Headers\t" + str(headers))
        log.debug("...Message\t" + str(msg))
        
        self.__agent_binding = str(content)
        log.info("Accepted Dataset Agent binding key: '%s'" % (self.__agent_binding))
        return True
    
    def op_data_message_callback(self, content, headers, msg):
        """
        Replace with op_data_message()..   gets called repeatedly by the underlying java dataset agent to stream data back to this wrapper agent.
        Perform the update - send rpc message to dataset agent providing context from op_get_context.  Agent response will be the dataset or an error.
        """
        # @todo: pass this message up to the eoi ingest service
        #log.info("<<<---@@@ Receiving incoming data stream...")
        #log.info("...Headers\t" + str(headers))
        #(content, headers, msg) = yield self.rpc_send('ingest', content)
        #log.info("Returned OOI DatasetID: " + str(content))
        
        return True
        
    
    @defer.inlineCallbacks
    def _spawn_dataset_agent(self):
        '''
        Instantiates the java dataset agent including providing appropriate connectivity information so the agent can establish messaging channels
        @param timeout The length of time in seconds to wait for receipt of the dataset agent's binding key after it is spawned
        '''
        log.debug("Spawning dataset agent")
        if self.is_agent_initialized():
            log.warn("External dataset agent is already spawned with PID: %s.  Agent will NOT be respawned." % (str(self.agent_phandle.pid)))
            defer.returnValue(self.agent_phandle.pid)
        
        # Step 1: Start the Dataset Agent (java) passing necessary spawn arguments
        try:
            proc = subprocess.Popen(self.agent_spawn_args)
        except ValueError, ex:
            #log.error("Received invalid spawn arguments: " + str(ex))
            raise RuntimeError("JavaWrapperAgent._spawn_agent(): Received invalid spawn arguments form JavaWrapperAgent.agent_spawn_args" + str(ex))
        except OSError, ex:
            #log.error("Dataset agent raised exception on spawning: " + str(ex))
            raise RuntimeError("Failed to spawn the external Dataset Agent")

        # Step 2: Maintain a reference to the subprocess object (Popen) for later communication
        log.info("Started external Dataset Agent with PID: '%d'" % (proc.pid))
        self.__agent_phandle = proc
        defer.returnValue(self.agent_phandle.pid)
        
    @defer.inlineCallbacks
    def _terminate_dataset_agent(self):
        # TODO: add a timeout to this methods signature
        log.debug("Entered _terminate_dataset_agent()")
        
        returncode = 0;
        
        if self.is_agent_active():
            # Step 1: Send a terminate message to the java dataset agent
#            (content, msg, headers) = yield self.rpc_send(self.agent_binding, self.agent_term_op, None)
#            log.debug("Recieved response from terminate request...")
#            log.debug("...Content:\t" + str(content))
#            log.debug("...Headers\t" + str(headers))
#            log.debug("...Message\t" + str(msg))
            log.info("@@@--->>> Sending terminating request to underlying Dataset Agent")
            yield self.send(self.agent_binding, self.agent_term_op, None)
            
            
#            # Step 2: Once the agent replies, begin waiting based on the given timeout
#            # TODO: do this
#            # TODO:  Check how the Java CC builds reply_ok messages to determine how we can check ok versus err
#            self.agent_phandle.wait()
#            returncode = self.agent_phandle.poll()
#
#            # TODO: If the timeout is reached, force terminate with SIGTERM
#            self.__agent_binding = None
#            self.__agent_phandle = None
#        elif self.is_agent_activating():
#            # Force termination using SIGTERM if the dataset agent is not active
#            #    If the agent is initialized but is not active we will not have a
#            #    binding key.  Therefore, we cannot send it a shutdown message...
#            log.info("Dataset Agent has spawned but is not active and cannot receive AMQP messages.  Agent will be shut down with SIGTERM")
#            self.agent_phandle.terminate()
#            returncode = yield self.agent_phandle.wait()
#        else:
#            log.info("Dataset Agent has not been spawned, and will not be terminated")
#        
#        defer.returnValue(returncode)
        
        self.__agent_binding = None
        self.__agent_phandle = None
        defer.returnValue("0")
        
           
    def _get_dataset_context(self, datasetID):
        '''
        determines information required to provide necessary context to the java dataset agent
        
        reaches back to resource repo to determine what is present in CI (to determine what must be appended/updated)
        (for now return the Service which must be updated; not concerned with data parts which must be updated)
        '''
        log.debug("Entered _get_dataset_context(datasetID=%s)" % (datasetID))
        if (datasetID in self.__dataset_context_dict):
            return self.__dataset_context_dict[datasetID]
        else:
            raise KeyError("Invalid datasetID: %s" % (datasetID))


    def is_agent_initialized(self):
        return self.agent_phandle != None
    
    def is_agent_active(self):
        return self.is_agent_initialized() and self.agent_binding != None
    
    def is_agent_activating(self):
        return self.is_agent_initialized() and not self.is_agent_active()

    @property
    def agent_phandle(self):
        '''
        @return: an instance of subprocess.Popen as a reference to the underlying dataset agent
                 if self._spawn_agent has been successfully invoked, otherwise None
        '''
        # Initialization done upon successfull call to self._spawn_agent()
        return self.__agent_phandle

    @property
    def agent_binding(self):
        '''
        @return: a string representing the reply-to binding key used to send messages to the underlying dataset agent
                 if the dataset agent has responded to spawning through callback self.on_binding_key_callback(),
                 otherwise None
        '''
        # Initialization done by Dataset agent through callback, self.on_binding_key_callback() 
        return self.__agent_binding

    @property
    def agent_spawn_args(self):
        # Lazy-initialize the spawn arguments
        if (self.__agent_spawn_args == None):
            self._init_agent_spawn_args()
        return self.__agent_spawn_args

    @property
    def agent_update_op(self):
        # Lazy-initialize the update operation name
        if (self.__agent_updt_op == None):
            self._init_agent_update_op()
        return self.__agent_updt_op

    @property
    def agent_term_op(self):
        # Lazy-initialize the terminate operation name
        if (self.__agent_term_op == None):
            self._init_agent_term_op()
        return self.__agent_term_op

    def _init_agent_spawn_args(self):
        # @todo: Generate jar_pathname dynamically
        # jar_pathname = "/Users/tlarocque/Development/Java/Workspace_eclipse/EOI_dev/build/TryAgent.jar"   # STAR #
        jar_pathname = "res/apps/eoi_test/TryAgent.jar"   # STAR #
        hostname = self.container.exchange_manager.message_space.connection.hostname
        exchange = self.container.exchange_manager.exchange_space.name
        wrapper = self.get_scoped_name("system", str(self.declare['name']))      # validate that 'system' is the correct scope
        callback = "binding_key_callback"

        # Do not return anything.  Store spawn arguments in __agent_spawn_args        
        result = ["java", "-jar", jar_pathname, hostname, exchange, wrapper, callback]
        log.debug("Acquired dataset agent spawn arguments:   " + str(result))
        self.__agent_spawn_args = result
    
    def _init_agent_update_op(self):
        # @todo: Acquiring the shutdown op may need to be dynamic in the future
        updt_op= "op_update"
        log.debug("Acquired Dataset Agent update op: %s" % (updt_op))
        self.__agent_updt_op = updt_op

    def _init_agent_term_op(self):
        # @todo: Acquiring the shutdown op may need to be dynamic in the future
        term_op= "op_shutdown"
        log.debug("Acquired Dataset Agent terminate op: %s" % (term_op))
        self.__agent_term_op = term_op
        

class JavaWrapperAgentClient(ServiceClient):
    """
    Client for direct (RPC) interaction with the JavaWrapperAgent ServiceProcess
    """
    
    def __init__(self, *args, **kwargs):
        kwargs['targetname'] = 'java_wrapper_agent'
        ServiceClient.__init__(self, *args, **kwargs)
    
    @defer.inlineCallbacks
    def rpc_request_update(self, datasetId):
        '''
        TODO:
        '''
        # Ensure a Process instance exists to send messages FROM...
        #   ...if not, this will spawn a new default instance.
        yield self._check_init()
        
        # Invoke [op_]update_request() on the target service 'dispatcher_svc' via RPC
        log.info("@@@--->>> Sending 'update_request' RPC message to java_wrapper_agent service")
        (content, headers, msg) = yield self.rpc_send('update_request', datasetId)
        
        defer.returnValue(str(content))
        
        
    
# Spawn of the process using the module name
factory = ProcessFactory(JavaWrapperAgent)



'''


#---------------------#
# Copy/paste startup:
#---------------------#
#  :spawn an agent
from ion.agents.eoiagents.java_wrapper_agent import JavaWrapperAgent, JavaWrapperAgentClient; agent = JavaWrapperAgent(); agent.spawn();

#  :spawn and immediately terminate an agent 
from ion.agents.eoiagents.java_wrapper_agent import JavaWrapperAgent, JavaWrapperAgentClient; client = JavaWrapperAgentClient(); agent = JavaWrapperAgent(); agent.spawn();
client.rpc_terminate()

#  :Send update request for the dataset 'sos_station_st'
client.rpc_request_update('sos_station_st')


'''











