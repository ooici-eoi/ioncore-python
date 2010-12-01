#!/usr/bin/env python

"""
@file:   ion/agents/eoiagents/java_wrapper_agent.py
@author: Chris Mueller
@author: Tim LaRocque
@brief:  EOI JavaWrapperAgent and JavaWrapperAgentClient class definitions
"""

import ion.util.ionlog
import subprocess

from twisted.internet import defer
from ion.core.process.process import Process, ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient


log = ion.util.ionlog.getLogger(__name__)


class JavaWrapperAgent(ServiceProcess):
    """
    Class designed to facilitate tight interaction with ION in leu of a complete Java CC, including:
    Agent registration, process lifecycle, and reactivity to other core ION services
    """
    
    
    
    declare = ServiceProcess.service_declare(name = 'java_wrapper_agent',
                                             version = '0.1.0',
                                             dependencies = []) # no dependencies
    
        
        
    def __init__(self, *args, **kwargs):
        '''
        TODO:
        '''
        # Step 1: Delegate initialization to parent "ServiceProcess"
        log.info('JavaWrapperAgent.__init__():  Initializing...')
        ServiceProcess.__init__(self, *args, **kwargs)
        
        # Step 2: Create class attributes
        self.__agent_subprocess = None
        
    def slc_init(self):
        '''
        TODO:
        '''
        ServiceProcess.slc_init(self)
        # TODO: add complex, 1-time, deferred initialization here 
    
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
        yield self.reply_ok(msg, 'Successfully hit update_request method')
        #yield self.reply_err(msg, 'Not Implemented')
    
    @defer.inlineCallbacks
    def get_context(self, content, headers, msg):
        '''
        determines information required to provide necessary context to the java dataset agent
        
        reaches back to resource repo to determine what is present in CI (to determine what must be appended/updated)
        (for now return the Service which must be updated; not concerned with data parts which must be updated)
        '''
        defer.returnValue(msg, 'Not Implemented')

    def spawn_dataset_agent(self, content, headers, msg):
        """
        Instantiates the java dataset agent including providing appropriate connectivity information so the agent can establish messaging channels
        """
#        # Step 1: Grab the connectivity params, so that the subprocess can callback
#        properties = self.__getScriptProperties(id)
#        
#        if properties is None:
#            raise IndexError("Given script id '%s' does not exist" % id)
#        
#        pathname = properties['path']
#        if pathname is None:
#            raise KeyError("Given script id '%s' is not associated with a script" % str(id))
        id = "TryAgent.java"
        pathname = ""
        
        
        # Step 2: Start the Dataset Agent (java) passing the resource_id and connectivity params
        proc = subprocess.Popen(str(pathname), shell=True)
        log.info("Started external Dataset Agent '%s' with PID: %d" % (id, proc.pid))
        
        # Step 3: Maintain a ref to this proc object for later communication
        self.__agent_subprocess = proc
        
        
        return "Started external Dataset Agent '%s' with PID: %d" % (id, proc.pid)
    
        
        
    @defer.inlineCallbacks
    def terminate_dataset_agent(self):
        returncode = 0;
        
        if self.__agent_subprocess:
            yield self.__agent_subprocess.kill()
            returncode = yield self.__agent_subprocess.wait()
        
        defer.returnValue(returncode)
        
    @defer.inlineCallbacks
    def get_update(self, datasetId, context):
        """
        
        """
        defer.returnValue('Not Implemented')    

    @defer.inlineCallbacks
    def op_get_update(self, content, headers, msg):
        """
        Replace with op_data_message()..   gets called repeatedly by the underlying java dataset agent to stream data back to this wrapper agent.
        Perform the update - send rpc message to dataset agent providing context from op_get_context.  Agent response will be the dataset or an error.
        """
        yield self.reply_err(msg, 'Not Implemented')
        

class JavaWrapperAgentClient(ServiceClient):
    """
    Client for direct (RPC) interaction with the JavaWrapperAgent ServiceProcess
    """
    
    def __init__(self, *args, **kwargs):
        '''
        TODO:
        '''
        kwargs['targetname'] = 'java_wrapper_agent'
        ServiceClient.__init__(self, proc = MyProcess(), *args, **kwargs)
    
    @defer.inlineCallbacks
    def update_request(self, datasetId):
        '''
        TODO:
        '''
        # Ensure a Process instance exists to send messages FROM...
        #   ...if not, this will spawn a new default instance.
        yield self._check_init()
        
        # Invoke [op_]update_request() on the target service 'dispatcher_svc' via RPC 
        (content, headers, msg) = yield self.rpc_send('update_request', datasetId)
        
        defer.returnValue(content)
        
        
    
# Spawn of the process using the module name
factory = ProcessFactory(JavaWrapperAgent)


class MyProcess(Process):
    
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)
    
    def send(self, recv, operation, content, headers=None, reply=False):
        """
        @brief Send a message via the process receiver to destination.
        Starts a new conversation.
        @retval Deferred for send of message
        """
        msgheaders = self._prepare_message(headers)
        message = dict(recipient=recv, operation=operation,
                       content=content, headers=msgheaders)
        
        print "We are replying" if reply else "Not a reply"
        print message
        
        if reply:
            d = self.receiver.send(**message)
        else:
            d = self.backend_receiver.send(**message)
        return d


'''


#---------------------#
# Copy/paste startup:
#---------------------#
#  :Preparation
from ion.agents.eoiagents.java_wrapper_agent import JavaWrapperAgent as jwa, JavaWrapperAgentClient as jwac; client = jwac(); agent = jwa(); agent.spawn();

#  :Send update request for the dataset 'sos'
client.update_request('sos')


'''
