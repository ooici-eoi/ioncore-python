#!/usr/bin/env python
"""
Created on Nov 9, 2010

@file:   ion/demo/eoilca/dispatcher_service.py
@author: Tim LaRocque
@brief:  Dispatching service for starting remote processes for data assimilation/processing on changes to availability of data
"""

import ion.util.ionlog
import subprocess

from twisted.internet import defer
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.messaging.ion_reply_codes import ResponseCodes as RC


log = ion.util.ionlog.getLogger(__name__)


# TODO: Process may stay alive even after the script completes.  This can be fixed by either polling the proc object
#       or making sure the memory ref to proc is released
#@todo: Rename to WorkflowDispatcher
class DispatcherService(ServiceProcess):
    """
    Dispatching service for starting data processing scripts
    """

    
    declare = ServiceProcess.service_declare(name='workflow_dispatcher',
                                             version='0.3.0',
                                             dependencies=[]) # no dependecies

    
    
    def __init__(self, *args, **kwargs):
        """
        Initializes the DispatherService class
        (Yields NOT allowed)
        """
        # Step 1: Delegate initialization to parent
        log.info('Starting initialization...')
#        if 'spawnargs' not in kwargs:
#            kwargs['spawnargs'] = {}
#        kwargs['spawnargs']['proc-name'] = __name__ + ".DispatcherService"
        ServiceProcess.__init__(self, *args, **kwargs)
        # Step 2: Add class attributes
        # NONE
        
    def slc_init(self):
        """
        Initializes the Dispatching Service when spawned
        (Yields ALLOWED)
        """
        pass
    
    @defer.inlineCallbacks
    def op_notify(self, content, headers, msg):
        """
        Notification operation.  
        """
        # Step 1: Retrieve workflow, dataset ID and name from the message content
        try:
            (datasetId, datasetName, workflow) = self._unpack_notification(content)
        except (TypeError, KeyError) as ex:
            reply = "Invalid notify content: %s" % (str(ex))
            yield self.reply_uncaught_err(msg, content = reply, response_code = reply)
            defer.returnValue(None)
        # Step 2: Build the subprocess arguments to start the workflow
        args = self._prepare_workflow(datasetId, datasetName, workflow)
        # Step 3: Start the workflow with the subprocess module
        try:
#            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            proc = subprocess.Popen(args)
        except Exception as ex:
            reply = "Could not start workflow: %s" % (str(ex))
            (content, headers, msg) = yield self.reply_uncaught_err(msg, content = reply, response_code = reply)
            defer.returnValue(content)
        returncode = proc.wait()
        if returncode == 0:
            yield self.reply(msg, content="Workflow completed with SUCCESS")
        else:
            msgout = proc.communicate()[0]
            yield self.reply(msg, content="Error on notify.  Retrieved return code: '%s'.  Retrieved message: %s" % (str(returncode), str(msgout)))
    
    def _unpack_notification(self, content):
        if (type(content) is not dict):
            raise TypeError("Content must be a dictionary with keys 'workflow' and 'dataset_id'")
        if ('dataset_id' not in content):
            raise KeyError("Content dict must contain an entry for 'dataset_id'")
        if ('dataset_name' not in content):
            raise KeyError("Content dict must contain an entry for 'dataset_name'")
        if ('workflow' not in content):
            raise KeyError("Content dict must contain an entry for 'workflow'")
        return (content['dataset_id'], content['dataset_name'], content['workflow'])
            
            
    def _prepare_workflow(self, datasetId, datasetName, workflow):
        '''
        Generates a list of arguments from the given parameters to start an external workflow.
        Currently just drops the given arguments into a list as they are.
        '''
        #@todo: This unsafe as it permits injection attacks.  Validate inputs here.
        return [workflow, datasetId, datasetName]
        
        
        
#        # Step 1: Retrieve a list of script ids which associate with the given 'source'
#        ids = self.__getScriptIDsBySource(str(content))
#        if (ids is None) or (len(ids) == 0):
#            return_message = 'No listeners affected by an update notification for source "%s"' % str(content)
#            yield self.reply_err(msg, return_message, None, ValueError("Operation 'notify' cannot be invoked for the given content '%s'" % str(content)))
#            defer.returnValue(return_message)
#            
#        # Step 2: Dispatch a 'start' request for each script (by id)
#        result = None
#        try:
#            for id in ids:
#                result = self.__dispatch(msg, id, op='start')
#        except (ValueError, KeyError, IndexError) as ex:
#            yield self.reply_err(msg, {'op':'notify', 'value':"Notification failed with message: '%s'" % ex.message}, {})
#        else:
#            if len(ids) == 1:
#                yield self.reply_ok(msg, {'value':"Dispatched START request for script id: '%s' with result '%s'" % (ids[0], result)}, {})          
#            else:
#                yield self.reply_ok(msg, {'value':"Dispatched START request for script ids: %s" % str(ids)}, {})


    

#    def __dispatch(self, recv, id, op='start'):
#        """
#        Dispatching operation.  Runs scripts to kick-off external data assimilation processes.
#        'content' should be provided as a dictionary with items 'op' and 'target'
#        For example:
#        
#            content = {'op':'start', 'target':'default_script'}
#        
#        Acceptable values for op include:
#            'start'    starts the script associated with the given target
#            'stop'     stops the script associated with the given target if it is active
#        """
#        # Perform the given operation ('op') on a process (process identified by 'id').
#        result = None
#        if op == 'start':
#            result = self.__startProcess(recv, id)
#        elif op == 'stop':
#            result = self.__stopProcess(recv, id)
#        else:
#            raise ValueError("Unknown operation given: %s" % op)
#        return result
#                
#    def __startProcess(self, msg, id):
#        """
#        TODO:
#        """
#        #Step 1: Grab the script properties for the given id
#        properties = self.__getScriptProperties(id)
#        
#        if properties is None:
#            raise IndexError("Given script id '%s' does not exist" % id)
#        
#        pathname = properties['path']
#        if pathname is None:
#            raise KeyError("Given script id '%s' is not associated with a script" % str(id))
#        
#        # Step 2: Start the external procedure associated with the script 'name'
#        proc = subprocess.Popen(str(pathname), shell=True)
#        log.info("Started external script '%s' with PID: %d" % (id, proc.pid))
#        
#        # Step 3: Store proc object in the script dictionary
#        properties['proc'] = proc        
#        return "Started processing script '%s' with PID: %d" % (id, proc.pid)
#    
#    @defer.inlineCallbacks
#    def __stopProcess(self, msg, id):
#        # Step 1: grab proc from script dictionary
#        if script not in self.__script_dictionary:
#            yield self.reply_err(msg, {'value':'Given script does not exist!  ' + str(id)}, {})
#        else:
#            # Step 2: Check if the process is alive...
#            proc_map = self.__script_dictionary[id]
#            proc = proc_map['proc']
#            if (proc is None):
#                yield self.reply_ok(msg, {'value':'Given script has not been started  ' + str(id)}, {})
#            else:
#                # Step 3: ...If proc is alive, kill process
#                log.info("Sending SIGKILL message to process with PID %s" % proc.pid)
#                proc.kill()
#                proc.wait()
#                yield self.reply_ok(msg, {'value':"Successfully stopped process with SIGKILL message.  Script PID = %s" % proc.pid}, {})
        
        
        
class DispatcherServiceClient(ServiceClient):
    """
    This is an example client which calls the DispatcherService.  It's
    intent is to notify the dispatcher of changes to data sources so
    it can make requests to start various data processing/modeling scripts
    """
    
    DEFAULT_GET_SCRIPT = 'res/apps/ooi2unidata/get_ooi_dataset.sh'
    _next_id = 0
    
    def __init__(self, *args, **kwargs):
        kwargs['targetname'] = 'workflow_dispatcher'
        ServiceClient.__init__(self, *args, **kwargs)
    
    @classmethod
    def next_id(cls):
        cls._next_id += 1
        return str(cls._next_id)
    
    @defer.inlineCallbacks
    def rpc_notify(self, datasetId, datasetName=None, workflow=DEFAULT_GET_SCRIPT):
        '''
        Dispatches a change notification so that the dispatcher can perform an
        update using the given datasetId (for example:  to the
        DispatcherService.  When the DispatcherService is notified that
        there have been changes to a source, it, in turn, dispatches a
        request to start any scripts associated with that source.
        '''
        yield self._check_init()
        if (datasetName == None):
            datasetName = 'example_dataset_' + DispatcherServiceClient.next_id()
        (content, headers, msg) = yield self.rpc_send('notify', {"dataset_id":datasetId, "dataset_name": datasetName, "workflow":workflow})
        log.info("<<<---@@@ Incoming RPC reply...")
        log.debug("\n\n\n...Content:\t" + str(content))
        log.debug("...Headers\t" + str(headers))
        log.debug("...Message\t" + str(msg) + "\n\n\n")
        
        if (headers[RC.MSG_STATUS] == RC.ION_OK):
            defer.returnValue("Notify invokation completed with status OK.  Result: %s" % (str(content)))
        else:
            defer.returnValue("Notify invokation completed with status %s.  Response code: %s" % (str(headers[RC.MSG_STATUS]), str(headers[RC.MSG_RESPONSE])))

factory = ProcessFactory(DispatcherService)




'''
#---------------------#
# Example usage:
#---------------------#
# :Spawn the dispatcher service process
spawn('ion.demo.eoilca.dispatcher_service')

# :Create an instance of the dispatching client
from ion.demo.eoilca.dispatcher_service import DispatcherServiceClient as dsc
client = dsc()

# :Notify the service of a change to an existing source (this uses the 'test1' source
client.notify_sourceChange('test1')



#---------------------#
# Copy/paste startup:
#---------------------#
#  :Preparation
from ion.demo.eoilca.dispatcher_service import DispatcherService as ds, DispatcherServiceClient as dsc; client = dsc(); service = ds(); service.spawn();

#  :Send notification of changes to source 'test1'
client.notify(datasetId=[add id])


'''
