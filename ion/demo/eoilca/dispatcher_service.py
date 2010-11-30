#!/usr/bin/env python
"""
Created on Nov 9, 2010

@file:   ion/play/dispatcher_service.py
@author: tlarocque
@brief:  Dispatching service for starting remote processes for data assimilation/processing on changes to availability of data
"""

import ion.util.ionlog
import subprocess

from twisted.internet import defer
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient


log = ion.util.ionlog.getLogger(__name__)


# TODO: Process may stay alive even after the script completes.  This can be fixed by either polling the proc object
#       or making sure the memory ref to proc is released
class DispatcherService(ServiceProcess):
    """
    Dispatching service for starting data processing scripts
    """

    
    declare = ServiceProcess.service_declare(name='dispatcher_svc',
                                             version='0.2.0',
                                             dependencies=[]) # no dependecies

    
    
    def __init__(self, *args, **kwargs):
        """
        Initializes the DispatherService class
        (Yields NOT allowed)
        """
        # Step 1: Delegate initialization to parent
        log.info('DispatcherService.__init__(): Starting initialization...')
        ServiceProcess.__init__(self, *args, **kwargs)
        
        # Step 2: Add class attributes
        self.__script_dictionary = []
    
    def slc_init(self):
        """
        Initializes the Dispatching Service when spawned
        (Yields ALLOWED)
        """
        # Initialize the script dictionary
        self.__init_scripts()
    
    def slc_stop(self):
        """
        ???
        """
        pass
    
    def slc_shutdown(self):
        """
        ???
        """
        pass
    
    @property
    def proc_name(self):
        # FIXME: this may be a dangerous overwrite because ion.core.process
        #        intentionally overwrites this value.  Find out if there is
        #        a reason, or if this is a bug
        return self.__module__
    
    @proc_name.setter
    def proc_name(self, value):
        if not isinstance(value, str):
            raise TypeError("proc_name must be a String.  Received %s" % str(type(value)))
        pass


    def __init_scripts(self):
        """
        Initializes the script dictionary;  The script dictionary provides linkage
        between script 'id's and the relative location of that processing script
        """
        # TODO: This should be generated from some external source
        #       so that it can modified/updated dynamically
        self.addScript('default_script', './res/scripts/data_script_default.sh', ['test1', 'test2', 'test3']) 
        self.addScript('julias_script',  './res/scripts/data_script_julia.sh',   ['sst', 'sst_ir', 'hycom', 'rivers', 'hfradar', 'altimeter', 'test1', 'test2', 'test3']) 
        self.addScript('elis_script',    './res/scripts/data_script_eli.sh',     ['ctd', 'xbt', 'nws', 'argo', 'glider', 'test1', 'test2'])
        self.addScript('johns_script',   './res/scripts/data_script_john.sh',    ['nam', 'test1']) 
    
    def addScript(self, id, pathname, sourceList):
        if type(sourceList) is not list:
            raise ValueError('The given argument "sourceList" is not a list object')
        script_props = {}
        script_props['path'] = str(pathname)
        script_props['sources'] = [str.lower(source) for source in sourceList]
        self.__script_dictionary.append((str(id), script_props))
        
    def __getScriptProperties(self, scriptID):
        """
        Retrieves a dictionary or properties for the given scriptID
        The returned dictionary contains the following keys:
            'path'     the relative or absolute path of the associated script
            'sources'  a list of source strings which this script may utilize
            'proc'     an instance of subprocess which is running the script denoted by 'path' (might be None)
        """
        result = None
        lowerID = str.lower(scriptID)
        for id, props in self.__script_dictionary:
            if lowerID == id:
                result = props
                break
        return result
    
    def __getScriptIDsBySource(self, source):
        """
        Retrieves a list of script IDs which contain the given
        source in their list of sources
        """
        result = []
        for id, props in self.__script_dictionary:
            if source in props['sources']:
                result.append(id)
        return result
    
#    @defer.inlineCallbacks
#    def op_result(self, content, headers, msg):
#        defer.returnValue('%s\n%s\n%s' % (str(content), str(headers), str(msg)))
    
    @defer.inlineCallbacks
    def op_notify(self, content, headers, msg):
        """
        Notification operation.  Checks which scripts are associated with the
        given source (as 'content') and starts each with the subprocess module.
        """
        # Step 1: Retrieve a list of script ids which associate with the given 'source'
        ids = self.__getScriptIDsBySource(str(content))
        if (ids is None) or (len(ids) == 0):
            return_message = 'No listeners affected by an update notification for source "%s"' % str(content)
            yield self.reply_err(msg, return_message, None, ValueError("Operation 'notify' cannot be invoked for the given content '%s'" % str(content)))
            defer.returnValue(return_message)
            
        # Step 2: Dispatch a 'start' request for each script (by id)
        result = None
        try:
            for id in ids:
                result = self.__dispatch(msg, id, op='start')
        except (ValueError, KeyError, IndexError) as ex:
            yield self.reply_err(msg, {'op':'notify', 'value':"Notification failed with message: '%s'" % ex.message}, {})
        else:
            if len(ids) == 1:
                yield self.reply_ok(msg, {'value':"Dispatched START request for script id: '%s' with result '%s'" % (ids[0], result)}, {})          
            else:
                yield self.reply_ok(msg, {'value':"Dispatched START request for script ids: %s" % str(ids)}, {})

    def __dispatch(self, recv, id, op='start'):
        """
        Dispatching operation.  Runs scripts to kick-off external data assimilation processes.
        'content' should be provided as a dictionary with items 'op' and 'target'
        For example:
        
            content = {'op':'start', 'target':'default_script'}
        
        Acceptable values for op include:
            'start'    starts the script associated with the given target
            'stop'     stops the script associated with the given target if it is active
        """
        # Perform the given operation ('op') on a process (process identified by 'id').
        result = None
        if op == 'start':
            result = self.__startProcess(recv, id)
        elif op == 'stop':
            result = self.__stopProcess(recv, id)
        else:
            raise ValueError("Unknown operation given: %s" % op)
        return result
                
    def __startProcess(self, msg, id):
        """
        TODO:
        """
        #Step 1: Grab the script properties for the given id
        properties = self.__getScriptProperties(id)
        
        if properties is None:
            raise IndexError("Given script id '%s' does not exist" % id)
        
        pathname = properties['path']
        if pathname is None:
            raise KeyError("Given script id '%s' is not associated with a script" % str(id))
        
        # Step 2: Start the external procedure associated with the script 'name'
        proc = subprocess.Popen(str(pathname), shell=True)
        log.info("Started external script '%s' with PID: %d" % (id, proc.pid))
        
        # Step 3: Store proc object in the script dictionary
        properties['proc'] = proc        
        return "Started processing script '%s' with PID: %d" % (id, proc.pid)
    
    @defer.inlineCallbacks
    def __stopProcess(self, msg, id):
        # Step 1: grab proc from script dictionary
        if script not in self.__script_dictionary:
            yield self.reply_err(msg, {'value':'Given script does not exist!  ' + str(id)}, {})
        else:
            # Step 2: Check if the process is alive...
            proc_map = self.__script_dictionary[id]
            proc = proc_map['proc']
            if (proc is None):
                yield self.reply_ok(msg, {'value':'Given script has not been started  ' + str(id)}, {})
            else:
                # Step 3: ...If proc is alive, kill process
                log.info("Sending SIGKILL message to process with PID %s" % proc.pid)
                proc.kill()
                proc.wait()
                yield self.reply_ok(msg, {'value':"Successfully stopped process with SIGKILL message.  Script PID = %s" % proc.pid}, {})
        
        
        
class DispatcherServiceClient(ServiceClient):
    """
    This is an example client which calls the DispatcherService.  It's
    intent is to notify the dispatcher of changes to data sources so
    it can make requests to start various data processing/modeling scripts
    """
    
    def __init__(self, *args, **kwargs):
        kwargs['targetname'] = 'dispatcher_svc'
        ServiceClient.__init__(self, *args, **kwargs)
    
    @defer.inlineCallbacks
    def notify(self, source='test1'):
        """
        Dispatches a change notification for the given 'source' to the
        DispatcherService.  When the DispatcherService is notified that
        there have been changes to a source, it, in turn, dispatches a
        request to start any scripts associated with that source.
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('notify', source)
        defer.returnValue(content)

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
client.notify('test1')


'''
