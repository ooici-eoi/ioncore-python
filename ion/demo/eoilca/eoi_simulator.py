'''
Created on Nov 30, 2010

@file:   ion/demo/eoilca/eoi_simulator.py
@author: tlarocque
@brief:  A client created for the EOI R1 LCA demo which simulates actions on EOI services
         from OOICI components which may or may not be implemented at the time of the demo.
         For example: this client is used to simulate an "update event" signaled from the Scheduler Service to initiate
                      the dataset update sequence in the JavaWrapperAgent (ion.agents.eoiagents.java_wrapper_agent.py).
                      This class is also used to simulate notifications to the dispatcher_service (ion/demo/eoilca/dispatcher_service.py)
                      which signal it to invoke scripts that retrieve and reconstitute datasets from OOICI. 
'''

import ion.util.ionlog

from twisted.internet import defer
from ion.core.process.service_process import ServiceClient


log = ion.util.ionlog.getLogger(__name__)


class Eoi_SimController():
    '''
    '''
    
    def __init__(self, *args, **kwargs):
        '''
        Constructor
        '''
        pass
    
    

class EoiSim_JavaWrapperAgentSignalerClient(ServiceClient):
    '''
    TODO:
    '''
    
    def __init__(self, *args, **kwargs):
        '''
        Initializes this ServiceClient with the target 'dispatcher_svc'
        '''
        # Set the target service by its 
        kwargs['targetname'] = 'java_wrapper_agent'
        ServiceClient.__init__(self, *args, **kwargs)
        
    @defer.inlineCallbacks()
    def notify(self, source):
        '''
        Dispatches a change notification for the given 'source' to the
        DispatcherService.  When the DispatcherService is notified that
        there have been changes to a source, it, in turn, dispatches a
        request to start any scripts associated with that source.
        '''
        # Ensure a Process instance exists to send messages FROM...
        #   ...if not, this will spawn a new instance.
        yield self._check_init()
        
        # Invoke [op_]notify() on the target service 'dispatcher_svc' via RPC 
        (content, headers, msg) = yield self.rpc_send('notify', source)
        
        defer.returnValue(content)



class EoiSim_DispatcherServiceNotifierClient(ServiceClient):
    '''
    Service Client which can simulate actions on EOI services
    '''



    def __init__(self, *args, **kwargs):
        '''
        Initializes this ServiceClient with the target 'dispatcher_svc'
        '''
        # Set the target service by its 
        kwargs['targetname'] = 'dispatcher_svc'
        ServiceClient.__init__(self, *args, **kwargs)
        
    @defer.inlineCallbacks()
    def notify(self, source):
        '''
        Dispatches a change notification for the given 'source' to the
        DispatcherService.  When the DispatcherService is notified that
        there have been changes to a source, it, in turn, dispatches a
        request to start any scripts associated with that source.
        '''
        # Ensure a Process instance exists to send messages FROM...
        #   ...if not, this will spawn a new instance.
        yield self._check_init()
        
        # Invoke [op_]notify() on the target service 'dispatcher_svc' via RPC 
        (content, headers, msg) = yield self.rpc_send('notify', source)
        
        defer.returnValue(content)
        
        
        
        
        