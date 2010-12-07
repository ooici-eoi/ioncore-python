#!/usr/bin/env python

"""
@file res/scripts/eoi_agent_test.py
@author Chris Mueller
@brief main module for bootstrapping eoi_dispatcher
"""

import logging
from twisted.internet import defer

from ion.core import ioninit
from ion.core import bootstrap

# Use the bootstrap configuration entries from the standard bootstrap
CONF = ioninit.config('startup.bootstrap1')

# Config files with lists of processes to start
agent_procs = ioninit.get_config('messaging_cfg', CONF)


@defer.inlineCallbacks
def start():
    """
    Main function of bootstrap. Starts EOI_DISPATCHER system with static config
    """
    logging.info("ION/EOI_DISPATCHER bootstrapping now...")
    startsvcs = []
 
 
    services = [
            {'name':'workflow_dispatcher','module':'ion.demo.eoilca.dispatcher_service','class':'DispatcherService'}
            ]
 
    startsvcs.extend(services)

    yield bootstrap.bootstrap(agent_procs, startsvcs)

start()
