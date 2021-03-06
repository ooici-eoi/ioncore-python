#!/usr/bin/env python

"""
@file ion/services/coi/identity_registry.py
@author Roger Unwin
@brief service for storing identities
"""







import ion.util.ionlog

log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from ion.core import ioninit, bootstrap

CONF = ioninit.config(__name__)

from ion.core.process.process import Process, ProcessClient, ProcessDesc, ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.security.authentication import Authentication
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance, ResourceClientError, ResourceInstanceError

from ion.core.object import object_utils

IDENT_TYPE = object_utils.create_type_identifier(object_id=1401, version=1)


class IdentityRegistryClient(ServiceClient):
    """
    """
    
    def __init__(self, proc=None, **kwargs):
        """
        """
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "identity_service"
        ServiceClient.__init__(self, proc, **kwargs)


    @defer.inlineCallbacks
    def register_user(self, user_cert, user_private_key):
        """
        This registers a user by storing the user certificate, user private key, and certificate subject line(derived from the certificate)
        It returns a ooi_id which is the uuid of the record and can be used to uniquely identify a user.
        """
        yield self._check_init()

        cont = {
            'user_cert': user_cert,
            'user_private_key': user_private_key
        }

        (content, headers, msg) = yield self.rpc_send('register_user_credentials', cont)
        defer.returnValue(str(content))


    @defer.inlineCallbacks
    def register_user_credentials(self, user_cert, user_private_key):
        """
        This registers a user by storing the user certificate, user private key, and certificate subject line(derived from the certificate)
        It returns a ooi_id which is the uuid of the record and can be used to uniquely identify a user.
        """
        yield self._check_init()

        cont = {
            'user_cert': user_cert,
            'user_private_key': user_private_key
        }

        (content, headers, msg) = yield self.rpc_send('register_user_credentials', cont)
        defer.returnValue(str(content))

        
    @defer.inlineCallbacks
    def update_user(self, user_dict):
        """
        """
        log.debug("in update_user")
        (content, headers, msg) = yield self.rpc_send('update_user', user_dict)
        defer.returnValue(content)


    @defer.inlineCallbacks
    def get_user(self, user_reference):
        """
        """
        log.debug("in get_user")
        (content, headers, msg) = yield self.rpc_send('get_user', user_reference)
        defer.returnValue(content)

        
    #
    #
    # UPDATE FIND_USERS when the repository supports this operation
    #
    #

    #--#op_find_users = BaseRegistryService.base_find_resource

    @defer.inlineCallbacks
    def find_users(self, user_description,regex=True,ignore_defaults=True, attnames=[]):
        """
        """
        #--#return self.base_find_resource('find_users',user_description,regex,ignore_defaults,attnames)




    @defer.inlineCallbacks
    def set_identity_lcstate(self, ooi_id, lcstate):
        """
        """
        log.debug("in set_identity_lcstate_new")
        
        cont = {
            'ooi_id': ooi_id,
            'lcstate': lcstate
        }

        (content, headers, msg) = yield self.rpc_send('set_lcstate', cont)
        defer.returnValue( content )

    @defer.inlineCallbacks
    def set_identity_lcstate_new(self, ooi_id):
        """
        """
        log.debug("in set_identity_lcstate_new")
        
        cont = {
            'ooi_id': ooi_id,
            'lcstate': 'New'
        }

        (content, headers, msg) = yield self.rpc_send('set_lcstate', cont)
        defer.returnValue( content )

    @defer.inlineCallbacks
    def set_identity_lcstate_active(self, ooi_id):
        """
        """
        log.debug("in set_identity_lcstate_active ")
        
        cont = {
            'ooi_id': ooi_id,
            'lcstate': 'Active'
        }

        (content, headers, msg) = yield self.rpc_send('set_lcstate', cont)
        defer.returnValue( content )
        
    @defer.inlineCallbacks
    def set_identity_lcstate_inactive(self, ooi_id):
        """
        """
        log.debug("in set_identity_lcstate_inactive")
        
        cont = {
            'ooi_id': ooi_id,
            'lcstate': 'Inactive'
        }

        (content, headers, msg) = yield self.rpc_send('set_lcstate', cont)
        defer.returnValue( content )

    @defer.inlineCallbacks
    def set_identity_lcstate_decommissioned(self, ooi_id):
        """
        """
        log.debug("in set_identity_lcstate_decommissioned")
        
        cont = {
            'ooi_id': ooi_id,
            'lcstate': 'Decommissioned'
        }

        (content, headers, msg) = yield self.rpc_send('set_lcstate', cont)
        defer.returnValue( content )

    @defer.inlineCallbacks
    def set_identity_lcstate_retired(self, ooi_id):
        """
        """
        log.debug("in set_identity_lcstate_retired")
        
        cont = {
            'ooi_id': ooi_id,
            'lcstate': 'Retired'
        }

        (content, headers, msg) = yield self.rpc_send('set_lcstate', cont)
        defer.returnValue( content )

    @defer.inlineCallbacks
    def set_identity_lcstate_developed(self, ooi_id):
        """
        """
        log.debug("in set_identity_lcstate_developed")
        
        cont = {
            'ooi_id': ooi_id,
            'lcstate': 'Developed'
        }

        (content, headers, msg) = yield self.rpc_send('set_lcstate', cont)
        defer.returnValue( content )

    @defer.inlineCallbacks
    def set_identity_lcstate_commissioned(self, ooi_id):
        """
        """
        log.debug("in set_identity_lcstate_commissioned")
        
        cont = {
            'ooi_id': ooi_id,
            'lcstate': 'Commissioned'
        }

        (content, headers, msg) = yield self.rpc_send('set_lcstate', cont)
        defer.returnValue( content )
    
    @defer.inlineCallbacks
    def is_user_registered(self, user_cert, user_private_key):
        """
        This determines if a user is registered by deriving the subject line from the certificate and scanning the registry for that line.
        It returns True or False
        """
        cont = {
            'user_cert': user_cert,
            'user_private_key': user_private_key,
        }
        
        (content, headers, msg) = yield self.rpc_send('verify_registration', cont)
        log.debug("in is_user_registered " + str(content))
        defer.returnValue( content )
        
    @defer.inlineCallbacks
    def authenticate_user(self, user_cert, user_private_key):
        """
        This authenticates that the user exists. If so, the credentials are replaced with the current ones, and a ooi_id is returned. If not, None is returned.
        """
        log.debug('in authenticate_user')
        cont = {
            'user_cert': user_cert,
            'user_private_key': user_private_key,
        }
        
        (content, headers, msg) = yield self.rpc_send('authenticate_user_credentials', cont)
        
        defer.returnValue( content )


class IdentityRegistryService(ServiceProcess):

    # Declaration of service
    declare = ServiceProcess.service_declare(name='identity_service', version='0.1.0', dependencies=[])
    
    def slc_init(self):
        """
        """
        # Service life cycle state. Initialize service here. Can use yields.
        
        # Can be called in __init__ or in slc_init... no yield required
        self.rc = ResourceClient(proc=self)
        
        self.instance_counter = 1
        # This is a hack to get past no 
        self._user_dict = {}


    @defer.inlineCallbacks
    def op_set_lcstate(self, request, headers, msg):
        """
        """
        log.debug('in op_get_user')
        identity = yield self.rc.get_instance(request['ooi_id'])

        if request['lcstate'] == 'New':
          identity.ResourceLifeCycleState = identity.NEW
          yield self.rc.put_instance(identity, 'updating LCSTATE to %s' % request['lcstate'])
          yield self.reply_ok(msg, True)
        elif request['lcstate'] == 'Active':
          identity.ResourceLifeCycleState = identity.ACTIVE
          yield self.rc.put_instance(identity, 'updating LCSTATE to %s' % request['lcstate'])
          yield self.reply_ok(msg, True)
        elif request['lcstate'] == 'Inactive':
          identity.ResourceLifeCycleState = identity.INACTIVE
          yield self.rc.put_instance(identity, 'updating LCSTATE to %s' % request['lcstate'])
          yield self.reply_ok(msg, True)
        elif request['lcstate'] == 'Commissioned':
          identity.ResourceLifeCycleState = identity.COMMISSIONED
          yield self.rc.put_instance(identity, 'updating LCSTATE to %s' % request['lcstate'])
          yield self.reply_ok(msg, True)
        elif request['lcstate'] == 'Decommissioned':
          identity.ResourceLifeCycleState = identity.DECOMMISSIONED
          yield self.rc.put_instance(identity, 'updating LCSTATE to %s' % request['lcstate'])
          yield self.reply_ok(msg, True)
        elif request['lcstate'] == 'Retired':
          identity.ResourceLifeCycleState = identity.RETIRED
          yield self.rc.put_instance(identity, 'updating LCSTATE to %s' % request['lcstate'])
          yield self.reply_ok(msg, True)
        elif request['lcstate'] == 'Developed':
          identity.ResourceLifeCycleState = identity.DEVELOPED
          yield self.rc.put_instance(identity, 'updating LCSTATE to %s' % request['lcstate'])
          yield self.reply_ok(msg, True)
        elif request['lcstate'] == 'Update':
          identity.ResourceLifeCycleState = identity.UPDATE
          yield self.rc.put_instance(identity, 'updating LCSTATE to %s' % request['lcstate'])
          yield self.reply_ok(msg, True)
        else:
          yield self.reply_ok(msg, False)



    @defer.inlineCallbacks
    def op_get_user(self, request, headers, msg):
        """
        """
        log.debug('in op_get_user')
        if request in self._user_dict.values():
          identity = yield self.rc.get_instance(request)
          user = {'user_cert' : identity.certificate,
                  'ooi_id' : identity.ResourceIdentity,
                  'subject' : identity.subject,
                  'lifecycle' : str(identity.ResourceLifeCycleState),
                  'user_private_key' : identity.rsa_private_key}
        
          yield self.reply_ok(msg, user)
        else:
          yield self.reply_ok(msg, None)
        # Above line needs to be altered when FIND is implemented

    
    @defer.inlineCallbacks
    def op_register_user_credentials(self, request, headers, msg):
        """
        This registers a user by storing the user certificate, user private key, and certificate subject line(derived from the certificate)
        It returns a ooi_id which is the uuid of the record and can be used to uniquely identify a user.
        """
        log.debug('in op_register_user_credentials')

        identity = yield self.rc.create_instance(IDENT_TYPE, name='Identity Registry', description='A place to store identitys')
        identity.certificate = request['user_cert']
        identity.rsa_private_key = request['user_private_key']

        authentication = Authentication()
        cert_info = authentication.decode_certificate(request['user_cert'])
        identity.subject = cert_info['subject']
       
        yield self.rc.put_instance(identity, 'Adding identity %s' % identity.subject)
        log.debug('Commit completed, %s' % identity.ResourceIdentity)
        yield self.reply_ok(msg, identity.ResourceIdentity)

        # Now we store the subject/ResourceIdentity pair so we can get around not having find.
        self._user_dict['testing'] = 'TESTING'
        self._user_dict[cert_info['subject']] = identity.ResourceIdentity
        # Above line needs to be altered when FIND is implemented
        
        

    @defer.inlineCallbacks
    def op_verify_registration(self, request, headers, msg):
        """
        This determines if a user is registered by deriving the subject line from the certificate and scanning the registry for that line.
        It returns True or False
        """

        log.info('in op_verify_registration')

        authentication = Authentication()
        cert_info = authentication.decode_certificate(request['user_cert'])

        if cert_info['subject'] in self._user_dict.keys():
           log.info('op_verify_registration: Registration VERIFIED')
           yield self.reply_ok(msg, True)
        else:
           yield self.reply_ok(msg, False)
           log.info('op_verify_registration: Registration NOT PRESENT')

    @defer.inlineCallbacks
    def op_authenticate_user_credentials(self, request, headers, msg):
        """
        This authenticates that the user exists. If so, the credentials are replaced with the current ones, and a ooi_id is returned. If not, None is returned.
        """

        log.info('in op_authenticate_user_credentials')

        authentication = Authentication()
        cert_info = authentication.decode_certificate(request['user_cert'])

        if cert_info['subject'] in self._user_dict.keys():
           log.info('op_verify_registration: Registration VERIFIED')
           identity = yield self.rc.get_instance(self._user_dict[cert_info['subject']])
           identity.certificate = request['user_cert']
           identity.rsa_private_key = request['user_private_key']
           self.rc.put_instance(identity, 'Updated user credentials')
           log.debug(str(identity.ResourceIdentity))
           yield self.reply_ok(msg, identity.ResourceIdentity)
        else:
           log.debug('returning None')
           yield self.reply_ok(msg, None)  # Should this be none? or False or something else

    @defer.inlineCallbacks
    def op_update_user(self, request, headers, msg):
        """
        This updates that the user record. 
        """
        log.info('in op_update_user')
        
        if request['subject'] in self._user_dict.keys():
           log.info('op_update_user: Found match')
           identity = yield self.rc.get_instance(self._user_dict[request['subject']])
           identity.certificate = request['user_cert']
           if identity.subject != request['subject']:
              log.error("CANNOT UPDATE A DERIVED ATTRIBUTE. Note: Changing the subject will make certificate reference a different user")
           identity.rsa_private_key = request['user_private_key']
           self.rc.put_instance(identity, 'Updated user credentials')
           
           yield self.reply_ok(msg, identity.ResourceIdentity)
        else:
           log.debug('op_update_user: no match')
           yield self.reply_ok(msg, None)  # Should this be none? or False or something else
        



# Spawn of the process using the module name
factory = ProcessFactory(IdentityRegistryService)
