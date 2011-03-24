#!/usr/bin/env python

"""
@file ion/integration/test_app_integration.py
@test ion.integration.app_integration_service
@author David Everett
"""

from twisted.trial import unittest

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

#from ion.integration.r1integration_service import R1IntegrationServiceClient
from ion.integration.ais.app_integration_service import AppIntegrationServiceClient
from ion.core.messaging.message_client import MessageClient
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient
from ion.services.coi.resource_registry_beta.resource_client import ResourceInstance
from ion.test.iontest import IonTestCase

from ion.core.object import object_utils

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE
from ion.integration.ais.ais_object_identifiers import REGISTER_USER_TYPE, \
                                                       UPDATE_USER_EMAIL_TYPE,   \
                                                       UPDATE_USER_DISPATCH_QUEUE_TYPE, \
                                                       OOI_ID_TYPE, \
                                                       FIND_DATA_RESOURCES_REQ_MSG_TYPE, \
                                                       GET_DATA_RESOURCE_DETAIL_REQ_MSG_TYPE


# Create CDM Type Objects
datasource_type = object_utils.create_type_identifier(object_id=4502, version=1)
dataset_type = object_utils.create_type_identifier(object_id=10001, version=1)
group_type = object_utils.create_type_identifier(object_id=10020, version=1)
dimension_type = object_utils.create_type_identifier(object_id=10018, version=1)
variable_type = object_utils.create_type_identifier(object_id=10024, version=1)
bounded_array_type = object_utils.create_type_identifier(object_id=10021, version=1)
array_structure_type = object_utils.create_type_identifier(object_id=10025, version=1)

attribute_type = object_utils.create_type_identifier(object_id=10017, version=1)
stringArray_type = object_utils.create_type_identifier(object_id=10015, version=1)
float32Array_type = object_utils.create_type_identifier(object_id=10013, version=1)
int32Array_type = object_utils.create_type_identifier(object_id=10009, version=1)

class AppIntegrationTest(IonTestCase):
    """
    Testing Application Integration Service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {'name':'app_integration','module':'ion.integration.ais.app_integration_service','class':'AppIntegrationService'},
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'servicename':'datastore'}},
            {'name':'resource_registry1','module':'ion.services.coi.resource_registry_beta.resource_registry','class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}},
            {'name':'identity_registry','module':'ion.services.coi.identity_registry','class':'IdentityRegistryService'}
        ]
        sup = yield self._spawn_processes(services)

        self.sup = sup

        self.aisc = AppIntegrationServiceClient(proc=sup)
        #self.aisc = R1IntegrationServiceClient(proc=sup)
        #self.rc = ResourceClient(proc=sup)
        self.dsID = None

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_findDataResources(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        rc = ResourceClient(proc=self.test_sup)
        
        # Use the message client to create a message object
        log.debug('DHE: AppIntegrationService! instantiating FindResourcesMsg.\n')
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = 'Dr. Chew'
        reqMsg.message_parameters_reference.minLatitude = 32.87521
        reqMsg.message_parameters_reference.maxLatitude = 32.97521
        reqMsg.message_parameters_reference.minLongitude = -117.274609
        reqMsg.message_parameters_reference.maxLongitude = -117.174609
        
        """
        DHE: temporarily passing the identity of the dummied dataset just
        created into the client so that it can access because currently there
        is now way to search.
        """
        log.debug('DHE: Calling findDataResources!!...')
        outcome1 = yield self.aisc.findDataResources(reqMsg)
        log.debug('DHE: findDataResources returned:\n' + \
                  'user_ooi_id: ' + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].user_ooi_id) + \
                  '\n' + \
                  str('resource_id: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].data_resource_id) + \
                  str('\n') + \
                  str('title: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].title) + \
                  str('\n') + \
                  str('institution: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].institution) + \
                  str('\n') + \
                  str('source: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].source) + \
                  str('\n') + \
                  str('references: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].references) + \
                  str('\n') + \
                  str('ion_time_coverage_start: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].ion_time_coverage_start) + \
                  str('\n') + \
                  str('ion_time_coverage_end: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].ion_time_coverage_end) + \
                  str('\n') + \
                  #str('summary: ') + \
                  #str(outcome1.message_parameters_reference[0].dataResourceSummary[0].summary) + \
                  #str('\n') + \
                  #str('comment: ') + \
                  #str(outcome1.message_parameters_reference[0].dataResourceSummary[0].comment) + \
                  #str('\n') + \
                  str('ion_geospatial_lat_min: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].ion_geospatial_lat_min) + \
                  str('\n') + \
                  str('ion_geospatial_lat_max: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].ion_geospatial_lat_max) + \
                  str('\n') + \
                  str('ion_geospatial_lon_min: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].ion_geospatial_lon_min) + \
                  str('\n') + \
                  str('ion_geospatial_lon_max: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].ion_geospatial_lon_max) + \
                  str('\n') + \
                  str('ion_geospatial_vertical_min: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].ion_geospatial_vertical_min) + \
                  str('\n') + \
                  str('ion_geospatial_vertical_max: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].ion_geospatial_vertical_max) + \
                  str('\n') + \
                  str('ion_geospatial_vertical_positive: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].ion_geospatial_vertical_positive) + \
                  str('\n') + \
                  str('standard_name: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].standard_name) + \
                  str('\n') + \
                  str('units: ') + \
                  str(outcome1.message_parameters_reference[0].dataResourceSummary[0].units))

        
        self.dsID = outcome1.message_parameters_reference[0].dataResourceSummary[0].data_resource_id
        

    @defer.inlineCallbacks
    def test_getDataResourceDetail(self):

        # Create a message client        
        mc = MessageClient(proc=self.test_sup)
        rc = ResourceClient(proc=self.test_sup)
        
        log.debug('DHE: testing getDataResourceDetail')

        # Use the message client to create a message object
        log.debug('DHE: AppIntegrationService! instantiating GetDataResourceDetailMsg.\n')
        
        # CHANGE THIS TO GET_DATA_RESOURCE_DETAIL_REQ_MSG_TYPE
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(GET_DATA_RESOURCE_DETAIL_REQ_MSG_TYPE)
        if self.dsID != None:
            reqMsg.message_parameters_reference.data_resource_id = self.dsID

        log.debug('DHE: Calling getDataResourceDetail!!...')
        outcome1 = yield self.aisc.getDataResourceDetail(reqMsg)
        #log.debug('DHE: getDataResourceDetail returned:\n'+str(outcome1))
        log.debug('DHE: getDataResourceDetail returned.\n')

    @defer.inlineCallbacks
    def test_createDownloadURL(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        rc = ResourceClient(proc=self.test_sup)
        
        log.debug('DHE: testing createDownloadURL')

        # Use the message client to create a message object
        log.debug('DHE: AppIntegrationService! instantiating CreateDownloadURLMSG.\n')
        
        # CHANGE THIS TO CREATE_DOWNLOAD_URL_REQ_MSG_TYPE
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        #reqMsg.message_parameters_reference.minLatitude = 32.87521

        log.debug('DHE: Calling createDownloadURL!!...')
        outcome1 = yield self.aisc.createDownloadURL(reqMsg)
        log.debug('DHE: createDownloadURL returned:\n'+str(outcome1))

    @defer.inlineCallbacks
    def test_registerUser(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create the register_user request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS RegisterUser request')
        msg.message_parameters_reference = msg.CreateObject(REGISTER_USER_TYPE)
        
        # fill in the certificate and key
        msg.message_parameters_reference.certificate = """-----BEGIN CERTIFICATE-----
MIIEMzCCAxugAwIBAgICBQAwDQYJKoZIhvcNAQEFBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTAxMTE4MjIyNTA2WhcNMTAxMTE5MTAzMDA2
WjBvMRMwEQYKCZImiZPyLGQBGRMDb3JnMRcwFQYKCZImiZPyLGQBGRMHY2lsb2dvbjELMAkGA1UE
BhMCVVMxFzAVBgNVBAoTDlByb3RlY3ROZXR3b3JrMRkwFwYDVQQDExBSb2dlciBVbndpbiBBMjU0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtb
g0kKLmivgoVsA4U7swNDRH6svW242THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq
7LWt2T6GVVA10ex5WAeB/o7br/Z4U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b
2lUtQc6cjuHRDU4NknXaVMXTBHKPM40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4
dszsqn2SC8YDw1xrujvW2Bd7Q7BwMQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+
6M6SMQIDAQABo4HdMIHaMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoG
CCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAgEwagYDVR0fBGMwYTAuoCygKoYoaHR0
cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2ljLmNybDAvoC2gK4YpaHR0cDovL2NybC5k
b2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwHwYDVR0RBBgwFoEUaXRzYWdyZWVuMUB5YWhv
by5jb20wDQYJKoZIhvcNAQEFBQADggEBAEYHQPMY9Grs19MHxUzMwXp1GzCKhGpgyVKJKW86PJlr
HGruoWvx+DLNX75Oj5FC4t8bOUQVQusZGeGSEGegzzfIeOI/jWP1UtIjzvTFDq3tQMNvsgROSCx5
CkpK4nS0kbwLux+zI7BWON97UpMIzEeE05pd7SmNAETuWRsHMP+x6i7hoUp/uad4DwbzNUGIotdK
f8b270icOVgkOKRdLP/Q4r/x8skKSCRz1ZsRdR+7+B/EgksAJj7Ut3yiWoUekEMxCaTdAHPTMD/g
Mh9xL90hfMJyoGemjJswG5g3fAdTP/Lv0I6/nWeH/cLjwwpQgIEjEAVXl7KHuzX5vPD/wqQ=
-----END CERTIFICATE-----"""
        msg.message_parameters_reference.rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtbg0kKLmivgoVsA4U7swNDRH6svW24
2THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq7LWt2T6GVVA10ex5WAeB/o7br/Z4
U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b2lUtQc6cjuHRDU4NknXaVMXTBHKP
M40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4dszsqn2SC8YDw1xrujvW2Bd7Q7Bw
MQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+6M6SMQIDAQABAoIBAAc/Ic97ZDQ9
tFh76wzVWj4SVRuxj7HWSNQ+Uzi6PKr8Zy182Sxp74+TuN9zKAppCQ8LEKwpkKtEjXsl8QcXn38m
sXOo8+F1He6FaoRQ1vXi3M1boPpefWLtyZ6rkeJw6VP3MVG5gmho0VaOqLieWKLP6fXgZGUhBvFm
yxUPoNgXJPLjJ9pNGy4IBuQDudqfJeqnbIe0GOXdB1oLCjAgZlTR4lFA92OrkMEldyVp72iYbffN
4GqoCEiHi8lX9m2kvwiQKRnfH1dLnnPBrrwatu7TxOs02HpJ99wfzKRy4B1SKcB0Gs22761r+N/M
oO966VxlkKYTN+soN5ID9mQmXJkCgYEA/h2bqH9mNzHhzS21x8mC6n+MTyYYKVlEW4VSJ3TyMKlR
gAjhxY/LUNeVpfxm2fY8tvQecWaW3mYQLfnvM7f1FeNJwEwIkS/yaeNmcRC6HK/hHeE87+fNVW/U
ftU4FW5Krg3QIYxcTL2vL3JU4Auu3E/XVcx0iqYMGZMEEDOcQPcCgYEA6sLLIeOdngUvxdA4KKEe
qInDpa/coWbtAlGJv8NueYTuD3BYJG5KoWFY4TVfjQsBgdxNxHzxb5l9PrFLm9mRn3iiR/2EpQke
qJzs87K0A/sxTVES29w1PKinkBkdu8pNk10TxtRUl/Ox3fuuZPvyt9hi5c5O/MCKJbjmyJHuJBcC
gYBiAJM2oaOPJ9q4oadYnLuzqms3Xy60S6wUS8+KTgzVfYdkBIjmA3XbALnDIRudddymhnFzNKh8
rwoQYTLCVHDd9yFLW0d2jvJDqiKo+lV8mMwOFP7GWzSSfaWLILoXcci1ZbheJ9607faxKrvXCEpw
xw36FfbgPfeuqUdI5E6fswKBgFIxCu99gnSNulEWemL3LgWx3fbHYIZ9w6MZKxIheS9AdByhp6px
lt1zeKu4hRCbdtaha/TMDbeV1Hy7lA4nmU1s7dwojWU+kSZVcrxLp6zxKCy6otCpA1aOccQIlxll
Vc2vO7pUIp3kqzRd5ovijfMB5nYwygTB4FwepWY5eVfXAoGBAIqrLKhRzdpGL0Vp2jwtJJiMShKm
WJ1c7fBskgAVk8jJzbEgMxuVeurioYqj0Cn7hFQoLc+npdU5byRti+4xjZBXSmmjo4Y7ttXGvBrf
c2bPOQRAYZyD2o+/MHBDsz7RWZJoZiI+SJJuE4wphGUsEbI2Ger1QW9135jKp6BsY2qZ
-----END RSA PRIVATE KEY-----"""

        # try to register this user for the first time
        reply = yield self.aisc.registerUser(msg)
        log.debug('registerUser returned:\n'+str(reply))
        log.debug('registerUser returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != OOI_ID_TYPE:
            self.fail('response does not contain an OOI_ID GPB')
        FirstOoiId = reply.message_parameters_reference[0].ooi_id
        log.info("test_registerUser: first time registration received ooi_id = "+str(reply.message_parameters_reference[0].ooi_id))
            
        # try to re-register this user for a second time
        reply = yield self.aisc.registerUser(msg)
        log.debug('registerUser returned:\n'+str(reply))
        log.debug('registerUser returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != OOI_ID_TYPE:
            self.fail('response does not contain an OOI_ID GPB')
        if FirstOoiId != reply.message_parameters_reference[0].ooi_id:
            self.fail("re-registration did not return the same OoiId as registration")
        log.info("test_registerUser: re-registration received ooi_id = "+str(reply.message_parameters_reference[0].ooi_id))
        
        # try to send registerUser the wrong GPB
        # create a bad request GPBs
        msg = yield mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS bad request')
        reply = yield self.aisc.registerUser(msg)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB is not an AIS_RESPONSE_ERROR_TYPE GPB')

        # try to send registerUser incomplete GPBs
        # create a bad GPB request w/o payload
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS bad request')
        reply = yield self.aisc.registerUser(msg)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to registerUser is not an AIS_RESPONSE_ERROR_TYPE GPB')
        # create a bad GPB request w/o certificate
        msg.message_parameters_reference = msg.CreateObject(REGISTER_USER_TYPE)
        reply = yield self.aisc.registerUser(msg)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to registerUser is not an AIS_RESPONSE_ERROR_TYPE GPB')
        # create a bad GPB request w/o key
        msg.message_parameters_reference.certificate = "dumming certificate"
        reply = yield self.aisc.registerUser(msg)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to registerUser is not an AIS_RESPONSE_ERROR_TYPE GPB')
            
    @defer.inlineCallbacks
    def test_updateUserDispatcherQueue(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        
        # create the register_user request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS RegisterUser request')
        msg.message_parameters_reference = msg.CreateObject(REGISTER_USER_TYPE)
        
        # fill in the certificate and key
        msg.message_parameters_reference.certificate = """-----BEGIN CERTIFICATE-----
MIIEMzCCAxugAwIBAgICBQAwDQYJKoZIhvcNAQEFBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTAxMTE4MjIyNTA2WhcNMTAxMTE5MTAzMDA2
WjBvMRMwEQYKCZImiZPyLGQBGRMDb3JnMRcwFQYKCZImiZPyLGQBGRMHY2lsb2dvbjELMAkGA1UE
BhMCVVMxFzAVBgNVBAoTDlByb3RlY3ROZXR3b3JrMRkwFwYDVQQDExBSb2dlciBVbndpbiBBMjU0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtb
g0kKLmivgoVsA4U7swNDRH6svW242THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq
7LWt2T6GVVA10ex5WAeB/o7br/Z4U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b
2lUtQc6cjuHRDU4NknXaVMXTBHKPM40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4
dszsqn2SC8YDw1xrujvW2Bd7Q7BwMQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+
6M6SMQIDAQABo4HdMIHaMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoG
CCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAgEwagYDVR0fBGMwYTAuoCygKoYoaHR0
cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2ljLmNybDAvoC2gK4YpaHR0cDovL2NybC5k
b2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwHwYDVR0RBBgwFoEUaXRzYWdyZWVuMUB5YWhv
by5jb20wDQYJKoZIhvcNAQEFBQADggEBAEYHQPMY9Grs19MHxUzMwXp1GzCKhGpgyVKJKW86PJlr
HGruoWvx+DLNX75Oj5FC4t8bOUQVQusZGeGSEGegzzfIeOI/jWP1UtIjzvTFDq3tQMNvsgROSCx5
CkpK4nS0kbwLux+zI7BWON97UpMIzEeE05pd7SmNAETuWRsHMP+x6i7hoUp/uad4DwbzNUGIotdK
f8b270icOVgkOKRdLP/Q4r/x8skKSCRz1ZsRdR+7+B/EgksAJj7Ut3yiWoUekEMxCaTdAHPTMD/g
Mh9xL90hfMJyoGemjJswG5g3fAdTP/Lv0I6/nWeH/cLjwwpQgIEjEAVXl7KHuzX5vPD/wqQ=
-----END CERTIFICATE-----"""
        msg.message_parameters_reference.rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtbg0kKLmivgoVsA4U7swNDRH6svW24
2THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq7LWt2T6GVVA10ex5WAeB/o7br/Z4
U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b2lUtQc6cjuHRDU4NknXaVMXTBHKP
M40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4dszsqn2SC8YDw1xrujvW2Bd7Q7Bw
MQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+6M6SMQIDAQABAoIBAAc/Ic97ZDQ9
tFh76wzVWj4SVRuxj7HWSNQ+Uzi6PKr8Zy182Sxp74+TuN9zKAppCQ8LEKwpkKtEjXsl8QcXn38m
sXOo8+F1He6FaoRQ1vXi3M1boPpefWLtyZ6rkeJw6VP3MVG5gmho0VaOqLieWKLP6fXgZGUhBvFm
yxUPoNgXJPLjJ9pNGy4IBuQDudqfJeqnbIe0GOXdB1oLCjAgZlTR4lFA92OrkMEldyVp72iYbffN
4GqoCEiHi8lX9m2kvwiQKRnfH1dLnnPBrrwatu7TxOs02HpJ99wfzKRy4B1SKcB0Gs22761r+N/M
oO966VxlkKYTN+soN5ID9mQmXJkCgYEA/h2bqH9mNzHhzS21x8mC6n+MTyYYKVlEW4VSJ3TyMKlR
gAjhxY/LUNeVpfxm2fY8tvQecWaW3mYQLfnvM7f1FeNJwEwIkS/yaeNmcRC6HK/hHeE87+fNVW/U
ftU4FW5Krg3QIYxcTL2vL3JU4Auu3E/XVcx0iqYMGZMEEDOcQPcCgYEA6sLLIeOdngUvxdA4KKEe
qInDpa/coWbtAlGJv8NueYTuD3BYJG5KoWFY4TVfjQsBgdxNxHzxb5l9PrFLm9mRn3iiR/2EpQke
qJzs87K0A/sxTVES29w1PKinkBkdu8pNk10TxtRUl/Ox3fuuZPvyt9hi5c5O/MCKJbjmyJHuJBcC
gYBiAJM2oaOPJ9q4oadYnLuzqms3Xy60S6wUS8+KTgzVfYdkBIjmA3XbALnDIRudddymhnFzNKh8
rwoQYTLCVHDd9yFLW0d2jvJDqiKo+lV8mMwOFP7GWzSSfaWLILoXcci1ZbheJ9607faxKrvXCEpw
xw36FfbgPfeuqUdI5E6fswKBgFIxCu99gnSNulEWemL3LgWx3fbHYIZ9w6MZKxIheS9AdByhp6px
lt1zeKu4hRCbdtaha/TMDbeV1Hy7lA4nmU1s7dwojWU+kSZVcrxLp6zxKCy6otCpA1aOccQIlxll
Vc2vO7pUIp3kqzRd5ovijfMB5nYwygTB4FwepWY5eVfXAoGBAIqrLKhRzdpGL0Vp2jwtJJiMShKm
WJ1c7fBskgAVk8jJzbEgMxuVeurioYqj0Cn7hFQoLc+npdU5byRti+4xjZBXSmmjo4Y7ttXGvBrf
c2bPOQRAYZyD2o+/MHBDsz7RWZJoZiI+SJJuE4wphGUsEbI2Ger1QW9135jKp6BsY2qZ
-----END RSA PRIVATE KEY-----"""

        # try to register this user for the first time
        reply = yield self.aisc.registerUser(msg)
        log.debug('registerUser returned:\n'+str(reply))
        log.debug('registerUser returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != OOI_ID_TYPE:
            self.fail('response does not contain an OOI_ID GPB')
        FirstOoiId = reply.message_parameters_reference[0].ooi_id
        log.info("test_registerUser: first time registration received ooi_id = "+str(reply.message_parameters_reference[0].ooi_id))

        # create the updateUserDispatcherQueue request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS updateUserDispatcherQueue request')
        msg.message_parameters_reference = msg.CreateObject(UPDATE_USER_DISPATCH_QUEUE_TYPE)
        msg.message_parameters_reference.user_ooi_id = FirstOoiId
        msg.message_parameters_reference.queue_name = "some_dispatcher_queue"
        reply = yield self.aisc.updateUserDispatcherQueue(msg)
        log.debug('updateUserDispatcherQueue returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')

        # try to send updateUserDispatcherQueue the wrong GPB
        # create a bad request GPBs
        msg = yield mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS bad request')
        reply = yield self.aisc.updateUserDispatcherQueue(msg)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to updateUserDispatcherQueue is not an AIS_RESPONSE_ERROR_TYPE GPB')

        # try to send updateUserDispatcherQueue incomplete GPBs
        # create a bad GPB request w/o payload
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS bad request')
        reply = yield self.aisc.updateUserDispatcherQueue(msg)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to updateUserDispatcherQueue to is not an AIS_RESPONSE_ERROR_TYPE GPB')
        # create a bad GPB request w/o ooi_id
        msg.message_parameters_reference = msg.CreateObject(UPDATE_USER_DISPATCH_QUEUE_TYPE)
        reply = yield self.aisc.updateUserDispatcherQueue(msg)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to updateUserDispatcherQueue is not an AIS_RESPONSE_ERROR_TYPE GPB')
        # create a bad GPB request w/o key
        msg.message_parameters_reference.user_ooi_id = "Some-ooi_id"
        reply = yield self.aisc.updateUserDispatcherQueue(msg)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to updateUserDispatcherQueue is not an AIS_RESPONSE_ERROR_TYPE GPB')

    @defer.inlineCallbacks
    def test_updateUserEmail(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        
        # create the register_user request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS RegisterUser request')
        msg.message_parameters_reference = msg.CreateObject(REGISTER_USER_TYPE)
        
        # fill in the certificate and key
        msg.message_parameters_reference.certificate = """-----BEGIN CERTIFICATE-----
MIIEMzCCAxugAwIBAgICBQAwDQYJKoZIhvcNAQEFBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTAxMTE4MjIyNTA2WhcNMTAxMTE5MTAzMDA2
WjBvMRMwEQYKCZImiZPyLGQBGRMDb3JnMRcwFQYKCZImiZPyLGQBGRMHY2lsb2dvbjELMAkGA1UE
BhMCVVMxFzAVBgNVBAoTDlByb3RlY3ROZXR3b3JrMRkwFwYDVQQDExBSb2dlciBVbndpbiBBMjU0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtb
g0kKLmivgoVsA4U7swNDRH6svW242THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq
7LWt2T6GVVA10ex5WAeB/o7br/Z4U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b
2lUtQc6cjuHRDU4NknXaVMXTBHKPM40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4
dszsqn2SC8YDw1xrujvW2Bd7Q7BwMQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+
6M6SMQIDAQABo4HdMIHaMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoG
CCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAgEwagYDVR0fBGMwYTAuoCygKoYoaHR0
cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2ljLmNybDAvoC2gK4YpaHR0cDovL2NybC5k
b2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwHwYDVR0RBBgwFoEUaXRzYWdyZWVuMUB5YWhv
by5jb20wDQYJKoZIhvcNAQEFBQADggEBAEYHQPMY9Grs19MHxUzMwXp1GzCKhGpgyVKJKW86PJlr
HGruoWvx+DLNX75Oj5FC4t8bOUQVQusZGeGSEGegzzfIeOI/jWP1UtIjzvTFDq3tQMNvsgROSCx5
CkpK4nS0kbwLux+zI7BWON97UpMIzEeE05pd7SmNAETuWRsHMP+x6i7hoUp/uad4DwbzNUGIotdK
f8b270icOVgkOKRdLP/Q4r/x8skKSCRz1ZsRdR+7+B/EgksAJj7Ut3yiWoUekEMxCaTdAHPTMD/g
Mh9xL90hfMJyoGemjJswG5g3fAdTP/Lv0I6/nWeH/cLjwwpQgIEjEAVXl7KHuzX5vPD/wqQ=
-----END CERTIFICATE-----"""
        msg.message_parameters_reference.rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtbg0kKLmivgoVsA4U7swNDRH6svW24
2THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq7LWt2T6GVVA10ex5WAeB/o7br/Z4
U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b2lUtQc6cjuHRDU4NknXaVMXTBHKP
M40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4dszsqn2SC8YDw1xrujvW2Bd7Q7Bw
MQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+6M6SMQIDAQABAoIBAAc/Ic97ZDQ9
tFh76wzVWj4SVRuxj7HWSNQ+Uzi6PKr8Zy182Sxp74+TuN9zKAppCQ8LEKwpkKtEjXsl8QcXn38m
sXOo8+F1He6FaoRQ1vXi3M1boPpefWLtyZ6rkeJw6VP3MVG5gmho0VaOqLieWKLP6fXgZGUhBvFm
yxUPoNgXJPLjJ9pNGy4IBuQDudqfJeqnbIe0GOXdB1oLCjAgZlTR4lFA92OrkMEldyVp72iYbffN
4GqoCEiHi8lX9m2kvwiQKRnfH1dLnnPBrrwatu7TxOs02HpJ99wfzKRy4B1SKcB0Gs22761r+N/M
oO966VxlkKYTN+soN5ID9mQmXJkCgYEA/h2bqH9mNzHhzS21x8mC6n+MTyYYKVlEW4VSJ3TyMKlR
gAjhxY/LUNeVpfxm2fY8tvQecWaW3mYQLfnvM7f1FeNJwEwIkS/yaeNmcRC6HK/hHeE87+fNVW/U
ftU4FW5Krg3QIYxcTL2vL3JU4Auu3E/XVcx0iqYMGZMEEDOcQPcCgYEA6sLLIeOdngUvxdA4KKEe
qInDpa/coWbtAlGJv8NueYTuD3BYJG5KoWFY4TVfjQsBgdxNxHzxb5l9PrFLm9mRn3iiR/2EpQke
qJzs87K0A/sxTVES29w1PKinkBkdu8pNk10TxtRUl/Ox3fuuZPvyt9hi5c5O/MCKJbjmyJHuJBcC
gYBiAJM2oaOPJ9q4oadYnLuzqms3Xy60S6wUS8+KTgzVfYdkBIjmA3XbALnDIRudddymhnFzNKh8
rwoQYTLCVHDd9yFLW0d2jvJDqiKo+lV8mMwOFP7GWzSSfaWLILoXcci1ZbheJ9607faxKrvXCEpw
xw36FfbgPfeuqUdI5E6fswKBgFIxCu99gnSNulEWemL3LgWx3fbHYIZ9w6MZKxIheS9AdByhp6px
lt1zeKu4hRCbdtaha/TMDbeV1Hy7lA4nmU1s7dwojWU+kSZVcrxLp6zxKCy6otCpA1aOccQIlxll
Vc2vO7pUIp3kqzRd5ovijfMB5nYwygTB4FwepWY5eVfXAoGBAIqrLKhRzdpGL0Vp2jwtJJiMShKm
WJ1c7fBskgAVk8jJzbEgMxuVeurioYqj0Cn7hFQoLc+npdU5byRti+4xjZBXSmmjo4Y7ttXGvBrf
c2bPOQRAYZyD2o+/MHBDsz7RWZJoZiI+SJJuE4wphGUsEbI2Ger1QW9135jKp6BsY2qZ
-----END RSA PRIVATE KEY-----"""

        # try to register this user for the first time
        reply = yield self.aisc.registerUser(msg)
        log.debug('registerUser returned:\n'+str(reply))
        log.debug('registerUser returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != OOI_ID_TYPE:
            self.fail('response does not contain an OOI_ID GPB')
        FirstOoiId = reply.message_parameters_reference[0].ooi_id
        log.info("test_registerUser: first time registration received ooi_id = "+str(reply.message_parameters_reference[0].ooi_id))

        # create the update Email request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS updateUserEmail request')
        msg.message_parameters_reference = msg.CreateObject(UPDATE_USER_EMAIL_TYPE)
        msg.message_parameters_reference.user_ooi_id = FirstOoiId
        msg.message_parameters_reference.email_address = "some_person@some_place.some_domain"
        reply = yield self.aisc.updateUserEmail(msg)
        log.debug('updateUserEmail returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')

        # try to send updateUserEmail the wrong GPB
        # create a bad request GPBs
        msg = yield mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS bad request')
        reply = yield self.aisc.updateUserEmail(msg)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to updateUserDispatcherQueue is not an AIS_RESPONSE_ERROR_TYPE GPB')

        # try to send updateUserDispatcherQueue incomplete GPBs
        # create a bad GPB request w/o payload
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS bad request')
        reply = yield self.aisc.updateUserEmail(msg)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to updateUserEmail to is not an AIS_RESPONSE_ERROR_TYPE GPB')
        # create a bad GPB request w/o ooi_id
        msg.message_parameters_reference = msg.CreateObject(UPDATE_USER_EMAIL_TYPE)
        reply = yield self.aisc.updateUserEmail(msg)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to updateUserEmail is not an AIS_RESPONSE_ERROR_TYPE GPB')
        # create a bad GPB request w/o emsil address
        msg.message_parameters_reference.user_ooi_id = "Some-ooi_id"
        reply = yield self.aisc.updateUserEmail(msg)
        if reply.MessageType != AIS_RESPONSE_ERROR_TYPE:
            self.fail('response to bad GPB to updateUserEmail is not an AIS_RESPONSE_ERROR_TYPE GPB')


    @defer.inlineCallbacks
    def test_createDataResource_success(self):
        raise unittest.SkipTest('This will be the test for a normal successful createDataResource')

    @defer.inlineCallbacks
    def test_createDataResource_failSource(self):
        raise unittest.SkipTest('This will be the test createDataResource when create source fails')
    
    @defer.inlineCallbacks
    def test_createDataResource_failSet(self):
        raise unittest.SkipTest('This will be the test createDataResource when create dataset fails')
    
    @defer.inlineCallbacks
    def test_createDataResource_failAssociation(self):
        raise unittest.SkipTest('This will be the test createDataResource when association fails')
    
    @defer.inlineCallbacks
    def test_createDataResource_failScheduling(self):
        raise unittest.SkipTest('This will be the test createDataResource when scheduling fails')







def _create_string_attribute(dataset, name, values):
    '''
    Helper method to create string attributes for variables and dataset groups
    '''
    atrib = dataset.CreateObject(attribute_type)
    atrib.name = name
    atrib.data_type= atrib.DataType.STRING
    atrib.array = dataset.CreateObject(stringArray_type)
    atrib.array.value.extend(values)
    return atrib

def _add_string_attribute(dataset, variable, name, values):
    '''
    Helper method to add string attributes to variable instances
    '''
    atrib = _create_string_attribute(dataset, name, values)
    
    atrib_ref = variable.attributes.add()
    atrib_ref.SetLink(atrib)        

