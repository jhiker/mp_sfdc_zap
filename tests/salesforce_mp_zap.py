import unittest
import time

from mock import patch

from simple_salesforce import Salesforce, SFType
from . import SalesforceApi, CustomMPDataError


class ValidTestCase(unittest.TestCase):

    stub_user = "test@test.com"

    mp_event_properties = {"time":1426700933, "distinct_id":stub_user,"$screen_height":800,"$screen_width":1280,"Name":"Test Name","mp_country_code":"US","mp_lib":"web"}   

    subject_components = ["Report Name"]

    mp_event_stub = {"event":"Purchase Item","properties": mp_event_properties}
    sf_ob_should_be = {'ActivityDate' : '2015-03-18',
        'Type': mp_event_stub['event'],
        'Description': str(mp_event_properties),
        'WhoId': 'User Id Here'
        }

    @patch('src.salesforce_mp_zap.logging')
    def setUp(self, mock_logging):
        self.mock_logging = mock_logging
        with patch.object(Salesforce, '__init__') as mock_sf:
            self.mock_sf = mock_sf
            mock_sf.return_value = None
            self.sf_api = SalesforceApi(self.subject_components, password = '', username = '',
                sandbox_name = '', prod_token = '', 
                dev_token = '', sandbox = '')
            self.sf_ob_should_be['Subject'] = self.sf_api.get_sf_task_subject(self.mp_event_stub['event'], self.mp_event_properties)
            
    def test_salesforce_called_on_init(self):
        '''Should inherit and instantiate Salesforce Object '''
        self.mock_sf.assert_called_once()

    def test_is_instance_of_API(self):
        self.assertIsInstance(self.sf_api, SalesforceApi)

    def test_is_instance_of_SF(self):
        self.assertIsInstance(self.sf_api, Salesforce)

    def test_logging(self):
        '''Should log every instance'''
        self.mock_logging.assert_called_once()

    def test_event_to_salesforce_task_object_calls_check_email_and_get_id_right(self):
        '''The covert function should call the email and id check function'''
        with patch.object(SalesforceApi, 'check_email_and_get_id') as mock_api:
            self.sf_api.event_to_salesforce_task_object(self.mp_event_stub)
            mock_api.assert_called_with(self.mp_event_stub['properties']['distinct_id'])
            
    def test_check_email_and_get_id(self):
        '''A valid email should call _get_user_ids'''
        with patch.object(SalesforceApi, '_get_user_ids') as mock_api:
            whoId = self.sf_ob_should_be['WhoId']
            mock_api.return_value = [whoId]
            user_id = self.sf_api.check_email_and_get_id(self.stub_user)
            mock_api.assert_called_with(self.stub_user)
            self.assertEquals(whoId, user_id)


    def test_event_to_salesforce_task_object_should_return_task(self):
        '''A valid email should call _get_user_ids'''
        with patch.object(SalesforceApi, 'check_email_and_get_id') as mock_api:
            mock_api.return_value = self.sf_ob_should_be['WhoId']
            task = self.sf_api.event_to_salesforce_task_object(self.mp_event_stub)
            mock_api.assert_called_with(self.mp_event_stub['properties']['distinct_id'])
            for k, v in self.sf_ob_should_be.items():
                self.assertIn(k, task)
                print task[k]
                self.assertEquals(task[k], v)

    def test_that_query_is_called_in_appropriate_lookup_methods(self):
        '''Tests that interactions with the SF query method are working appropriately'''
        with patch.object(SalesforceApi, 'query') as mock_query:
            user_ids = ["Test"]
            user_obj = {}
            for i in user_ids: user_obj['Id'] = i 
            mock_query.return_value = {'records': [user_obj] }
            return_ids = self.sf_api._get_user_ids(self.stub_user)
            mock_query.assert_called_once()
            self.assertEquals(return_ids, user_ids) 
            self.sf_api._save_activity_records(user_ids[0])
            self.assertEquals(self.sf_api.searched_records[user_ids[0]], mock_query.return_value['records'])

    def test_api_buffer_is_called_on_creating_task(self):
        '''Buffer should be called'''
        class CustomException(Exception):
            pass
        with patch.object(SalesforceApi, 'buffer_api', side_effect = CustomException('test')) as mock_sleep:
            mock_sleep.assert_called_once()
            self.assertRaises(CustomException, self.sf_api.create_sfdc_task_from_mp_object, '')

    def test_throttling_prev_on_creating_task(self):
        '''Shoulds save user as Nonetype, interrupt, and log on CustomMPDataError from event_to_salesforce_task_object'''
        with patch.object(SalesforceApi, 'create_dupeless_task') as mock_create:
            with patch.object(SalesforceApi, 'event_to_salesforce_task_object', side_effect = CustomMPDataError()) as mock_con:
                self.sf_api.create_sfdc_task_from_mp_object(self.mp_event_stub)
                self.assertEquals(self.sf_api.saved_users[self.stub_user], None)
                mock_con.assert_called_with(self.mp_event_stub)

    def test_create_dupless_task_searched_records_dupe(self):
        '''Should not lookup or save task if already searched  but raises CustomMPDataError'''
        with patch.object(SalesforceApi, '_save_activity_records') as mock_save:
            self.sf_api.searched_records[self.sf_ob_should_be['WhoId']] = [self.sf_ob_should_be]
            self.assertRaises( CustomMPDataError, self.sf_api.create_dupeless_task, self.sf_ob_should_be)
            assert not mock_save.called

    def test_create_dupless_task_found_in_sfdc_dupe(self):
        '''Should not lookup or save task if returned by salesforce' but raises CustomMPDataError'''
        with patch.object(SalesforceApi, 'query', return_value = {'records': [self.sf_ob_should_be]}) as mock_query:
            self.assertRaises( CustomMPDataError, self.sf_api.create_dupeless_task, self.sf_ob_should_be)
            mock_query.assert_called_once()

class InvalidTestCases(unittest.TestCase):

    '''Don't send crappy data to SFDC '''

    @patch('src.salesforce_mp_zap.logging')
    def setUp(self, mock_logging):
        self.mock_logging = mock_logging
        with patch.object(Salesforce, '__init__') as mock_sf:
            self.mock_sf = mock_sf
            mock_sf.return_value = None
            self.sf_api = SalesforceApi([], password = '', username = '',
                sandbox_name = '', prod_token = '', 
                dev_token = '', sandbox = '')

    def test_bad_email(self):
        '''Tests that SF query is not called and that non-email raises CustomMPDataError '''
        with patch.object(SalesforceApi, 'query', return_value = {'records': []}) as mock_query:
            stub_user = "12314"
            self.assertRaises(CustomMPDataError, self.sf_api.check_email_and_get_id, stub_user)
            self.assertFalse(mock_query.called)

    def test_nonexistent_email(self):
        '''Tests that SF query is called and that nonexistent email raises CustomMPDataError '''
        with patch.object(SalesforceApi, 'query', return_value = {'records': []}) as mock_query:
            mock_query.assert_called_once()
            self.assertRaises(CustomMPDataError, self.sf_api.check_email_and_get_id, ValidTestCase.stub_user)

    def test_nonexistent_email(self):
        '''Tests that missing event name is not sent to SFDC '''
        event = dict(ValidTestCase.mp_event_stub)

        del event['event']
        with patch.object(SalesforceApi, 'create_dupeless_task') as mock_create:
            self.assertRaises(KeyError, self.sf_api.event_to_salesforce_task_object, event)
            self.sf_api.create_sfdc_task_from_mp_object(event)
            self.assertFalse(mock_create.called)

    def test_nonexistent_properties(self):
        '''Tests that missing event properties is not sent to SFDC '''
        event = dict(ValidTestCase.mp_event_stub)
        del event['properties']
        with patch.object(SalesforceApi, 'create_dupeless_task') as mock_create:
            self.assertRaises(KeyError, self.sf_api.event_to_salesforce_task_object, event)
            self.sf_api.create_sfdc_task_from_mp_object(event)
            self.assertFalse(mock_create.called)

    def test_nonexistent_property_time(self):
        '''Tests that missing event property time is not sent to SFDC '''
        properties = dict(ValidTestCase.mp_event_stub['properties'])
        del properties['time']
        event = dict(ValidTestCase.mp_event_stub)
        event['properties'] = properties
        with patch.object(SalesforceApi, 'create_dupeless_task') as mock_create:
            self.assertRaises(KeyError, self.sf_api.event_to_salesforce_task_object, event)
            self.sf_api.create_sfdc_task_from_mp_object(event)
            self.assertFalse(mock_create.called)






