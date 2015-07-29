#! /usr/bin/env python
#
# 
# Author: Jon Leslie 2015
# Api Client for Mixpanel/Salesforce integration 
#
# 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
#  
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.



from simple_salesforce import Salesforce
from validate_email import validate_email
import logging, sys, json, datetime, time


class CustomMPDataError(Exception):
    pass

class SalesforceApi(Salesforce):

    '''Wrapper on the API that handles a mixpanel object, makes Task creation easier, handles logging'''

    def __init__(self, **kwargs):
        #This is how the subject is created:
        self.subject_components = kwargs['subject_components']
        #This is the state of the Salesforce task:
        self.task_status = kwargs["task_status"]
        self.searched_records, self.saved_users = {}, {}
        Salesforce.__init__(self, **kwargs)
        self.owner_id = self.get_ownerid_from_assigned_to_name(kwargs["assigned_to"])

    @staticmethod
    def convert_time_stamp_to_sf_date_format(d):
        return datetime.datetime.fromtimestamp(int(d)).strftime('%Y-%m-%d')


    def get_sf_task_subject(self, event_name, properties):
        replace_quotes = lambda x: x.replace('&#8217;', "'")
        try:
            subject =  event_name + ": " +  " - ".join([replace_quotes(properties[d]) for d in self.subject_components])
        except KeyError:
            subject = event_name
            logging.info('Incorrect subject_components %s' % str(subject_components))
        return subject 

    def event_to_salesforce_task_object(self, event):
        '''
        Assumes distinct_id is email.
        Returns object of distinct_id and salesforce task object w/o Id
        The subject is created from a list of keys.  The description: the report properties.
        '''

        event_name = event['event']
        properties = event['properties']
        subject = self.get_sf_task_subject(event_name, properties)
        date_ = self.convert_time_stamp_to_sf_date_format(properties['time'])

        task = {
            'ActivityDate': date_,
            'Status': self.task_status,
            'Type': event_name,
            'Subject': subject,
            'Description': str(properties),
            'OwnerId': self.owner_id
        }

        user_email = properties['distinct_id']

        if user_email in self.saved_users and self.saved_users[user_email] is None:
            raise CustomMPDataError('Already checked and invalid')
        user_id = self.check_email_and_get_id(user_email)
        task['WhoId'] = user_id 
        return task

    def _get_user_ids(self, user_email):
        '''Returns list of user_ids for email'''
        data = self.query("SELECT Id FROM Contact WHERE Email = '%s'" % user_email)
        return [datum['Id'] for datum in data['records']]
    
    def get_ownerid_from_assigned_to_name(self, assignee):
        data = self.query("SELECT Id from User where Name='%s'" % assignee)
        return data['records'][0]['Id']

    @staticmethod
    def buffer_api():
        #max 20 per second 
        time.sleep(0.05)

    def _save_activity_records(self, user_id):
        '''Returns tasks for given user_id'''
        data = self.query("SELECT WhoId, ActivityDate, Subject FROM Task WHERE WhoId = '%s' " % user_id)
        self.searched_records[user_id] = data['records']

    def create_dupeless_task(self, task):
        '''
        Check that task does not already exist (first in memory then through API call),
        If not, create Salesforce task
        '''
        user_id = task['WhoId']
        if user_id not in self.searched_records:
            self._save_activity_records(user_id)
        for record in self.searched_records[user_id]:
            compare_task_record = lambda item: \
                str(task[item].encode('ascii', 'ignore')).strip() == str(record[item].encode('ascii', 'ignore')).strip()  
            try:
                if compare_task_record('ActivityDate') and compare_task_record('Subject'):
                    raise CustomMPDataError("Task '%s' already created...skipping" % task['Subject'])
            except KeyError:
                pass
            except AttributeError:
                pass
        print task 
        self.Task.create(task)
        logging.info('Created task "%s" w date %s for user %s' % tuple([task[i] for i in "Subject", "ActivityDate", "WhoId"]))

    def check_email_and_get_id(self, email):
        '''Checks if id is an email, then if email exists in SFDC instance, then returns first'''
        if not validate_email(email):
            raise CustomMPDataError('Not a valid email') 
        user_ids = self._get_user_ids(email)
        if not len(user_ids):
            raise CustomMPDataError('No user with that email')
        if len(user_ids) > 1:
            logging.warning('Multiple contacts with same email %s.  Taking first record' % email)
        self.saved_users[email]= user_ids[0]
        return user_ids[0]

    def create_sfdc_task_from_mp_object(self, event, **kwargs):
        '''
        Creates a new task, if possible (and desirable, through Salesforce API
        Else logs reason why not 
        '''
        self.buffer_api()
        try:
            try:
                #Create task from object and id lookup 
                task = self.event_to_salesforce_task_object(event)
                #Save task:
                self.create_dupeless_task(task)
                return True 
            except CustomMPDataError as e:
                id_ = event['properties']['distinct_id']
                self.saved_users[id_] = None
                logging.info('User %s; Error: %s' % (id_, str(e)))
        except KeyError as e:
            logging.info('Problem %s with mixpanel event %s' % (str(e), str(event) )) 





