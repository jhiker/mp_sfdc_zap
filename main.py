#! /usr/bin/env python

import datetime, sys, os

from src.mixpanel_api import Mixpanel
from src.salesforce_mp_zap import SalesforceApi

import logging, argparse


arg_parser = argparse.ArgumentParser()


arg_parser.add_argument('-token', action='store', dest='token',
                    help='Security token')

arg_parser.add_argument('-pwd', action='store', dest='pwd',
                    help='Password')

arg_parser.add_argument('-user', action='store', dest='user',
                    help='Username')

arg_parser.add_argument('-task_status', action='store', dest='task_status',
                    help='Give a task status')

arg_parser.add_argument('-assigned', action='store', dest='assigned_to',
                    help='Give task assigned_to')

arg_parser.add_argument('-mp_api_key', action='store', dest='mp_api_key',
                    help='Mixpanel api key')

arg_parser.add_argument('-mp_api_secret', action='store', dest='mp_api_secret',
                    help='Mixpanel api secret')

arg_parser.add_argument('-events', action='store', dest="events", nargs='+', type=str)

arg_parser.add_argument('-subject', action='store', dest="subject_components", nargs='+', type=str)

arg_parser.add_argument('-dates', action='store', dest="dates", nargs='+', type=int)

arg_parser.add_argument('-sandbox', action='store', dest="sandbox", type=bool)

passed_args = arg_parser.parse_args()

print passed_args

token = passed_args.token
sfdc_pass = passed_args.pwd
sfdc_username = passed_args.user
EVENTS_TO_TRACK = passed_args.events
SANDBOX = passed_args.sandbox
DATE_START, DATE_END = passed_args.dates #Date interval to request 
SUBJECT_COMPONENTS = passed_args.subject_components


filename = "log" if os.path.isdir("log") else "/var/log"

logging.basicConfig(filename='%s/mp2sfdc_zap.log' % filename,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%Y %m %d %H:%M:%S',
                            level=logging.DEBUG)



def format_days_ago_in_y_m_d(d):
    return (datetime.date.today() - datetime.timedelta(days=d)).strftime("%Y-%m-%d")

def call_mp():
    mixpanel = Mixpanel(
        api_key = passed_args.mp_api_key,
        api_secret = passed_args.mp_api_secret
    )
    
    mixpanel.request(['export'], 
        {
        # if you want to specify an event, you can do that here:
        #'event': ['some event', 'some other event'],
        # if you want to specify a filter, you can do that here:
        #'where': '"Marshall" in properties["some property"]',
        'to_date': format_days_ago_in_y_m_d(DATE_END),
        'from_date': format_days_ago_in_y_m_d(DATE_START)
        })
    return mixpanel

def call_sf_api():
    return SalesforceApi(password = sfdc_pass, username = sfdc_username,
            security_token = token, sandbox = SANDBOX, assigned_to = passed_args.assigned_to,
            subject_components = passed_args.subject_components, task_status = passed_args.task_status)

if __name__ == '__main__':

    salesforce_api = call_sf_api()
    mixpanel = call_mp()
    stub = {"event":"Report Preview Clicked","properties":{"time":1426700933,"distinct_id":"jonathanleslie73@gmail.com","$browser":"Chrome","$city":"Jamaica","$initial_referrer":"$direct","$initial_referring_domain":"$direct","$lib_version":"2.4.0","$os":"Mac OS X","$referrer":"http://novarica.com/wp-login.php?redirect_to=http%3A%2F%2Fnovarica.com%2Fb_and_t_trends_workers_comp_2014%2F","$referring_domain":"novarica.com","$region":"New York","$screen_height":800,"$screen_width":1280,"Report Name":"Business and Technology Trends: Workers&#8217; Compensation","mp_country_code":"US","mp_lib":"web"}}    
    
    events = mixpanel.generate_salesforce_task_objects_by_event_type(EVENTS_TO_TRACK)
    for event in events:
        print "add '%s' event to %s for user %s" % \
                (str(event['event']), "Sandbox" if SANDBOX else "Production", event['properties']['distinct_id'])
        added = salesforce_api.create_sfdc_task_from_mp_object(event)
        if added: print "Success!"




