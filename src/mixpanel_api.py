#! /usr/bin/env python
#
# Mixpanel, Inc. -- http://mixpanel.com/
#
# Python API client library export Mixpanel event data to a CSV file
#
# Copyright 2010-2014 Mixpanel, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Edited by Jon Leslie 2015 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
import urllib
import time
import sys
import csv, codecs, cStringIO
try:
    import json
except ImportError:
    import simplejson as json
import datetime, sys, time


def getOptions():
    print "We are going to ask a few questions to get your data\n"
    api_key = raw_input("API Key: ")
    api_secret = raw_input("API Secret: ")
    from_date = raw_input("From Date (YYYY-MM-DD): ")
    to_date = raw_input("To Date (YYYY-MM-DD) - cannot be today or in the future!: ")
    options = {'api_key': api_key,
            'api_secret': api_secret,
            'to_date': to_date,
            'from_date': from_date
            }
    return options

def getSubKeys(listOfDicts):
    '''a method for getting keys from a list of dictionaries'''
    subkeys = set()
    for event_dict in listOfDicts:
        if event_dict[u'properties']:
            subkeys.update(set(event_dict[u'properties'].keys()))
        else:
            pass
    return subkeys

class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row): 
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)
 
class Mixpanel(object):

    ENDPOINT = 'http://data.mixpanel.com/api'
    VERSION = '2.0'

    def __init__(self, api_key, api_secret, events_to_track = []):
        self.api_key = api_key
        self.api_secret = api_secret
        self.events_to_track = set(events_to_track)

    def request(self, methods, params, format='json'):
        """
            methods - List of methods to be joined, e.g. ['events', 'properties', 'values']
                      will give us http://mixpanel.com/api/2.0/events/properties/values/
            params - Extra parameters associated with method
        """
        assert self.api_key != '' and self.api_secret != '',\
            'Your API KEY and API SECRET are not set. Add these keys to the script and run again' 
        params['api_key'] = self.api_key
        params['expire'] = int(time.time()) + 600   # Grant this request 10 minutes.
        params['format'] = format
        if 'sig' in params: del params['sig']
        params['sig'] = self.hash_args(params)

        request_url = '/'.join([self.ENDPOINT, str(self.VERSION)] + methods) + '/?' + self.unicode_urlencode(params)
        print request_url
        request = urllib.urlopen(request_url)
        data_output = request.read()
        self.data = data_output

    def unicode_urlencode(self, params):
        """
            Convert lists to JSON encoded strings, and correctly handle any
            unicode URL parameters.
        """
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        return urllib.urlencode(
            [(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params]
        )

    def hash_args(self, args, secret=None):
        """
            Hashes arguments by joining key=value pairs, appending a secret, and
            then taking the MD5 hex digest.
        """
        for a in args:
            if isinstance(args[a], list): args[a] = json.dumps(args[a])

        args_joined = ''
        for a in sorted(args.keys()):
            if isinstance(a, unicode):
                args_joined += a.encode('utf-8')
            else:
                args_joined += str(a)

            args_joined += '='

            if isinstance(args[a], unicode):
                args_joined += args[a].encode('utf-8')
            else:
                args_joined += str(args[a])

        hash = hashlib.md5(args_joined)

        if secret:
            hash.update(secret)
        elif self.api_secret:
            hash.update(self.api_secret)
        return hash.hexdigest()

    def generate_salesforce_task_objects_by_event_type(self, events = None):
        """
        generates events--filted by events if provided
        """
        event_raw = self.data.split('\n')
        event_raw.pop()

        if events: 
            events = set(events)

        event_list = []
        for event in event_raw:
            event_dict = json.loads(event)
            if not events or event_dict['event'] in events:
                yield event_dict


    def export_csv(self):
        """
        takes mixpanel export API json and returns a csv file
        """
        event_raw = self.data.split('\n')
        outfileName = 'mixpanel_%s' %  str(int(time.time()))
        '''remove the lost line, which is a newline'''
        event_raw.pop()

        event_list = []
        for event in event_raw:
            event_json = json.loads(event)
            event_list.append(event_json)

        subkeys = getSubKeys(event_list)

        #open the file
        f = open(outfileName, 'w')
        writer = UnicodeWriter(f)
        
        #write the file header
        f.write(codecs.BOM_UTF8)

        #writer the top row
        header = [u'event']
        for key in subkeys:
            header.append(u'property_' + key)
        writer.writerow(header)

        #write all the data rows
        for event in event_list:
            line = []
            #get the event name
            try:
                line.append(event[u'event'])
            except KeyError:
                line.append("")
            #get each property value
            for subkey in subkeys:
                try:
                    line.append(unicode(event[u'properties'][subkey]))
                except KeyError:
                    line.append("")
            #write the line
            writer.writerow(line)


