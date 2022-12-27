import requests
import json
import re
import time
import csv
import pandas as pd
from datetime import datetime
from typing import List, Set, Dict, Tuple, Optional
from PrecipitationParser import PrecipitationParser
import os


pd.options.display.width = 0

"""
TO DO LIST, FEATURE EDITION
Consolidate files
Log that actually works
Appending with duplicate drops
    - Remember to Where on max(effective) from the historical to save time

* Map top (eventually, all we care about) HS addresses and service regions
* Map top Pharma mfg addresses and ring a loud bell when someone gets hit hard
* Automate so this runs every 3 hours or so
* Fold in FEMA's API for large disasters

* Something with data science that can let us know how volume will be affected before a storm hits

TO DO, RESEARCH

TO DO LIST
Cron job to run this every day, appending data
Visualization of data model
"""

def retry_get(url, url_params = {}, max_retries = 10, pass_errors = False):
    response = requests.get(url, params = url_params, verify=should_verify)
    retries = 0
    while retries < max_retries and response.status_code in (408, 502, 503, 504):
        print("Retryable status received, retry", retries)
        time.sleep(5)
        retries += 1
        response = requests.get(url, params = url_params, verify=should_verify)
    if pass_errors:
        return (response.status_code, response)
    elif (response.status_code < 200 or response.status_code > 299):
        print("Error:", response.status_code)
        raise("Too many failed retries")
    else:
        return response

class NWSFetcher():
    def __init__(self, event_params):
        self.event_params = event_params
        self.cursor = None
        self.finished = False
        self.event_params = event_params
        self.event_params_index = 0

    def fetchWeatherData(self):
        params = self.event_params[self.event_params_index]
        retries = 0
        if self.cursor is None:
            try:
                response = retry_get('https://api.weather.gov/alerts/?', url_params = params, pass_errors = False)
            except:
                print("Too many failed retries")
        else:
            try:
                response = retry_get(self.cursor, url_params = params, pass_errors = False)
                data_raw = json.loads(str(response.text))
            except:
                print("Too many failed retries")
        try:
            data_raw = json.loads(str(response.text))
        except:
            print("Json Load error")

        if 'pagination' in data_raw.keys():
            self.cursor = data_raw['pagination']['next']
        else:
            self.event_params_index += 1
            self.cursor = None
            if self.event_params_index == len(self.event_params):
                self.finished = True
            else:
                print(
                    f"Now checking {event_params[self.event_params_index]['event']}")
        return data_raw

class UGCFetcher():
    def __init__(self):
        self.max_retries = 5
        self.land_url = 'https://api.weather.gov/zones/land/'
        self.county_url = 'https://api.weather.gov/zones/county/'
        self.ugc_df = pd.read_csv('ugc_master.csv')
        self.goodzones = (
            r"(AL|AK|AS|AR|AZ|CA|CO|CT|DE|DC|FL|GA|GU|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI"
            r"|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|PR|RI|SC|SD|TN|TX|UT|VT|VI"
            r"|VA|WA|WV|WI|WY|MP|PW|FM|MH)[ZC][0-9]{3}"
        )
        self.zones_to_check = []

    def load_unknowns(self, ugc):
        known = self.ugc_df[self.ugc_df['ugc'] == ugc]
        if ~known.empty:
            self.zones_to_check.append(ugc)

    def update_zones(self):
        #with open('data/raw/zones_to_check.txt', 'w', encoding='utf-8') as f:
        #   f.write(str(self.zones_to_check))
        new_ugc_df = pd.DataFrame()
        county_zones = []
        fails = []
        self.zones_to_check = set(self.zones_to_check) - set(self.ugc_df['ugc'])
        print("Checking", len(self.zones_to_check),"zones")
        for i, ugc in enumerate(set(self.zones_to_check)):
            time.sleep(2)
            new_row = {'land': 1}
            type_id = re.search("\w{2}(?P<identifier>C|Z)",ugc).groupdict()
            url = self.land_url if type_id['identifier'] == 'Z' else self.county_url
            try:
                code, response = retry_get(url + ugc.strip(), pass_errors = True)
                if code == '404':
                    print("Neither land nor county:",ugc)
                    new_row['land'] = 0
                    new_row['ugc'] = ugc
                    new_ugc_df = pd.concat([new_ugc_df,pd.DataFrame([new_row])])
                    continue
                data_raw = json.loads(str(response.text))['properties']
                new_row = new_row | {
                    'ugc': ugc,
                    'type': data_raw['type'],
                    'name': data_raw['name'],
                    'state': data_raw['state']
                }
                new_ugc_df = pd.concat([new_ugc_df,pd.DataFrame([new_row])])
            except:
                fails.append({
                'ugc': ugc,
                'code': code,
                'id': type_id
                })
                print("Failed to add",ugc)
            print(f"Added zone {i} of {len(self.zones_to_check)} ({ugc})")
        pd.DataFrame(fails).to_csv('zones_to_check_again.csv')
        new_ugc_df.to_csv('ugc_master.csv')

should_verify = True
# If behind the company VPN, you'll need to be bad and add verify=False here
# Definitely, extremely don't leave verify = false if you're using this for real
# Can't emphasize that enough. Seriously.

process_start = time.time()

now = datetime.now()  # current date and time
year = now.strftime("%Y")
month = now.strftime("%m")
start_time = datetime.strptime(
    year + month + "01T00:00-05:00", '%Y%m%dT%H:%M%z')
end_time = datetime.strptime(
    str(int(year) + 1) + month + "01T00:00-05:00", '%Y%m%dT%H:%M%z')

event_params = [
                   {
                       'event': 'Winter Storm Warning',
                       'severity': 'Severe'
                   },
                   {
                        'event': 'Tornado Warning'
                    }
    ]
fetcher = NWSFetcher(event_params)
ugc_check = UGCFetcher()

precip_parser = PrecipitationParser()

def fetch_new_events():
    new_events = pd.DataFrame()
    new_event_locations = pd.DataFrame()
    #event = features[0]['properties']
    crawling = True
    events_processed = 0
    while crawling:
        data_raw = fetcher.fetchWeatherData()
        time.sleep(1)
        if fetcher.finished:
            print("Wrapping up")
            crawling = False
        else:
            features = data_raw['features']
            for i, e_ in enumerate(features):
                if (events_processed+i) % 100 == 0:
                    print("Working on event",events_processed+i)
                event = e_['properties']
                event_data = {
                'nws_id': event['id'],
                'start':event['effective'],
                'end':event['ends'],
                'severity':event['severity'],
                'type':event['event']}

                precip_parser.dump()
                precip_parser.load_description(event['description'])
                try:
                    precip_data = precip_parser.process()
                    event_data = event_data | precip_data
                except e_:
                    print("Precipitation Parsing Error", e)
                    event_data = event_data | {'precipitation_error': 1}

                new_event_row = pd.DataFrame([event_data])
                new_events = pd.concat([new_events,new_event_row])
                new_events = new_events.reset_index(drop = True)

                for ugc in event['geocode']['UGC']:
                    ugc_check.load_unknowns(ugc)
                    new_loc = pd.DataFrame([{
                            'nws_id': event['id'],
                            'ugc': ugc
                        }])
                    new_event_locations = pd.concat([new_event_locations, new_loc])
                events_processed += 1
    salt = str(datetime.now().strftime('%Y%m%d_%H'))
    new_event_locations.reset_index(drop=True).to_csv('data/v2_' + salt + '_event_locations.csv')
    new_events.reset_index(drop=True).to_csv('data/v2_' + salt + '_events.csv')

def retro_process():
    new_events = pd.DataFrame()
    new_event_locations = pd.DataFrame()
    #event = features[0]['properties']
    events_processed = 0
    directory = 'data/raw/'
    salt = None
    for file in os.listdir(directory):
         filename = os.fsdecode(file)
         file_path = directory+filename
         if filename.endswith(".json"):
            if salt is None:
                salt = file_path[19:30]
            if salt != file_path[19:30]:
                print(salt)
                # new day data
                new_event_locations.reset_index(drop=True).to_csv('data/v2_' + salt + '_event_locations.csv')
                new_events.reset_index(drop=True).to_csv('data/v2_' + salt + '_events.csv')
                salt = file_path[19:30]
            data_raw = json.loads(open(file_path).read())
            features = data_raw['features']
            for i, e_ in enumerate(features):
                if (events_processed+i) % 100 == 0:
                    print("Working on event",events_processed+i)
                event = e_['properties']
                event_data = {
                'nws_id': event['id'],
                'start':event['effective'],
                'end':event['ends'],
                'severity':event['severity'],
                'type':event['event']}

                precip_parser.dump()
                precip_parser.load_description(event['description'])
                try:
                    precip_data = precip_parser.process()
                    event_data = event_data | precip_data
                except e_:
                    print("Precipitation Parsing Error", e)
                    event_data = event_data | {'precipitation_error': 1}

                new_event_row = pd.DataFrame([event_data])
                new_events = pd.concat([new_events,new_event_row])
                new_events = new_events.reset_index(drop = True)

                for ugc in event['geocode']['UGC']:
                    ugc_check.load_unknowns(ugc)
                    new_loc = pd.DataFrame([{
                            'nws_id': event['id'],
                            'ugc': ugc
                        }])
                    new_event_locations = pd.concat([new_event_locations, new_loc])
                events_processed += 1

#fetch_new_events()
retro_process()
print(f"*******\n\nFinished getting events!\nAnd it only took me {round(time.time() - process_start,2)} seconds.\nUpdating UGC now!\n\n*******")
#ugc_check.update_zones()
print(f"All done! Total time elapsed: {round(time.time() - process_start,2)} seconds")

"""
MOZ034
OKZ037
"""


"""
Event
Event_ID
Loc_ID
Alert_ID

Location
UGC
State
County
"""
