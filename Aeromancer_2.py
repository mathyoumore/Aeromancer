"""
README
Author: Matthew Mohr (matt@mathyoumore.com)

This generates reports via the NWS API (https://www.weather.gov/documentation/services-web-api)
The script is broadly autonomous and should be allowed to run at least once per day. 
The NWS API is limited in what it can return, roughly 2-4 weeks before the request date based on record count, 
so this has to be run regularly (and maybe more frequently during high-impact seasons).

NWS uses something called Unique Geographical Codes (UGC, https://www.weather.gov/pimar/PubZone) to send out alerts. 
This coding system ties pretty closely to county. 
There are exceptions - particularly around mountains and valleys because mountains and valleys will have different weather from sea-level elevations

UGCs can be updated by querying
* https://api.weather.gov/zones/public
* https://api.weather.gov/zones/county
Note that the results of these queries will return nearly 10k records, so build it once and be done with it

NWS events are indexed to nws_id:
* Event (index: nws_id) - 1:M -> UGC

The current table structure, therefore:
* Events (PK: nws_id) - 1:M -> Event_Locations (FK: nws_id, ugc_id)
* Event_Locations (PK: nws_is, ugc_id) - 1:1 -> Locations(PK: UGC)

This, with a custom-built file that marries counties to zip codes, can be used to map NWS Events to zip codes and counties
by joining on the name and state of a UGC. It mostly works, with exceptions for the mountains and valleys as mentioned above. 
"""

"""
TO DO LIST, FEATURE EDITION
Log that actually works

* Automate so this runs every 3 hours or so
* Fold in FEMA's API for large disasters

* Something with data science that can let us know how volume will be affected before a storm hits
"""

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

should_verify = True
# Sometimes VPNs gets mad if you try to make HTTP requests outside of a browser
# If you're behind a VPN that does this, switch this to False to run and then switch it back to True

event_params = [
                   {
                       'event': "Winter Storm Warning,Ice Storm Warning,Blizzard Warning,Tornado Warning",
                       'severity': "Severe,Extreme"
                   }
    ]

fetcher = NWSFetcher(event_params)

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
                    new_loc = pd.DataFrame([{
                            'nws_id': event['id'],
                            'ugc': ugc
                        }])
                    new_event_locations = pd.concat([new_event_locations, new_loc])
                events_processed += 1


def retro_process_2():
    i = 0
    new_events = pd.DataFrame()
    new_event_locations = pd.DataFrame()
    events_processed = 0
    directory = 'data/raw/'
    salt = None
    for file in os.listdir(directory):
         filename = os.fsdecode(file)
         file_path = directory+filename
         if filename.endswith(".csv"):
            if salt is None:
                salt = file_path[12:-7]
            if salt != file_path[12:-7]:
                print(salt)
                # new day data
                new_event_locations.reset_index(drop=True).to_csv('data/v2_' + salt + '_event_locations.csv')
                new_events.reset_index(drop=True).to_csv('data/v2_' + salt + '_events.csv')
                salt = file_path[12:-7]
            data_raw = pd.read_csv(file_path,on_bad_lines='warn')
            for event in data_raw.itertuples():
                i+=1
                if (events_processed+i) % 100 == 0:
                    print("Working on event",events_processed+i)
                event_data = {
                'nws_id': event.id,
                'start':event.start_raw,
                'end':event.expiry_raw,
                'severity':event.severity,
                'type':event.event}

                precip_parser.dump()
                precip_parser.load_description(event.description)
                try:
                    precip_data = precip_parser.process()
                    event_data = event_data | precip_data
                except:
                    print("Precipitation Parsing Error", e)
                    event_data = event_data | {'precipitation_error': 1}

                new_event_row = pd.DataFrame([event_data])
                new_events = pd.concat([new_events,new_event_row])
                new_events = new_events.reset_index(drop = True)
                if not(pd.isna(event.zip_codes)):
                    try:
                        for zip in event.zip_codes.split(','):
                            new_loc = pd.DataFrame([{
                                    'nws_id': event.id,
                                    'zip': zip
                                }])
                            new_event_locations = pd.concat([new_event_locations, new_loc])
                    except e_:
                        breakpoint()
                events_processed += 1


process_start = time.time()
#fetch_new_events()

# I've included retro_process and SimpleFetch.py as a backup in case Aeromancer stops working because of API changes or whatever
# Simply run SimpleFetch.py every day until you have a stable version going and then use retro_process to rerun the new process on the old files
#retro_process()

retro_process_2()
print(f"*******\n\nFinished getting events!\nAnd it only took me {round(time.time() - process_start,2)} seconds.\nUpdating UGC now!\n\n*******")
print(f"All done! Total time elapsed: {round(time.time() - process_start,2)} seconds")

