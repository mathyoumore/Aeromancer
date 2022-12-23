import requests
import json
import re
import time
import csv
import markdowntable
import pandas as pd
from datetime import datetime
from typing import List, Set, Dict, Tuple, Optional

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

sentence_tokenizer = r'(\s*[^.!?]*[.!?]{1,3})'
event_tokenizer = (
    r"(?P<precipitation>sleet|snow|ice)(fall)?\saccumulations\s?((of|between)\s)?"
    r"(?P<accumulation_range_qualifier>(up to|a coating)\s)?(\sup\s)?"
    r"(?P<accumulation_range_lower>[1234567890]{1,3}|one|two|three|four|five|six|seven|eight|nine)?\s?"
    r"(?P<accumulation_denom_1>tenths?\s|quarters?\s)?(to|of|or|and)?\s?"
    r"(?P<accumulation_range_upper>[1234567890]{1,3}\s|one\s|two\s|three\s|four\s|five\s|six\s|seven\s|eight\s|nine\s)?"
    r"(?P<accumulation_denom_2>tenths?\s|quarters?\s)?(of\s)?"
    r"(?P<accumulation_unit>inch|inches|an inch|a foot)?"
)

badwords = (
            r"(Mountain(s)?|Foothills|Island|Ft|"
            r"Highway|Canal|Cape|Lakeshore|Highlands|0 Feet|"
            r"Virginia Blue Ridge|Alaska Range|[Yy]ampa [Rr]iver|"
            r"lake region|range|Slopes|Zion national park|denali|"
            r"Deltana and tanana|valley|basin)"
            )

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
        breakpoint()
        new_ugc_df = pd.DataFrame()
        county_zones = []
        """
        with open('data/raw/zones_to_check.txt') as f:
            print('If you are not testing, you should not be reading this.')
            raw_zones = f.read().split(',')
            self.zones_to_check = []
            for z in raw_zones:
                try:
                    ugc_match = re.search(r"(?P<ugc>[A-Z]{3}\d{2,3})",z).groups()
                    self.zones_to_check.append(ugc_match[0])
                except:
                    print("I don't know what to do with this:",z)
            new_ugc_df = pd.read_csv('pre_master_ugc.csv')
            print("Old length:",len(self.zones_to_check))
            self.zones_to_check = set(self.zones_to_check) - set(new_ugc_df['ugc'])
            print("New length:",len(self.zones_to_check))
        """
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

    """
    Iterate through each region zone
    If it exists in the known database, skip it
    If it isn't a good code, skip it
    Otherwise store it
    """

"""
UGC identifier for a NWS forecast zone or county.
The first two letters will correspond to either a state code or marine area code
(see #/components/schemas/StateTerritoryCode and #/components/schemas/MarineAreaCode for lists of valid letter combinations).
The third letter will be Z for public/fire zone or C for county.
"""

def processPrecipitation(raw):
    matches = []
    simple_desc = re.sub("\n", ' ', str(raw))
    sentences = re.split(sentence_tokenizer, simple_desc)
    for s in sentences:
        match = re.search(event_tokenizer, s, flags=re.IGNORECASE)
        if match is not None:
            matches.append(match)
    return matches

def accumulation_to_num(raw_):
    result = -1
    raw = raw_.strip()
    if re.search('\d',raw) is not None:
        result = raw
    else:
        match raw:
            case 'one':
                result = 1.0
            case 'two':
                result = 2.0
            case 'three':
                result = 3.0
            case 'four':
                result = 4.0
            case 'five':
                result = 5.0
            case 'six':
                result = 6.0
            case 'seven':
                result = 7.0
            case 'eight':
                result = 8.0
            case 'nine':
                result = 9.0
    return result

def accumluation_denominator(raw):
    if raw is None:
        return 1
    return {'tenth': 0.1, 'tenths': 0.1,
     'quarter': 0.25, 'quarters': 0.25,
     'fifth': 0.2, 'fifths': 0.2}[raw.strip()]

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

county_cleaner = (r"(North[ews]?\w*\s|South[ews]?\w*\s|East(ern)?\s|"
                  r"West(ern)?\s|Central\s|Interior\s|Coastal\s|\s?Inland\s|"
                  r"\s?County\s?|Upper\s|Lower\s)")
county_badwords = (r"(Mountain(s)?|Foothills|Island|Ft|"
                   r"Highway|Canal|Cape|Lakeshore|Highlands|0 Feet|"
                   r"Virginia Blue Ridge|Alaska Range|[Yy]ampa [Rr]iver|"
                   r"lake region|range|Slopes|Zion national park|denali|"
                   "Deltana and tanana|valley|basin)")

#zips_df = pd.read_csv('data/zip_code_database.csv')

event_params = [
                   {
                       'event': 'Winter Storm Warning',
                       'severity': 'Severe'
                   },
                   {
                        'event': 'Tornado Warning'
                    },
                   {
                        'event': 'Winter Weather Advisory'
                    },
    ]
fetcher = NWSFetcher(event_params)
ugc_check = UGCFetcher()

def fetch_new_events():
    new_events = pd.DataFrame()
    new_event_locations = pd.DataFrame()
    #event = features[0]['properties']
    crawling = True

    while crawling:
        data_raw = fetcher.fetchWeatherData()
        time.sleep(1)
        if fetcher.finished:
            print("Wrapping up")
            crawling = False
        else:
            features = data_raw['features']
            for i, e_ in enumerate(features):
                if i % 100 == 0:
                    print("Working on event number",i)
                event = e_['properties']
                event_data = {
                'nws_id': event['id'],
                'start':event['effective'],
                'end':event['ends'],
                'severity':event['severity'],
                'type':event['event']}

                precipitation = processPrecipitation(event['description'])
                if precipitation == []  :
                    event_data['precipitation_error'] = 'No Precipitation'
                for detail in precipitation:
                    max_precip, min_precip = None, None
                    d = detail.groupdict()
                    type_precip = d['precipitation'].lower()
                    if d['accumulation_range_lower'] is not None:
                        if d['accumulation_range_qualifier'] is not None:
                            if d['accumulation_range_lower'] == 'one':
                                max_precip = 1 * accumluation_denominator(d['accumulation_denom_1'])
                            else:
                                try:
                                    max_precip = accumulation_to_num(d['accumulation_range_lower']) * accumluation_denominator(d['accumulation_denom_1'])
                                except:
                                    breakpoint()
                            min_precip = None
                        else:
                            try:
                                min_precip = accumulation_to_num(d['accumulation_range_lower']) * accumluation_denominator(d['accumulation_denom_1'])
                                max_precip = accumulation_to_num(d['accumulation_range_upper']) * accumluation_denominator(d['accumulation_denom_2'])
                                event_data[type_precip + '_min'] = min_precip
                                event_data[type_precip + '_max'] = max_precip
                            except:
                                event_data['precipitation_error'] = 'Invalid Range'

                new_event_row = pd.DataFrame([event_data])
                new_events = pd.concat([new_events,new_event_row])
                new_events = new_events.reset_index(drop = True)

                salt = str(datetime.now().strftime('%Y%m%d_%H'))
                for ugc in event['geocode']['UGC']:
                    ugc_check.load_unknowns(ugc)
                    new_loc = pd.DataFrame([{
                            'nws_id': event['id'],
                            'ugc': ugc
                        }])
                    new_event_locations = pd.concat([new_event_locations, new_loc])
                    new_event_locations.to_csv('data/v2_' + salt + '_event_locations.csv')
                new_events.to_csv('data/v2_' + salt + '_events.csv')


    print("\n\n\n\n")

fetch_new_events()
ugc_check.update_zones()

breakpoint()

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
