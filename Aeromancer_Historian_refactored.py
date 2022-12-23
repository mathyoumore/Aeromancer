import requests
import json
import re
import time
import csv
import markdowntable
import pandas as pd
from datetime import datetime
from typing import List, Set, Dict, Tuple, Optional

"""

TO DO LIST, FEATURE EDITION
Panda-fy to get rid of markdowntable
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
GeoCode SAME might have a leading zero to Zip. Is that true?
Cron job to run this every day, appending data
Visualization of data model

"""

should_verify = False
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

url = 'https://api.weather.gov/alerts/?'
table = markdowntable.MarkdownTable(
    ['id',
     'state',
     'county',
     'zip_codes',
     'population',
     'severity',
     'event',
     'start',
     'start_raw',
     'expiry',
     'expiry_raw',
     'description'])

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
event_params_index = 0

crawling = True

failed_counties = []

total_affected = 0


def generateCountyZip():
    county_zip = {}
    zip_code_df = pd.read_csv("data/zip_code_database.csv")
    for _, row in zip_code_df.iterrows():
        zip = row['zip']
        state = row['state']
        county = row['clean county']
        lat = row['latitude']
        lon = row['longitude']
        pop = row['irs_estimated_population']
        nws_id = row['ugc']
        if nws_id not in county_zip.keys():
            county_zip[nws_id] = {
                'state': state,
                'county': county,
                'pop': pop,
                'zip_codes': [zip]
            }
        else:
            county_zip[nws_id]['pop'] += pop
            county_zip[nws_id]['zip_codes'].append(zip)
    return county_zip


def mapZipsAndCounties():
    zip_county_state = {}

    zip_code_df = pd.read_csv("data/zip_code_database.csv")

    for _, row in zip_code_df.iterrows():
        zip = row['zip']
        state = row['state']
        county = row['clean county']
        lat = row['latitude']
        lon = row['longitude']
        pop = row['irs_estimated_population']
        nws_id = row['ugc']

        if state not in zip_county_state.keys():
            zip_county_state[state] = {
                'state': state,
                'counties': {
                    county: {
                        'zip_codes': [zip],
                        'rough_lat': lat,
                        'rough_lon': lon,
                        'population': int(pop),
                        'nws_id': nws_id
                    }
                }
            }
        else:
            if county not in zip_county_state[state]['counties']:
                zip_county_state[state]['counties'][county] = {
                    'zip_codes': [str(zip)],
                    'rough_lat': lat,
                    'rough_lon': lon,
                    'population': int(pop),
                    'nws_id': nws_id
                }
            else:
                zip_county_state[state]['counties'][county]['zip_codes'].append(
                    str(zip))
                zip_county_state[state]['counties'][county]['population'] += int(
                    pop)
    return zip_county_state


def closingRemarks():
    print("\n\n*******\nComplete\n*******\n\n ")
    print(f"Found {table.rows} events.\nCreating files.")
    if len(failed_counties) > 0:
        print(
            f"Could not find these counties: {failed_counties}\nSorry, hoss. Maybe update the badwords list.")
        total_affected_str = "{:,}".format(total_affected)
        print(f"Confirmed affected: {total_affected_str}")

    salt = str(datetime.now().strftime('%Y%m%d_%H'))
    table.makeTable('data/AeroHistorian_test_r' + salt + '.csv')
    # id,date_created,event_count,time_to_complete,failed_counties

    with open("data/log.csv", "a+", errors='ignore') as logfile:
        csvreader = csv.reader(logfile)
        id = -1
        for row in open("data/log.csv"):
            id += 1
        log = ", ".join([str(id), str(datetime.now()), str(table.rows), str(round(
            time.time() - process_start, 2)), str(len(failed_counties))])
        log += "\n"
        logfile.write(log)

    print(f"{round(time.time() - process_start,2)} seconds elapsed")
    print("Thank you for using Aeromancer. Have a nice day!")
    print("\n\n\n*******************************************\nRemember to update the master file!")


class NWSFetcher():
    def __init__(self, event_params):
        self.event_params = event_params
        self.cursor = None
        self.finished = False
        self.event_params = event_params
        self.event_params_index = 0

    def fetchWeatherData(self):
        params = self.event_params[self.event_params_index]
        if self.cursor is None:
            response = requests.get(url, params=params, verify=should_verify)
        else:
            response = requests.get(self.cursor, verify=should_verify)

        data_raw = json.loads(str(response.text))

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


class WeatherEvent():
    def __init__(self, properties):
        self.properties = properties
        self.data = {}
        self.known_events = []
        self.should_be_added = True
        self.simpleStrip()
        if not self.isIgnorableEvent():
            self.countyClean()
            self.dateClean()
        else:
            print("Replacement event skipped")

    def setShouldNotBeAdded(self):
        self.should_be_added = False

    def checkOrAddKnownEvent(self, id, simple_county, state):
        event_pseudohash = id + simple_county + state
        if event_pseudohash in self.known_events:
            self.should_be_added = False
        else:
            self.known_events.append(event_pseudohash)

    def simpleStrip(self):
        for k in ['id', 'severity', 'event', 'description']:
            self.data[k] = self.properties[k]

    def isIgnorableEvent(self):
        if self.data['description'] == 'The Tornado Warning has been cancelled and is no longer in effect.' or \
                re.match(r'The storm which prompted the warning', str(self.data['description'])) is not None or\
                re.match(r'[Rr]eplac', str(self.data['description'])) is not None:
            self.setShouldNotBeAdded()
            return True
        return False

    def countyClean(self):
        if len(self.properties['areaDesc'].split(';')) != len(self.properties['affectedZones']):
            raise("FATAL ERROR: State and City assignment is built on the assertion that areaDesc and affectedZones are equal length")

        state_counties = []  # type: List[Dict]
        areaDesc = self.properties['areaDesc']
        unique_geocode = self.properties['geocode']['UGC']
        for i, v in enumerate(areaDesc.split('; ')):
            state_counties.append({})
            state_counties[i]['full_county'] = v
            badwords = re.search(
                county_badwords, v, re.IGNORECASE)
            if badwords is not None:
                if badwords.string not in ["Rock Island", "Highlands"]:
                    # Who names a county Highlands? Not "Highland"?
                    self.setShouldNotBeAdded()
            elif re.search(v, 'dekalb', re.IGNORECASE) is not None:
                v = 'De Kalb'
            if badwords is None or v == 'De Kalb':
                simple_county = re.sub(county_cleaner, '', v, re.IGNORECASE)
                state_counties[i]['simple_county'] = simple_county.capitalize()
                state = unique_geocode[i][0:2]
                state_counties[i]['state'] = state
                self.checkOrAddKnownEvent(
                    self.data['id'], simple_county, state)

        self.data['state_counties'] = state_counties

    def dateClean(self):
        self.data['start_raw'] = datetime.fromisoformat(
            self.properties['effective'])
        self.data['start'] = self.data['start_raw'].strftime('%Y/%m/%d %H:%M')

        self.data['expiry_raw'] = datetime.fromisoformat(
            self.properties['expires'])
        self.data['expiry'] = self.data['expiry_raw'].strftime(
            '%Y/%m/%d %H:%M')

    def affectedAreas(self):
        return self.data['state_counties']

    def affectedUGS(self):
        return self.properties['geocode']['UGC']


fetcher = NWSFetcher(event_params)
zip_county_state = mapZipsAndCounties()
aaa = generateCountyZip()

# I made zip_county_state less stupid. Now we can just search on nws_id in the unique_geocode[UGS] field and pull everything from WeatherEvent
# you'll need to start using that now


while crawling:

    data_raw = fetcher.fetchWeatherData()
    features = data_raw['features']

    if fetcher.finished:
        crawling = False
    else:
        for i, f in enumerate(features):
            should_add = True
            properties = f['properties']
            weather_event = WeatherEvent(properties)

            if weather_event.should_be_added:

                for state_county in weather_event.affectedAreas():
                    # ugs = weather_event.affectedUGS()[0]
                    # if ugs in aaa.keys():
                    #     ugs = aaa[weather_event.affectedUGS()[0]]
                    #     ugs_state = ugs['state']
                    #     ugs_zips = ugs['zip_codes']
                    #     ugs_county = ugs['county']
                    #     ugs_pop = ugs['pop']
                    try:
                        state = state_county['state']
                    except:
                        breakpoint()
                    simple_county = state_county['simple_county']
                    if simple_county in zip_county_state[state]['counties'].keys():
                        zips = zip_county_state[state]['counties'][simple_county]['zip_codes']
                        population = zip_county_state[state]['counties'][simple_county]['population']
                        total_affected += population
                    else:
                        zips = []
                        population = 0
                        failed_counties.append(simple_county)
                    simple_event = weather_event.data.copy()
                    del simple_event['state_counties']
                    table.addRowRefactored(simple_event
                                           | {'state': state,
                                              'county': state_county['full_county'],
                                              'simple_county': simple_county,
                                              'zip_codes': zips,
                                              'population': population})
                """
                for zone in weather_event.affectedUGS():
                    if zone in aaa.keys():
                        ugs = aaa[zone]
                        simple_event = weather_event.data.copy()
                        del simple_event['state_counties']
                        table.addRowRefactored(simple_event
                                               | {'state': ugs['state'],
                                                  'county': ugs['county'],
                                                  'zip_codes': ugs['zip_codes'],
                                                  'population': ugs['pop']})

                    else:
                        failed_counties.append(zone)
                """

    time.sleep(1)

closingRemarks()
