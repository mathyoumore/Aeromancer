import requests
import json
import re
import time
import csv
import markdowntable
from datetime import datetime
from typing import List, Set, Dict, Tuple, Optional

"""

TO DO LIST, FEATURE EDITION
* File Merger
    * Merged into Master Canvas file, but there's a lot of duplication.
    * Need to manually add to the master file as we go
    * Also a problem with ZIPs - the reference file was borked so a bunch are missing
    * I think we can reliably pick the longer set of zips and be good built
    it might be easier to redo them all again.
    I'm starting to think we're going to save ourselves a LOT of time by
    dropping description for a tokenizer precipitation thing

    * Use ID to merge CSV files
    * Be aware of changing columns
    * Mark merged files as Archival
    * Still save separate days, maybe

* Fold in Census data to have a BAN that says "X,###,000 Under Severe Weather Warning"
* Map top (eventually, all we care about) HS addresses and service regions
* Map top Pharma mfg addresses and ring a loud bell when someone gets hit hard
* Automate so this runs every 3 hours or so
* Fold in FEMA's API for large disasters
* Estimated impact based on temperature, anticipated percipitation quantity and type
* Something with data science that can let us know how volume will be affected before a storm hits

TO DO, RESEARCH

TO DO LIST
GeoCode SAME might have a leading zero to Zip. Is that true?
Cron job to run this every day, appending data
"""

"""
Start with 2 files
Go line by line in every csv
strip columns and have some master copy to compare them to
Decide if we've seen this by concating everything and jamming it into a known_events thing (strip newlines and white spaces first)
If we haven't seen it, process the description too
Jam into new master file

"""
# raise()

should_verify = False
# If behind the company VPN, you'll need to be bad and add verify=False here
# Definitely, extremely don't leave verify = false if you're using this for real
# Can't emphasize that enough. Seriously.

process_start = time.time()

now = datetime.now()  # current date and time
year = now.strftime("%Y")
month = '02'  # now.strftime("%m")
start_time = datetime.strptime(
    year + month + "01T00:00-05:00", '%Y%m%dT%H:%M%z')
end_time = datetime.strptime(
    str(int(year) + 1) + month + "01T00:00-05:00", '%Y%m%dT%H:%M%z')

county_cleaner = (r"(North[ews]?\w*\s|South[ews]?\w*\s|East(ern)?\s|"
                  r"West(ern)?\s|Central\s|Interior\s|Coastal\s|\s?Inland\s|"
                  r"\s?County\s?)")
county_badwords = (r"(Mountain(s)?|Foothills|Island|Ft|"
                   r"Highway|Canal|Cape|Lakeshore|Highlands|0 Feet|"
                   r"Virginia Blue Ridge|Alaska Range|[Yy]ampa [Rr]iver|"
                   r"lake region|range|Slopes|Zion national park)")
sentence_tokenizer = r'(\s*[^.!?]*[.!?]{1,3})'
event_tokenizer = (
    r"(?P<precipitation>sleet|snow|ice)\saccumulations\sof\s"
    r"(?P<accumulation_range_qualifier>up\sto\s)?"
    r"(?P<accumulation_range_lower>[1234567890]{1,2}|one|two|three|four|five|six|seven|eight|nine)\s"
    r"(?P<accumulation_denom_1>tenths?\s|quarters?\s)?(to\s)?(of\s)?"
    r"(?P<accumulation_range_upper>[1234567890]{1,2}\s|one\s|two\s|three\s|four\s|five\s|six\s|seven\s|eight\s|nine\s)?"
    r"(?P<accumulation_denom_2>tenths?\s|quarters?\s)?(of\s)?"
    r"(?P<accumulation_unit>inch|inches|an inch|a foot)")


url = 'https://api.weather.gov/alerts/?'
table = markdowntable.MarkdownTable(
    ['id',
     'state',
     'simple_county',
     'county',
     'zips',
     'population',
     'severity',
     'event',
     'start',
     'start_raw',
     'expiry',
     'expiry_raw'])

precipitation_table = markdowntable.MarkdownTable([
    'id',
    'precipitation',
    'accumulation_range_qualifier',
    'accumulation_range_lower',
    'accumulation_denom_1',
    'accumulation_range_upper',
    'accumulation_denom_2',
    'accumulation_unit'
])

crawling = True

failed_counties = []

total_affected = 0


def mapZipsAndCounties():
    zip_county_state = {}
    with open("data/zip_code_database.csv", "r", errors='ignore') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)

        for row in csvreader:
            zip = row[0]
            state = row[1]
            county = row[2].capitalize()
            lat = row[3]
            lon = row[4]
            pop = row[5]

            if state not in zip_county_state.keys():
                zip_county_state[state] = {
                    'state': state,
                    'counties': {
                        county: {
                            'zips': [zip],
                            'rough_lat': lat,
                            'rough_lon': lon,
                            'population': int(pop)
                        }
                    }
                }
            else:
                if county not in zip_county_state[state]['counties']:
                    zip_county_state[state]['counties'][county] = {
                        'zips': [zip],
                        'rough_lat': lat,
                        'rough_lon': lon,
                        'population': int(pop)
                    }
                else:
                    zip_county_state[state]['counties'][county]['zips'].append(
                        zip)
                    zip_county_state[state]['counties'][county]['population'] += int(
                        pop)
    return zip_county_state


class NWSFetcher():
    def __init__(self, event, severity):
        self.event = event
        self.severity = severity
        self.cursor = None
        self.finished = False

    def fetchWeatherData(self):
        params = {
            'start': start_time.isoformat(),
            'event': 'Winter Storm Warning',
            'severity': 'Severe'
        }
        if self.cursor is None:
            response = requests.get(url, params=params, verify=should_verify)
        else:
            print(f"Paging to {self.cursor}")
            response = requests.get(self.cursor, verify=should_verify)

        data_raw = json.loads(str(response.text))

        if 'pagination' in data_raw.keys():
            self.cursor = data_raw['pagination']['next']
        else:
            self.finished = True
        return data_raw


class WeatherEvent():
    def __init__(self, properties):
        self.properties = properties
        self.data = {}
        self.precipitation_data = []
        self.known_events = []
        self.should_be_added = True
        self.simpleStrip()
        if not self.isReplacementEvent():
            self.tokenizeDescription()
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

    def isReplacementEvent(self):
        if re.match(r'[Rr]eplac', str(self.data['description'])) is not None:
            self.setShouldNotBeAdded()
            return True
        return False

    def tokenizeDescription(self):
        matches = []
        simple_desc = re.sub("\n", ' ', str(self.data['description']))
        sentences = re.split(sentence_tokenizer, simple_desc)
        for s in sentences:
            match = re.search(event_tokenizer, s)
            if match is not None:
                matches.append(match)
        for m in matches:
            self.checkFailedEvents(m)
            self.precipitation_data.append(
                self.safePrecipitation(m) | {'id': self.data['id']})

    def safePrecipitation(self, match):
        safe_match = match.groupdict().copy()
        for k in ['accumulation_range_lower', 'accumulation_range_upper']:
            if k in safe_match and safe_match[k] is not None:
                match safe_match[k].strip():
                    case 'one':
                        safe_match[k] = 1.0
                    case 'two':
                        safe_match[k] = 2.0
                    case 'three':
                        safe_match[k] = 3.0
                    case 'four':
                        safe_match[k] = 4.0
                    case 'five':
                        safe_match[k] = 5.0
                    case 'six':
                        safe_match[k] = 6.0
                    case 'seven':
                        safe_match[k] = 7.0
                    case 'eight':
                        safe_match[k] = 8.0
                    case 'nine':
                        safe_match[k] = 9.0
                    case 'zero':
                        safe_match[k] = 0.0
        for k in ['accumulation_denom_1', 'accumulation_denom_2']:
            if k in safe_match and safe_match[k] is not None:
                match safe_match[k].strip():
                    case 'tenth':
                        safe_match[k] = 10
                    case 'tenths':
                        safe_match[k] = 10
                    case 'quarter':
                        safe_match[k] = 4
                    case 'quarters':
                        safe_match[k] = 4

        if safe_match['accumulation_unit'].strip() == 'an inch' or safe_match['accumulation_unit'].strip() == 'inches':
            safe_match['accumulation_unit'] = 'inch'

        if safe_match['accumulation_denom_1'] is not None:
            safe_match['accumulation_range_lower'] = float(
                safe_match['accumulation_range_lower']) / float(safe_match['accumulation_denom_1'])
        if safe_match['accumulation_denom_2'] is not None:
            safe_match['accumulation_range_upper'] = float(
                safe_match['accumulation_range_upper']) / float(safe_match['accumulation_denom_2'])

        if safe_match['accumulation_range_qualifier'] is not None:
            match safe_match['accumulation_range_qualifier'].strip():
                case 'up to':
                    safe_match['accumulation_range_qualifier'] = 'lte'
            # case 'over':
            #     safe_match['accumulation_range_qualifier'] = 'gte'

        for k, v in safe_match.items():
            if safe_match[k] is None:
                safe_match[k] = ''
            safe_match[k] = str(safe_match[k])
        return safe_match

    def checkFailedEvents(self, matches):
        match_dict = matches.groupdict()
        if match_dict['precipitation'] is None or match_dict['accumulation_range_lower'] is None or match_dict['accumulation_unit'] is None:
            print(f"I'm not sure what to do with this:\n\t{matches.string}")

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
                county_badwords, v)
            if badwords is not None:
                if badwords.string not in ["Rock Island", "Highlands"]:
                    # Who names a county Highlands? Not "Highland"?
                    self.setShouldNotBeAdded()
            else:
                simple_county = re.sub(county_cleaner, '', v)
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


severe_winter_warning_fetcher = NWSFetcher(
    'Winter Weather Warning', 'Severe')
zip_county_state = mapZipsAndCounties()


while crawling:

    print(f"---\nWorking backwards, currently at: {end_time}\n---")

    data_raw = severe_winter_warning_fetcher.fetchWeatherData()
    features = data_raw['features']

    if severe_winter_warning_fetcher.finished:
        crawling = False
    else:
        for i, f in enumerate(features):
            should_add = True
            properties = f['properties']
            weather_event = WeatherEvent(properties)

            if weather_event.should_be_added:
                for state_county in weather_event.affectedAreas():
                    state = state_county['state']
                    simple_county = state_county['simple_county']
                    if simple_county in zip_county_state[state]['counties'].keys():
                        zips = zip_county_state[state]['counties'][simple_county]['zips']
                        population = zip_county_state[state]['counties'][simple_county]['population']
                        total_affected += population
                    else:
                        zips = []
                        population = 0
                        failed_counties.append(simple_county)

                    simple_event = weather_event.data.copy()
                    del simple_event['state_counties']
                    del simple_event['description']
                    table.addRowRefactored(simple_event
                                           | {'state': state,
                                              'county': state_county['full_county'],
                                              'simple_county': simple_county,
                                              'zips': zips,
                                              'population': population})
                    for precipitation_datum in weather_event.precipitation_data:
                        precipitation_table.addRowRefactored(
                            precipitation_datum)

    time.sleep(1)
print(f"Found {table.rows} events.\nCreating files.")
if len(failed_counties) > 0:
    print(
        f"Could find these counties: {failed_counties}\nSorry, hoss. Maybe update the badwords list.")
    total_affected_str = "{:,}".format(total_affected)
    print(f"Confirmed affected: {total_affected_str}")

"""
if population_of_affected > 100,000:
    print("Sound the alarms, y'all, something's happening")
"""

"""
if population_of_affected > 1,000,0000:
    print("BEEFCAKE DETECTED!!! Assemble a war room immediately!!!")
"""

salt = str(datetime.now().strftime('%Y%m%d_%H'))
table.makeTable('data/AeroHistorian_token' + salt + '.csv')
precipitation_table.makeTable(
    'data/AeroHistorian_token_precip' + salt + '.csv')

should_log = False

if should_log:
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
