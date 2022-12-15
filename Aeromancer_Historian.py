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
Data stripping sucks, make it into a methodized process
Append data to a file instead of rewriting it
Anything we can do to make should_add a oneline?
start_date and end_date aren't really useful anymore, are they?
Anticipated Percipitation Type and quantity (maybe in a different file?)
"""

month = '01'

should_verify = False
# If behind the company VPN, you'll need to be bad and add verify=False here
# Definitely, extremely don't leave verify = false if you're using this for real
# Can't emphasize that enough. Seriously.

process_start = time.time()

now = datetime.now()  # current date and time
year = now.strftime("%Y")
start_time = datetime.strptime(
    year + month + "01T00:00-05:00", '%Y%m%dT%H:%M%z')
end_time = datetime.strptime(
    str(int(year) + 1) + month + "01T00:00-05:00", '%Y%m%dT%H:%M%z')

county_cleaner = "(North[ews]\w*\s|South[ews]\w*\s|Eastern\s|Western\s|Central\s|Interior\s|Coastal\s|\s?Inland\s|\s?County\s?)"
county_badwords = "(Mountains|Foothills|Island|Ft|Highway|Canal|Cape|Lakeshore|Highlands|0 Feet|Virginia Blue Ridge|Denali|[Rr]ange)"

url = 'https://api.weather.gov/alerts/?'
table = markdowntable.MarkdownTable(
    ['id', 'State', 'County', 'Full County', 'Zips', 'Population', 'Severity', 'Event', 'Effective', 'Effective ISO', 'Expiration', 'Expiration ISO', 'Description'])

crawling = True

cccc = 0

known_events = []

failed_counties = []

# Start zip_county_state work ##############################################
zip_county_state = {}
with open("data/zip_code_database.csv", "r", errors='ignore') as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader)

    for row in csvreader:
        zip = row[0]
        state = row[1]
        county = row[2]
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
                zip_county_state[state]['counties'][county]['zips'].append(zip)
                zip_county_state[state]['counties'][county]['population'] += int(
                    pop)

# End zip_county_state work

cursor = "none"
while crawling:

    print(f"---\nWorking backwards, currently at: {end_time}\n---")
    params = {
        'start': start_time.isoformat(),
        'event': 'Winter Storm Warning',
        'severity': 'Severe'
    }
    if cursor == "none":
        response = requests.get(url, params=params, verify=should_verify)
    else:
        response = requests.get(cursor, verify=should_verify)

    data_raw = json.loads(response.text)
    features = data_raw['features']
    if 'pagination' not in data_raw:
        print(
            "*******************************\nAin't nothing left\n*******************************")
        crawling = False
    else:
        cursor = data_raw['pagination']['next']
        for i, f in enumerate(features):
            should_add = True
            properties = f['properties']
            id = str(properties['id'])

            zones = properties['affectedZones']

            expiry = properties['expires']

            state_counties = []  # type: List[Dict]
            areaDesc = properties['areaDesc']
            unique_geocode = properties['geocode']['UGC']
            for i, v in enumerate(areaDesc.split('; ')):
                state_counties.append({})
                state_counties[i]['full_county'] = v
                simple_county = re.sub(county_cleaner, '', v)
                badwords = re.search(
                    county_badwords, simple_county,re.IGNORECASE)

                if badwords is not None:
                    if badwords.string != "Rock Island" or badwords.string != "Highlands":
                        should_add = False

                state_counties[i]['simple county'] = simple_county
                state = unique_geocode[i][0:2]
                state_counties[i]['state'] = state

                event_pseudohash = id + simple_county + state
                if event_pseudohash in known_events:
                    should_add = False
                else:
                    known_events.append(event_pseudohash)

            start_raw = datetime.fromisoformat(
                properties['effective'])
            start = start_raw.strftime('%Y/%m/%d %H:%M')

            expiry_raw = datetime.fromisoformat(properties['expires'])
            expiry = expiry_raw.strftime('%Y/%m/%d %H:%M')

            description = str(properties['description'])
            if 'replaced' in description:
                should_add = False

            severity = properties['severity']
            event = properties['event']

            if start_raw < end_time:
                end_time = start_raw

            # This breaks the program bleep blorp
            """
            AreaDesc is a set of counties which can be split
            It just so happens to have the same number of fields as affectedZones
            Split areaDesc and then send to Add row as
                state = affectedZone[i], county = areaDesc[i]
            """

            if len(properties['areaDesc'].split(';')) != len(properties['affectedZones']):
                print(f"{properties['areaDesc'].split(';')}")
                print(f"{properties['affectedZones']}")
                raise("PANIC!")

            if should_add is True:
                for state_county in state_counties:
                    state = state_county['state']
                    simple_county = state_county['simple county']
                    if simple_county in zip_county_state[state]['counties'].keys():
                        zips = zip_county_state[state]['counties'][simple_county]['zips']
                        population = zip_county_state[state]['counties'][simple_county]['population']
                    else:
                        zips = []
                        population = 0
                        failed_counties.append(simple_county)
                    table.addRow([id,
                                  state,
                                  simple_county,
                                  state_county['full_county'],
                                  zips,
                                  population,
                                  severity,
                                  event,
                                  start,
                                  str(start_raw),
                                  expiry,
                                  str(expiry_raw), description])

    time.sleep(1)
print(f"Found {table.rows} events.\nCreating files.")
if len(failed_counties) > 0:
    print(
        f"Could find these counties: {failed_counties}\nSorry, hoss. Maybe update the badwords list.")
salt = str(datetime.now().strftime('%Y%m%d_%H'))
table.makeTable('data/AeroHistorian3' + salt + '.csv')
table.makeTableMd('data/AeroHistorian3' + salt + '.md')
print(f"{round(time.time() - process_start,2)} seconds elapsed")
print("Thank you for using Aeromancer. Have a nice day!")
