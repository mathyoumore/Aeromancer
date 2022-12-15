import requests
import json
import pprint
import re
import time
import markdowntable

pp = pprint.PrettyPrinter()

"""
For the purposes of this tool, I'm skipping Alaska.
Alaskan counties do not match up neatly to weather zones
AK also isn't the most populace, so juice != squeeze
Sorry, Alaska.
"""

clinics = {
    "Mayo": {
        "state": "MN",
        "counties": [
            "Olmsted"
        ]
    },
    "Baylor Scott And White": {
        "state": "TX",
        "counties": [
            "Dallas"
        ]
    },
    "Northwestern Medicine": {
        "state": "IL",
        "counties": [
            "Cook"
        ]
    },
    "Mercy Health of Cincinnati": {
        "state": "OH",
        "counties": [
            "Hamilton"
        ]
    },
    "Allina": {
        "state": "MN",
        "counties": [
            "Hannepin"
        ]
    },
    "Scripps": {
        "state": "CA",
        "counties": [
            "San Diego"
        ]
    },
    "Prisma": {
        "state": "SC",
        "counties": [
            "San Diego"
        ]
    },

    "Sanford SD": {
        "state": "SD",
        "counties": [
            "Minnehaha",
            "Pennington"
        ]
    },
    "Sanford ND": {
        "state": "ND",
        "counties": [
            "Cass",
            "Burleigh",
            "Grand Forks"
        ]
    },
    "Lexington": {
        "state": "SC",
        "counties": [
            "Lexington"
        ]
    }
}

state_counties = {}

for clinic, v in clinics.items():
    s = v['state']
    for county in v['counties']:
        if s not in state_counties.keys():
            state_counties[s] = [county]
        elif county not in state_counties[s]:
            state_counties[s].append(county)

o = open("out.md", "w")
url = 'https://api.weather.gov/alerts/active?'
md_table = markdowntable.MarkdownTable(
    ['Area', 'Severity', 'Event', 'Description', 'Expiration'])

print(f"I will check {len(state_counties.keys())} states")
for clinic, v in clinics.items():

    print(f"Checking {v['state']}")

    params = {'area': v['state']}
    # If behind the company VPN, you'll need to be bad and add verify=False here
    # Definitely, extremely don't leave verify = false if you're using this for real
    # Can't emphasize that enough. Seriously.
    response = requests.get(url, params=params, verify=False)
    data_raw = json.loads(response.text)
    features = data_raw['features']

    for f in features:
        table = ''
        found = False
        properties = f['properties']
        for c in state_counties[v['state']]:
            found = found or properties['areaDesc'].find(c) != -1
        if found:
            area = properties['areaDesc']
            expiry = properties['expires']
            severity = properties['severity']
            description = re.sub("\n", ' ', properties['description'])
            description = description.replace(
                '*', '<br>*').replace('<br>*', '* ', 1)

            event = properties['event']

            md_table.addRow([str(area), severity, event, description, expiry])

    if len(features) > 0 and md_table.rows > 0:
        o.write("## " + clinic
                + " (" + str(v['state']) + ")" + "\n" + md_table.getTable() + "\n")
    else:
        o.write("## " + clinic + " (" + str(v['state']) + ")" + "\n"
                + "No significant weather events to report" + "\n")
    md_table.purgeTable()
    o.write("\n")
    time.sleep(1)

o.close()
