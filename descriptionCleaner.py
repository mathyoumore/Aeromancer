import re
import requests
import json
from datetime import datetime

sentence_tokenizer = r'(\s*[^.!?]*[.!?]{1,3})'
event_tokenizer = r'(?P<precipitation>sleet|snow|ice)\saccumulations\sof\s(?P<accumulation_range_qualifier>up\sto\s)?(?P<accumulation_range_lower>[1234567890]{1,2}|one|two|three|four|five|six|seven|eight|nine)\s(?P<accumulation_denom_1>tenths?\s|quarters?\s)?(to\s)?(of\s)?(?P<accumulation_range_upper>[1234567890]{1,2}\s|one\s|two\s|three\s|four\s|five\s|six\s|seven\s|eight\s|nine\s)?(?P<accumulation_denom_2>tenths?\s|quarters?\s)?(of\s)?(?P<accumulation_unit>inch|inches|an inch|a foot)'

#raise("Use this to deconstruct existing CSV logs")

now = datetime.now()  # current date and time
year = now.strftime("%Y")
month = '2'
start_time = datetime.strptime(
    year + month + "01T00:00-05:00", '%Y%m%dT%H:%M%z')

url = 'https://api.weather.gov/alerts/?'


def processPrecipitation(raw):
    matches = []
    simple_desc = re.sub("\n", ' ', str(raw))
    sentences = re.split(sentence_tokenizer, simple_desc)
    for s in sentences:
        match = re.search(event_tokenizer, s)
        if match is not None:
            matches.append(match)
    return matches


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
            response = requests.get(url, params=params, verify=False)
        else:
            response = requests.get(self.cursor, verify=False)

        data_raw = json.loads(str(response.text))

        if 'pagination' in data_raw.keys():
            self.cursor = data_raw['pagination']['next']
        else:
            self.finished = True
        return data_raw


severe_winter_warning_fetcher = NWSFetcher(
    'Winter Weather Warning', 'Severe')

crawling = True
while crawling:

    data_raw = severe_winter_warning_fetcher.fetchWeatherData()
    features = data_raw['features']

    matches = []
    if severe_winter_warning_fetcher.finished:
        crawling = False

    for feature in features:
        raw_desc = feature['properties']['description']
        matches = processPrecipitation(raw_desc)
        for detail in matches:
            d = detail.groupdict()
            s = ''
            s = d['precipitation'].capitalize() + ", "
            if d['accumulation_range_qualifier'] is not None:
                s += "up to "
            s += d['accumulation_range_lower'] + ' '
            if d['accumulation_denom_1'] is not None:
                s += d['accumulation_denom_1'] + ' '
            if d['accumulation_range_upper'] is not None:
                s += 'to ' + d['accumulation_range_upper']
            if d['accumulation_denom_2'] is not None:
                s += d['accumulation_denom_2'] + ' '
            s += d['accumulation_unit']
            if d['accumulation_range_upper'] is not None or d['accumulation_range_qualifier'] is not None:
                s += 'es'
            print(s)
