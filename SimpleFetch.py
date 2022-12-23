import requests
import json
import re
import time
import csv
from datetime import datetime


now = datetime.now()  # current date and time
year = now.strftime("%Y")
month = now.strftime("%m")
start_time = datetime.strptime(
    year + month + "01T00:00-05:00", '%Y%m%dT%H:%M%z')
end_time = datetime.strptime(
    str(int(year) + 1) + month + "01T00:00-05:00", '%Y%m%dT%H:%M%z')

class NWSFetcher():
    def __init__(self, event_params):
        self.event_params = event_params
        self.cursor = None
        self.finished = False
        self.event_params = event_params
        self.event_params_index = 0
        self.url = 'https://api.weather.gov/alerts/?'

    def fetchWeatherData(self):
        params = self.event_params[self.event_params_index]
        url = self.url if self.cursor is None else self.cursor
        if self.cursor is None:
            response = self.retry_get(url, url_params=params)
        else:
            response = self.retry_get(url)

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

    def retry_get(self, url, url_params = {}, max_retries = 10, pass_errors = False):
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
            raise("Too many failed retries")
        else:
            return response

crawling = True
page = 0
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

should_verify = True
salt = str(datetime.now().strftime('%Y%m%d_%H'))
while crawling:
    time.sleep(3)
    data_raw = fetcher.fetchWeatherData()
    if fetcher.finished:
        crawling = False
    with open('data/raw/raw_report' + salt + '_' + str(page) +'.json', 'w', encoding='utf-8') as f:
        json.dump(data_raw, f, ensure_ascii=False, indent=4)
    print('Wrote page',page)
    page += 1
