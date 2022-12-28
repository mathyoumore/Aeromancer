from requests import get
import pandas as pd
from time import sleep
from json import loads

"""
This is a utility file to help build out the ugc_master.csv file that you can use to join events to UGCs

You should only run this if: 
* You don't have ugc_master.csv 
* We've added a state since the last time this was run 
"""
p_url = "https://api.weather.gov/zones/public"
c_url = "https://api.weather.gov/zones/county"

def retry_get(url, url_params = {}, max_retries = 10, pass_errors = False, should_verify = True, sleep_cycle = 5):
    response = get(url, params = url_params, verify=should_verify)
    retries = 0
    while retries < max_retries and response.status_code in (408, 502, 503, 504):
        print("Retryable status received, retry", retries)
        sleep(sleep_cycle)
        retries += 1
        response = get(url, params = url_params, verify=should_verify)
    if pass_errors:
        return (response.status_code, response)
    elif (response.status_code < 200 or response.status_code > 299):
        print("Error:", response.status_code)
        raise("Too many failed retries")
    else:
        return response

# I wouldn't bump the max_retries higher than this, to be nice to NWS
# If you're getting a lot of retries, run this at another time (maybe one without a notable weather event happening)
public_zones = loads(retry_get(p_url, max_retries = 3, pass_errors = False).text)['features']
county_zones = loads(retry_get(c_url, max_retries = 3, pass_errors = False).text)['features']

nws_master = public_zones + county_zones

new_row = {}
ugc_df = pd.DataFrame()
for item in nws_master:
    data_raw = item['properties']
    new_row = new_row | {
        'ugc': data_raw['id'],
        'type': data_raw['type'],
        'name': data_raw['name'],
        'state': data_raw['state']
    }
    ugc_df = pd.concat([ugc_df,pd.DataFrame([new_row])])

ugc_df.reset_index(drop=True).to_csv('ugc_master.csv', index=False)
