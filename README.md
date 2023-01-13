## Aeromancer
### Author: Matthew Mohr (matt at math you more dot com)
https://public.tableau.com/app/profile/matthew.mohr/viz/WinterWeatherTracker/Dashboard1

This generates reports via the NWS API (https://www.weather.gov/documentation/services-web-api). The script is broadly autonomous and should be allowed to run at least once per day.  The NWS API is limited in what it can return, roughly 2-4 weeks before the request date based on record count, so this has to be run regularly (and maybe more frequently during high-impact seasons). 

NWS uses something called Unique Geographical Codes (UGC, https://www.weather.gov/pimar/PubZone) to send out alerts.  This coding system ties *pretty* closely to county. There are exceptions - particularly around mountains and valleys because mountains and valleys will have different weather from sea-level elevations. There are also exceptions for areas of interest for the Department of the Interior, but frankly, if you're interested in those areas you're probably better off looking at a more robust toolset. The most up-to-date UGCs can be generated with UGC.py. The output file is nearly 100k records that don't change frequently, so you should be able to run it once and be done with it. 

Aeromancer.py will output two files: `data/{date}_events.csv` and `data/{date}_event_locations.csv`
NWS events are indexed to nws_id:
* Event (index: nws_id) - 1:M -> UGC. 

The current table structure, therefore:
* Events (PK: nws_id) - 1:M -> Event_Locations (FK: nws_id, ugc_id)
* Event_Locations (PK: nws_is, ugc_id) - 1:1 -> Locations(PK: UGC)
This, with a custom-built file that marries counties to zip codes, can be used to map NWS Events to zip codes and counties
by joining on the name and state of a UGC. It mostly works, with exceptions for the mountains and valleys as mentioned above. 
Included in this project is the Report Generator notebook. 
This API will pull in a large amount of rundant data, and the Report Generator will take care of that
