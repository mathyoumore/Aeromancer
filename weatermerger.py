import pandas as pd
# import required module
import os
import re
import datetime


# assign directory

sentence_tokenizer = r'(\s*[^.!?]*[.!?]{1,3})'
event_tokenizer = r'(?P<precipitation>sleet|snow|ice)\saccumulations\sof\s(?P<accumulation_range_qualifier>up\sto\s)?(?P<accumulation_range_lower>[1234567890]{1,2}|one|two|three|four|five|six|seven|eight|nine)\s(?P<accumulation_denom_1>tenths?\s|quarters?\s)?(to\s)?(of\s)?(?P<accumulation_range_upper>[1234567890]{1,2}\s|one\s|two\s|three\s|four\s|five\s|six\s|seven\s|eight\s|nine\s)?(?P<accumulation_denom_2>tenths?\s|quarters?\s)?(of\s)?(?P<accumulation_unit>inch|inches|an inch|a foot)'


def processPrecipitation(raw):
    matches = []
    simple_desc = re.sub("\n", ' ', str(raw))
    sentences = re.split(sentence_tokenizer, simple_desc)
    for s in sentences:
        match = re.search(event_tokenizer, s)
        if match is not None:
            matches.append(match)
    return matches


directory = 'data/merge_prep'

panda_cage = []
panda_cage_names = []

generic_precip = [
    'accumulation_range_qualifier',
    'accumulation_range_lower',
    'accumulation_denom_1',
    'accumulation_range_upper',
    'accumulation_denom_2',
    'accumulation_unit']


merged_table = pd.DataFrame(columns=[
    'id',
    'state',
    'simple_county',
    'full_county',
    'severity',
    'event',
    'effective',
    'effective_iso',
    'expiration',
    'expiration_iso'
    'snow',
    'snow_accumulation_range_qualifier',
    'snow_accumulation_range_lower',
    'snow_accumulation_denom_1',
    'snow_accumulation_range_upper',
    'snow_accumulation_denom_2',
    'snow_accumulation_unit',
    'ice',
    'ice_accumulation_range_qualifier',
    'ice_accumulation_range_lower',
    'ice_accumulation_denom_1',
    'ice_accumulation_range_upper',
    'ice_accumulation_denom_2',
    'ice_accumulation_unit']
)

for filename in os.listdir(directory):

    f = os.path.join(directory, filename)
    if f[len(f) - 3:] == 'csv':
        raw_panda = pd.read_csv(f, on_bad_lines='skip')
        clean_panda = raw_panda.rename(columns={
            "State": "state",
            "County": "full_county",
            "county": "full_county",
            "Full County": "full_county",
            "Zips": "zip_codes",
            "zips": "zip_codes",
            "Population": "population",
            "Event": "event",
            "start": "effective",
            "start_raw": "effective_iso",
            "Severity": "severity",
            "Effective": "effective",
            "Effective ISO": "effective_iso",
            "Expiration": "expiration",
            "Expiration ISO": "expiration_iso",
            "expiry": "expiration",
            "expiry_raw": "expiration_iso",
            })
        panda_cage.append(clean_panda)
        panda_cage_names.append((f))

dirty_merge = pd.concat(panda_cage, ignore_index=True)

print("Pandas caged")


merged_table = dirty_merge[['id',
                           'state',
                            'full_county',
                            'zip_codes',
                            'severity',
                            'event',
                            'effective',
                            'effective_iso',
                            'expiration',
                            'expiration_iso']].copy()

#nan_rows = dirty_merge[dirty_merge['effective'].isnull()]
for id, row in dirty_merge.iterrows():
    if id % 100 == 0:
        print(f"{id}")
    matches = processPrecipitation(row['description'])
    precip = {}
    for m in matches:
        m_dict = m.groupdict()
        type = m_dict['precipitation']
        for gp in generic_precip:
            precip[type + '_' + gp] = m_dict[gp]
        precip[type] = 1
    for column_name, value in precip.items():
        merged_table.at[id, column_name] = value
merged_table.drop_duplicates()
merged_table.sort_values(by=['effective_iso'], inplace=True)
clean_table = pd.DataFrame(columns=merged_table.columns)
clean_table = pd.concat([merged_table, clean_table], ignore_index=True)
mydate = datetime.datetime.now()
mydate.strftime("%B")
out_filename = mydate.strftime("%Y %B") + " report.csv"
clean_table.to_csv(out_filename)
