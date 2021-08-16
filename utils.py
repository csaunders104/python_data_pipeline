import pandas as pd

def read_csv(file_name):
    return pd.read_csv('data/' + file_name)

def transform_events_data(file_list):
    # hardcoded country data
    countries_data = read_csv('countries.csv')

    # creation of empty list to be filled with dataframes
    dfs = []

    for file in file_list:
        print('Processing ' + file)
        events_data = pd.read_json(file)
        # file_name column added
        events_data = events_data.assign(file_name=file)
        dfs.append(events_data)
    events_data = pd.concat(dfs)

    events_data = events_data.replace(to_replace={'file_name': r'(data(?:/|\\))(.*)'}, value=r'\2', regex=True)

    # UID created using to_address
    events_data = events_data.replace(to_replace={'to_address': r'.*@.*'}, value='xxxxxxxx@xxxxx.xxx', regex=True)
    events_data = events_data.rename(columns={'to_address': 'UID'})

    # geo_country_name column added
    geo = events_data.geo_country.replace(countries_data.set_index('geo_country')['geo_country_name'])
    events_data['geo_country_name'] = geo
    events_data = events_data.replace(to_replace={'geo_country_name': r'^[\w]{2}$'}, value='N/A', regex=True)

    # file_date column added
    events_data['file_date'] = events_data['file_name']
    events_data = events_data.replace(to_replace={'file_date': r'(events_data-)([0-9-]+)(.json)'}, value=r'\2', regex=True)

    events_data = events_data[['uuid', 'event_type', 'device_type', 'geo_country', 'geo_country_name', 'geo_timezone', 'campaign_title', 'campaign_type', 'event_id', 'dt_occurred', 'UID', 'file_name', 'file_date']]

    print('~ Processed ' + str(len(events_data.index)) + ' rows ~')

    return events_data
