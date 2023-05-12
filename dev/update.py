"""This script updates the historical_aws table with updates from the last 7 days"""
from datetime import datetime, timedelta
from urllib.request import urlopen
from json import loads as json_loads

def update_aws_table():
    API_URL = ('https://amrdcdata.ssec.wisc.edu/api/action/package_search?q='\
               'title:"quality-controlled+observational+data"&rows=1000')
    with urlopen(API_URL) as response:
        results = json_loads(response.read())

    datasets = results['result']['results']

    new_datasets = []

    for dataset in datasets:
        for resource in dataset['resources']:
            if resource['last_modified'] > datetime.now() - timedelta(days=7):
                new_datasets.append(resource['url'])

    ## Capture new rows while updating hash collisions
    ## INSERT INTO table (col1, col2...) VALUES (value1, value2....)
    ## ON CONFLICT (station, date, time) DO UPDATE SET (col1 = value1, col2 = value2...)
    return None


def update_realtime_table():
    cutoff_date = datetime.now() - timedelta(days=30)
    ## DROP * FROM realtime_aws WHERE date < cutoff_date

    ## Parse data normally
    
    ## INSERT INTO realtime_aws (col1, col2....) VALUES (value1, value2...)
    ## ON CONFLICT (station, date, time) DO NOTHING
    return None