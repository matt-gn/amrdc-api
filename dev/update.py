"""This script updates the historical_aws table with updates from the last 7 days"""
from datetime import datetime, timedelta
from urllib.request import urlopen
from json import loads as json_loads
from os import environ
import psycopg2

## Set DB credentials
DB_NAME = environ.get("POSTGRES_DB")
DB_USER = environ.get("POSTGRES_USER")
DB_PASSWORD = environ.get("POSTGRES_PASSWORD")
DB_HOST = environ.get("POSTGRES_HOST")
DB_PORT = environ.get("POSTGRES_PORT")
postgres = psycopg2.connect(
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT)

def update_aws_table():

    def process_datapoint(name: str, line: str) -> tuple:
        """Accepts a row of data from a datafile and returns a formatted dict"""
        try:
            row = line.split()
            formatted_date = f"{row[0]}-{row[2]}-{row[3]}"
            params = (name,formatted_date) + tuple(row[4:])
            return params
        except Exception as error:
            print(f"Error processing datapoint: {name}\n{line}")
            print(error)

    def process_datafile(resource: tuple) -> tuple:
        """Accesses a datafile via URL and returns a list containing each row"""
        name, url = resource
        try:
            with urlopen(url) as datafile:
                data = datafile.read().decode('utf-8').strip().split('\n')[2:]
                formatted_datafile = tuple(process_datapoint(name, line) for line in data)
                return formatted_datafile
        except Exception as error:
            print(f"Could not process resource: {name}\n{url}")
            print(error)

    API_URL = ('https://amrdcdata.ssec.wisc.edu/api/action/package_search?q='\
               'title:"quality-controlled+observational+data"&rows=1000')
    with urlopen(API_URL) as response:
        results = json_loads(response.read())

    datasets = results['result']['results']

    new_datasets = []

    for dataset in datasets:
        for resource in dataset['resources']:
            last_modified = datetime.strptime(resource['last_modified'], '%Y-%m-%dT%H:%M:%S.%f')
            if last_modified > datetime.now() - timedelta(days=7):
                name = dataset['title'].split(' Automatic Weather Station')[0]
                url = resource['url']
                new_datasets.append(tuple(name, url))

    for dataset in new_datasets:
    ## Capture new rows while updating hash collisions
        data = process_datafile(dataset)
        insert_statement = """INSERT INTO aws_10min VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                              ON CONFLICT (station_name, date, time) DO UPDATE
                              SET station_name = %s,
                                   date = %s,
                                   time = %s,
                                   temperature = %s,
                                   pressure = %s,
                                   wind_speed = %s,
                                   wind_direction = %s,
                                   humidity = %s,
                                   delta_t = %s"""
        with postgres:
            db = postgres.cursor()
            for line in data:
                db.execute(insert_statement, data)

    return None


def update_realtime_table():
    cutoff_date = datetime.now() - timedelta(days=30)
    with postgres:
        db = postgres.cursor()
        db.execute(f"DELETE FROM aws_realtime WHERE date < {cutoff_date}")

    def get_data_url(argos_id: str):
        """Helper method that returns URLs for the Argos data"""
        return f"https://amrc.ssec.wisc.edu/data/surface/awstext/{argos_id}.txt"


    def read_data(url: str) -> list | None:
        """Access data via AMRC URL and return the most recent datapoint"""
        try:
            with urlopen(url) as datafile:
                data = datafile.read().decode('utf-8').strip().split('\n')
                table = [line.split()[1:] for line in data if len(line.split()) == 10][2:]
                if len(table) > 0:
                    return table
                return None             ## Return None if there's no available data
        except Exception as error:
            print(f"Could not read datafile: {url}")
            print(error)

    def process_datapoint(station_name: str, coords: list, region: str, data: list) -> dict:
        """Formats a row of data into our database schema dict"""
        try:
            date_str, time, temp, press, wind_spd, wind_dir, hum, _, _ = data   # Unpack datapoint
            standard_date = datetime.strptime(date_str, '%Y%j').date()
            standard_time = datetime.strptime(time, '%H%M%S').time()
            params = {
                "station_name": station_name,
                "date": standard_date,
                "time": standard_time,
                "temperature": float(temp),
                "pressure": float(press),
                "wind_speed": float(wind_spd),
                "wind_direction": int(wind_dir),
                "humidity": float(hum),
                "latitude": coords[0],
                "longitude": coords[1],
                "region": region
            }
            return params
        except Exception as e:
            print(f"Could not process datapoint: {station_name}\n{data}")
            print(e)


    ## Parse data normally
    for (aws, station_name, coords, region) in ARGOS:
        data = read_data(get_data_url(aws))
        params = [process_datapoint(station_name, coords, region, row) for row in data]
        with postgres:
            db = postgres.cursor()
            for row in params:
                db.execute("""INSERT INTO aws_realtime VALUES (
                                %(station_name)s,
                                %(date)s,
                                %(time)s,
                                %(temperature)s,
                                %(pressure)s,
                                %(wind_speed)s,
                                %(wind_direction)s,
                                %(humidity)s,
                                %(latitude)s,
                                %(longitude)s,
                                %(region)s)
                           ON CONFLICT (station_name, date, time) DO NOTHING""", params)

    return None