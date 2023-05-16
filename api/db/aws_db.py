"""Initialize/rebuild the historical AWS database tables for the AMRDC AWS API"""
import urllib3
from datetime import datetime
from config import postgres

## Define HTTP connection pool manager
http = urllib3.PoolManager()

def extract_resource_list(dataset: dict) -> tuple:
    """Receives a dict of an AMRDC AWS dataset and returns its resource urls."""
    try:
        name,_ = dataset["title"].split(" Automatic Weather Station,")
        resource_list = tuple((name, resource["url"])
                          for resource in dataset["resources"]
                          if "10min" in resource["name"])
        return resource_list
    except Exception as error:
        print("Error extracting resource: " + dataset["title"])
        print(error)


def get_resource_urls() -> tuple:
    """Fetches a list of all AWS datasets and returns all available resources."""
    try:
        API_URL = ('https://amrdcdata.ssec.wisc.edu/api/action/package_search?q='\
                   'title:"quality-controlled+observational+data"&rows=1000')
        global http
        response = http.request("GET", API_URL, retries=3)
        datasets = response.json()
        att = "Alexander Tall Tower"
        resource_lists = tuple(extract_resource_list(dataset)
                          for dataset in datasets['result']['results']
                          if att not in dataset["title"])
        return resource_lists
    except Exception as error:
        print("Could not fetch dataset list from AMRDC repository API.")
        print(error)


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
        global http
        datafile = http.request("GET", url, retries=5)
        data = datafile.data.decode('utf-8').strip().split('\n')[2:]
        formatted_datafile = tuple(process_datapoint(name, line) for line in data)
        return formatted_datafile
    except Exception as error:
        print(f"Could not process resource: {name}\n{url}")
        print(error)


def init_aws_table() -> None:
    """Initialize database; collect resource urls; read each resource into database"""
    try:
        with postgres:
            db = postgres.cursor()
            for resource_list in get_resource_urls():
                formatted_data = tuple(data for resource in resource_list
                                       for data in process_datafile(resource))
                db.executemany("INSERT INTO aws_10min VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               formatted_data)
            db.execute("INSERT INTO aws_10min_last_update (last_update) VALUES (CURRENT_DATE)")
    except Exception as error:
        print("Error building AWS table.")
        print(error)

## TODO Test this!
def get_new_resource_list() -> list:
    API_URL = ('https://amrdcdata.ssec.wisc.edu/api/action/package_search?q='\
               'title:"quality-controlled+observational+data"&rows=1000')
    global http
    response = http.request("GET", API_URL, retries=3)
    results = response.json()
    datasets = results['result']['results']
    new_datasets = []
    with postgres:
        db = postgres.cursor()
        db.execute("SELECT last_update FROM aws_10min_last_update")
        cutoff_date = db.fetchall()[0][0]
    for dataset in datasets:
        for resource in dataset['resources']:
            if "10min" in resource["name"]:
                last_modified = datetime.strptime(resource['last_modified'], '%Y-%m-%dT%H:%M:%S.%f').date()
                if last_modified > cutoff_date:
                    name = dataset['title'].split(' Automatic Weather Station')[0]
                    url = resource['url']
                    new_datasets.append((name, url))
    return new_datasets

def rebuild_aws_table():
    try:
        new_datasets = get_new_resource_list()
        with postgres:
            db = postgres.cursor()
            if new_datasets:
                for dataset in new_datasets:
                ## Capture new rows while updating hash collisions
                    data = process_datafile(dataset)
                    insert_statement = """MERGE INTO aws_10min AS target
                                        USING (VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s))
                                        AS source(station_name, date, time, temperature, pressure, 
                                                    wind_speed, wind_direction, humidity, delta_t)
                                        ON (target.station_name = source.station_name 
                                            AND target.date = source.date
                                            AND target.time = source.time)
                                        WHEN MATCHED THEN
                                            UPDATE SET temperature = source.temperature,
                                                        pressure = source.pressure,
                                                        wind_speed = source.wind_speed,
                                                        wind_direction = source.wind_direction,
                                                        humidity = source.humidity,
                                                        delta_t = source.delta_t
                                        WHEN NOT MATCHED THEN
                                            INSERT (station_name, date, time, temperature, pressure,
                                                    wind_speed, wind_direction, humidity, delta_t)
                                            VALUES (source.station_name, source.date, source.time,
                                                    source.temperature, source.pressure, source.wind_speed,
                                                    source.wind_direction, source.humidity, source.delta_t)"""
                    for line in data:
                        db.execute(insert_statement, data)
            else:
                print("No new datasets")
            db.execute("DELETE FROM aws_10min_last_update")
            db.execute("INSERT INTO aws_10min_last_update (last_update) VALUES (CURRENT_DATE)")
    except Exception as error:
        print("Error updating AWS table.")
        print(error)

if __name__ == "__main__":
    print(f"{datetime.now()}\tStarting AWS database update")
    rebuild_aws_table()
    print(f"{datetime.now()}\tDone")
