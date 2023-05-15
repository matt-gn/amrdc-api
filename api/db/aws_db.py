"""Initialize/rebuild the historical AWS database tables for the AMRDC AWS API"""
from datetime import datetime
from urllib.request import urlopen
from json import loads as json_loads
from config import postgres

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
        with urlopen(API_URL) as response:
            datasets = json_loads(response.read())
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
        with urlopen(url) as datafile:
            data = datafile.read().decode('utf-8').strip().split('\n')[2:]
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
    except Exception as error:
        print("Error building AWS table.")
        print(error)


def get_new_resource_list() -> list:
    API_URL = ('https://amrdcdata.ssec.wisc.edu/api/action/package_search?q='\
               'title:"quality-controlled+observational+data"&rows=1000')
    with urlopen(API_URL) as response:
        results = json_loads(response.read())
    datasets = results['result']['results']
    new_datasets = []
    for dataset in datasets:
        for resource in dataset['resources']:
            if "10min" in resource["name"]:
                last_modified = datetime.strptime(resource['last_modified'], '%Y-%m-%dT%H:%M:%S.%f')
                if last_modified > datetime.now() - timedelta(days=7):
                    name = dataset['title'].split(' Automatic Weather Station')[0]
                    url = resource['url']
                    new_datasets.append(tuple(name, url))
    return new_datasets

def rebuild_aws_table():
    try:
        new_datasets = get_new_resource_list()
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
    except Exception as error:
        print("Error updating AWS table.")
        print(error)

if __name__ == "__main__":
    print(f"{datetime.now()}\tStarting AWS database update")
    rebuild_aws_table()
    print(f"{datetime.now()}\tDone")
