"""Initialize/rebuild the historical AWS database tables for the AMRDC AWS API"""
import urllib3
import json
from datetime import datetime
from config import postgres
import test

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
        datasets = json.loads(response.data)
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
            db.execute("INSERT INTO aws_10min_last_update (last_update) VALUES (NOW()::timestamp)")
            for resource_list in get_resource_urls():
                formatted_data = tuple(data for resource in resource_list
                                       for data in process_datafile(resource))
                db.executemany("INSERT INTO aws_10min VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               formatted_data)
    except Exception as error:
        print("Error initializing AWS table.")
        print(error)

def new_resources() -> bool:
    with postgres:
        db = postgres.cursor()
        db.execute("SELECT last_update FROM aws_10min_last_update")
        cutoff_date = db.fetchall()[0][0]
    API_URL = 'https://amrdcdata.ssec.wisc.edu/api/3/action/recently_changed_packages_activity_list'
    global http
    response = http.request("GET", API_URL, retries=3)
    results = json.loads(response.data)
    repo_timestamp = results['result'][0]['timestamp']
    last_modified = datetime.strptime(repo_timestamp, '%Y-%m-%dT%H:%M:%S.%f')
    if last_modified > cutoff_date:
        return True
    return False

def rebuild_aws_table():
    try:
        if new_resources():
            print("New resources available from data repo")
            with postgres:
                db = postgres.cursor()
                db.execute("""CREATE TABLE aws_10min_rebuild (
                            station_name VARCHAR(18),
                            date DATE,
                            time TIME,
                            temperature REAL,
                            pressure REAL,
                            wind_speed REAL,
                            wind_direction REAL,
                            humidity REAL,
                            delta_t REAL)""")
                db.execute("DELETE FROM aws_10min_last_update")
                db.execute("INSERT INTO aws_10min_last_update (last_update) VALUES (NOW()::timestamp)")
                for resource_list in get_resource_urls():
                    formatted_data = tuple(data for resource in resource_list
                                        for data in process_datafile(resource))
                    db.executemany("INSERT INTO aws_10min_rebuild VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                formatted_data)
                db.execute("DROP TABLE aws_10min")
                db.execute("ALTER TABLE aws_10min_rebuild RENAME TO aws_10min")
        else:
            print("No new resources available from data repo")
    except Exception as error:
        print("Error rebuilding AWS table.")
        print(error)

if __name__ == "__main__":
    print(f"{datetime.now()}\tStarting AWS database update")
    rebuild_aws_table()
    print(f"{datetime.now()}\tDone")
    test.test_db()
