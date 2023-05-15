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
        resource_lists = (extract_resource_list(dataset)
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


def initialize_database() -> list[str]:
    """Commands to initialize the database with the required AWS tables"""
    return ["""CREATE TABLE aws_10min (
                station_name VARCHAR(18),
                date DATE,
                time TIME,
                temperature REAL,
                pressure REAL,
                wind_speed REAL,
                wind_direction REAL,
                humidity REAL,
                delta_t REAL)""",
                "CREATE TABLE aws_10min_backup (LIKE aws_10min INCLUDING ALL)"]


def rebuild_database() -> list[str]:
    """Commands to backup old database and create new one for rebuild"""
    return ["DROP TABLE aws_10min_backup",
            "ALTER TABLE aws_10min RENAME TO aws_10min_backup",
            """CREATE TABLE aws_10min (
                station_name VARCHAR(18),
                date DATE,
                time TIME,
                temperature REAL,
                pressure REAL,
                wind_speed REAL,
                wind_direction REAL,
                humidity REAL,
                delta_t REAL)"""]

def init_aws_table() -> None:
    """Initialize database; collect resource urls; read each resource into database"""
    try:
        with postgres:
            db = postgres.cursor()
            init_commands = initialize_database()
            for command in init_commands:
                db.execute(command)
            for resource_list in get_resource_urls():
                formatted_data = tuple(data for resource in resource_list
                                       for data in process_datafile(resource))
                db.executemany("INSERT INTO aws_10min VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               formatted_data)
    except Exception as error:
        print("Error building database.")
        print(error)


def rebuild_aws_table() -> None:
    """Initialize database; collect resource urls; read each resource into database"""
    try:
        with postgres:
            db = postgres.cursor()
            rebuild_commands = rebuild_database()
            for command in rebuild_commands:
                db.execute(command)
            for resource_list in get_resource_urls():
                formatted_data = tuple(data for resource in resource_list
                                       for data in process_datafile(resource))
                db.executemany("INSERT INTO aws_10min VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               formatted_data)
    except Exception as error:
        print("Error building database.")
        print(error)


if __name__ == "__main__":
    print(f"{datetime.now()}\tStarting AWS database update")
    rebuild_aws_table()
    print(f"{datetime.now()}\tDone")
