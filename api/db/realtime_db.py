"""Initialize/update the realtime database tables for the AMRDC AWS API"""
from datetime import datetime
from urllib.request import urlopen
from config import postgres

## Hardcoded ARGOS AWS metadata:
## (ARGOS ID#, Station Name, Webmap vector, Antarctic Region)
ARGOS: tuple = (
    (8909, 'Cape Denison', [160, 185], 'Adelie Coast'),
    (8914, 'D-10', [165, 182], 'Adelie Coast'),
    (8916, 'D-47', [165, 180], 'Adelie Coast'),
    (8912, 'D-85', [165, 175], 'Adelie Coast'),
    (8927, 'AGO-4', [155, 150], 'High Polar Plateau'),
    (8989, 'Dome C II', [160, 165], 'High Polar Plateau'),
    (8904, 'Dome Fuji', [142, 125], 'High Polar Plateau'),
    (8985, 'Henry', [134, 140], 'High Polar Plateau'),
    (30305, 'JASE2007', [145, 115], 'High Polar Plateau'),
    (21359, 'Mizuho', [162, 115], 'High Polar Plateau'),
    (8924, 'Nico', [140, 145], 'High Polar Plateau'),
    (8918, 'Relay Station', [155, 120], 'High Polar Plateau'),
    (8984, 'Possession Island', [140, 185], 'Ocean Islands'),
    (8988, 'Whitlock', [137, 177], 'Ocean Islands'),
    (8905, 'Manuela', [142, 180], 'Reeves Glacier'),
    (21357, 'Elaine', [137, 160], 'Ross Ice Shelf'),
    (8939, 'Emilia', [137, 167], 'Ross Ice Shelf'),
    (8919, 'Emma', [131, 161], 'Ross Ice Shelf'),
    (8911, 'Gill', [130, 165], 'Ross Ice Shelf'),
    (8928, 'Lettau', [132, 162], 'Ross Ice Shelf'),
    (8910, 'Margaret', [127, 162], 'Ross Ice Shelf'),
    (8934, 'Marilyn', [140, 160], 'Ross Ice Shelf'),
    (8915, 'Sabrina', [130, 160], 'Ross Ice Shelf'),
    (8913, 'Schwerdtfeger', [137, 162], 'Ross Ice Shelf'),
    (8931, 'Vito', [135, 170], 'Ross Ice Shelf'),
    (8947, 'Ferrell', [137, 169], 'Ross Island'),
    (21360, 'Laurie II', [137, 171], 'Ross Island'),
    (8906, 'Marble Point', [145, 175], 'Ross Island'),
    (7351, 'Alessandra (IT)', [144, 180], 'Transantarctic Mountains'),
    (7357, 'Arelis (IT)', [145, 175], 'Transantarctic Mountains'),
    (7353, 'Eneide (IT)', [144, 180], 'Transantarctic Mountains'),
    (7355, 'Modesta (IT)', [147, 179], 'Transantarctic Mountains'),
    (7354, 'Rita (IT)', [145, 177], 'Transantarctic Mountains'),
    (7350, 'Sofia (IT)', [150, 180], 'Transantarctic Mountains'),
    (8903, 'Byrd', [115, 152], 'West Antarctica'),
    (21361, 'Elizabeth', [120, 155], 'West Antarctica'),
    (21363, 'Erin', [125, 150], 'West Antarctica'),
    (8900, 'Harry', [117, 150], 'West Antarctica'),
    (8936, 'Janet', [115, 160], 'West Antarctica'),
    (30393, 'Siple Dome', [125, 160], 'West Antarctica'),
    (8930, 'Thurston Island', [97, 150], 'West Antarctica')
)

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
                return table[-1]
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


def init_realtime_table() -> None:
    """Initialize the aws_realtime database table"""
    try:
        with postgres:
            db = postgres.cursor()
            db.execute("""CREATE TABLE aws_realtime (
                        station_name VARCHAR(18),
                        date DATE,
                        time TIME,
                        temperature REAL,
                        pressure REAL,
                        wind_speed REAL,
                        wind_direction REAL,
                        humidity REAL,
                        latitude VARCHAR(3),
                        longitude VARCHAR(3),
                        region VARCHAR(24))""")
            for (aws, station_name, coords, region) in ARGOS:
                data = read_data(get_data_url(aws))
                if data:
                    params = process_datapoint(station_name, coords, region, data)
                    if params:
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
                                   %(region)s)""", params)
    except Exception as e:
        print("Realtime table initialization failed.")
        print(e)


def update_realtime_table() -> None:
    """Updates the aws_realtime database table"""
    try:
        with postgres:
            db = postgres.cursor()
            for (aws, station_name, coords, region) in ARGOS:
                data = read_data(get_data_url(aws))
                if data:    ## Important since we're returning None if there's no data
                    params = process_datapoint(station_name, coords, region, data)
                    if params:
                        db.execute("""UPDATE aws_realtime SET
                                    date = %(date)s,
                                    time = %(time)s,
                                    temperature = %(temperature)s,
                                    pressure = %(pressure)s,
                                    wind_speed = %(wind_speed)s,
                                    wind_direction = %(wind_direction)s,
                                    humidity = %(humidity)s
                                    WHERE station_name = %(station_name)s""", params)
    except Exception as e:
        print("Realtime table update failed.")
        print(e)


def init_aggregate_table() -> None:
    """Initialize the aws_realtime_aggregate database table with daily max/min values."""
    try:
        with postgres:
            db = postgres.cursor()
            db.execute("""CREATE TABLE aws_realtime_aggregate (
                       date DATE,
                       time TIME,
                       agg_type VARCHAR(3),
                       variable VARCHAR(14),
                       station_name VARCHAR(18),
                       datapoint REAL)""")
            # Here we iterate through each measurement variable so we can evaluate each max/min
            for variable in ("temperature", "pressure", "wind_speed", "wind_direction", "humidity"):
                # MAX: Pulls the current max out of the main table
                db.execute(f"""SELECT date, time, station_name, {variable} FROM aws_realtime
                                ORDER BY {variable} DESC LIMIT 1""")
                results = db.fetchone()
                (date, time, station_name, max_var) = results
                insert_row = (date, time, "max", variable, station_name, max_var)
                db.execute("INSERT INTO aws_realtime_aggregate VALUES (%s, %s, %s, %s, %s, %s)",
                           insert_row)
                # MIN: Same but with the minimum value
                db.execute(f"""SELECT date, time, station_name, {variable} FROM aws_realtime
                                ORDER BY {variable} ASC LIMIT 1""")
                results = db.fetchone()
                (date, time, station_name, min_var) = results
                insert_row = (date, time, "min", variable, station_name, min_var)
                db.execute("INSERT INTO aws_realtime_aggregate VALUES (%s, %s, %s, %s, %s, %s)",
                           insert_row)
    except Exception as e:
        print("Aggregate table initialization failed.")
        print(e)


def update_aggregate_table() -> None:
    """Update aws_realtime_aggregate database table with current daily max/min."""
    ## NOTE Values are changed if: max/min is exceeded; or if day has changed
    try:
        with postgres:
            db = postgres.cursor()
            ## Iterate through all measurements to derive max/min
            for variable in ("temperature", "pressure", "wind_speed", "wind_direction", "humidity"):
                # MAX: Get current maximum value from realtime table, daily maximum from daily_agg
                db.execute(f"""SELECT date, time, station_name, {variable}
                              FROM aws_realtime ORDER BY {variable} DESC LIMIT 1""")
                current_max_data = db.fetchone()
                (current_date,current_time,station_name,current_max) = current_max_data
                update_row = {"current_date": datetime.datetime(current_date).date,
                              "current_time": datetime.datetime(current_time).time(),
                              "current_max": float(current_max),
                              "station_name": station_name, 
                              "variable": variable}
                db.execute(f"""IF (SELECT datapoint FROM aws_realtime_aggregate
                                  WHERE agg_type="max" AND variable={variable}) < {current_max}
                              OR (SELECT date FROM aws_realtime_aggregate
                                  WHERE agg_type="max" AND variable={variable}) < {current_date}
                              THEN
                                  UPDATE aws_realtime_aggregate SET date={current_date}, time={current_time},
                                  station_name={station_name}, datapoint={current_max}
                                  WHERE agg_type="max" AND variable={variable}""", update_row)

                # MIN: Get current min from realtime and daily min from daily_agg
                db.execute(f"""SELECT date, time, station_name, {variable}
                              FROM aws_realtime ORDER BY {variable} ASC LIMIT 1""")
                current_min_data = db.fetchone()
                (current_date,current_time,station_name,current_min) = current_min_data
                update_row = {"current_date": datetime.datetime(current_date).date,
                              "current_time": datetime.datetime(current_time).time(),
                              "current_min": float(current_min),
                              "station_name": station_name,
                              "variable": variable}
                db.execute(f"""IF (SELECT datapoint FROM aws_realtime_aggregate
                                  WHERE agg_type="min" AND variable={variable} > {current_min}
                              OR (SELECT date FROM aws_realtime_aggregate
                                  WHERE agg_type="min" AND variable={variable} < {current_date}
                              THEN
                                  UPDATE aws_realtime_aggregate SET date={current_date}, time={current_time},
                                  station_name={station_name}, datapoint={current_min}
                                  WHERE agg_type="min" AND variable={variable}""", update_row)
    except Exception as e:
        print("Aggregate table update failed.")
        print(e)


if __name__ == "__main__":
    update_realtime_table()
    update_aggregate_table()
