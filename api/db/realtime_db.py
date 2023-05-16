"""Initialize/update the realtime database tables for the AMRDC AWS API"""
from datetime import datetime
import urllib3
from config import postgres

## Define HTTP connection pool manager
http = urllib3.PoolManager()

## Hardcoded ARGOS AWS metadata:
## (ARGOS ID#, Station Name, Antarctic Region)
ARGOS: tuple = (
    (8909, 'Cape Denison', 'Adelie Coast'),
    (8914, 'D-10', 'Adelie Coast'),
    (8916, 'D-47', 'Adelie Coast'),
    (8912, 'D-85', 'Adelie Coast'),
    (8927, 'AGO-4', 'High Polar Plateau'),
    (8989, 'Dome C II', 'High Polar Plateau'),
    (8904, 'Dome Fuji', 'High Polar Plateau'),
    (8985, 'Henry', 'High Polar Plateau'),
    (30305, 'JASE2007', 'High Polar Plateau'),
    (21359, 'Mizuho', 'High Polar Plateau'),
    (8924, 'Nico', 'High Polar Plateau'),
    (8918, 'Relay Station', 'High Polar Plateau'),
    (8984, 'Possession Island', 'Ocean Islands'),
    (8988, 'Whitlock', 'Ocean Islands'),
    (8905, 'Manuela', 'Reeves Glacier'),
    (21357, 'Elaine', 'Ross Ice Shelf'),
    (8939, 'Emilia', 'Ross Ice Shelf'),
    (8919, 'Emma', 'Ross Ice Shelf'),
    (8911, 'Gill', 'Ross Ice Shelf'),
    (8928, 'Lettau', 'Ross Ice Shelf'),
    (8910, 'Margaret', 'Ross Ice Shelf'),
    (8934, 'Marilyn', 'Ross Ice Shelf'),
    (8915, 'Sabrina', 'Ross Ice Shelf'),
    (8913, 'Schwerdtfeger', 'Ross Ice Shelf'),
    (8931, 'Vito', 'Ross Ice Shelf'),
    (8947, 'Ferrell', 'Ross Island'),
    (21360, 'Laurie II', 'Ross Island'),
    (8906, 'Marble Point', 'Ross Island'),
    (7351, 'Alessandra (IT)', 'Transantarctic Mountains'),
    (7357, 'Arelis (IT)', 'Transantarctic Mountains'),
    (7353, 'Eneide (IT)', 'Transantarctic Mountains'),
    (7355, 'Modesta (IT)', 'Transantarctic Mountains'),
    (7354, 'Rita (IT)', 'Transantarctic Mountains'),
    (7350, 'Sofia (IT)', 'Transantarctic Mountains'),
    (8903, 'Byrd', 'West Antarctica'),
    (21361, 'Elizabeth', 'West Antarctica'),
    (21363, 'Erin', 'West Antarctica'),
    (8900, 'Harry', 'West Antarctica'),
    (8936, 'Janet', 'West Antarctica'),
    (30393, 'Siple Dome', 'West Antarctica'),
    (8930, 'Thurston Island', 'West Antarctica')
)

def get_data_url(argos_id: str):
    """Helper method that returns URLs for the Argos data"""
    return f"http://amrc.ssec.wisc.edu/data/surface/awstext/{argos_id}.txt"


def read_data(url: str) -> list | None:
    """Access data via AMRC URL and return the most recent datapoint"""
    try:
        global http
        datafile = http.request("GET", url, retries=5)
        data = datafile.data.decode('utf-8').strip().split('\n')
        table = [line.split()[1:] for line in data if len(line.split()) == 10][2:]
        return table if table else None
    except Exception as error:
        print(f"Could not read datafile: {url}")
        print(error)


def process_datapoint(station_name: str, region: str, data: list) -> dict:
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
            "region": region
        }
        return params
    except Exception as e:
        print(f"Could not process datapoint: {station_name}\n{data}")
        print(e)

def update_realtime_table():
    with postgres:
        db = postgres.cursor()
        db.execute("DELETE FROM aws_realtime WHERE date < current_date - interval '30 days'")
    for (aws, station_name, region) in ARGOS:
        data = read_data(get_data_url(aws))
        params = tuple(process_datapoint(station_name, region, row) for row in data)
        with postgres:
            db = postgres.cursor()
            for row in params:
                db.execute("""MERGE INTO aws_realtime AS target
                              USING (VALUES (%(station_name)s,
                                             %(date)s,
                                             %(time)s,
                                             %(temperature)s,
                                             %(pressure)s,
                                             %(wind_speed)s,
                                             %(wind_direction)s,
                                             %(humidity)s,
                                             %(region)s))
                                      AS source(station_name, date, time, temperature, pressure, 
                                                wind_speed, wind_direction, humidity, delta_t)
                                      ON (target.station_name = source.station_name 
                                          AND target.date = source.date
                                          AND target.time = source.time)
                                      WHEN NOT MATCHED THEN
                                          INSERT (station_name, date, time, temperature, pressure,
                                                  wind_speed, wind_direction, humidity, delta_t)
                                          VALUES (source.station_name, source.date, source.time,
                                                  source.temperature, source.pressure, source.wind_speed,
                                                  source.wind_direction, source.humidity, source.delta_t)""", row)

if __name__ == "__main__":
    print(f"{datetime.now()}\tStarting realtime database update")
    update_realtime_table()
    print(f"{datetime.now()}\tDone")
