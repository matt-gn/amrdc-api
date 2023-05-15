"""Initialize/update the realtime database tables for the AMRDC AWS API"""
from datetime import datetime, timedelta
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
    return f"https://amrc.ssec.wisc.edu/data/surface/awstext/{argos_id}.txt"


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
    cutoff_date = datetime.now() - timedelta(days=30)
    with postgres:
        db = postgres.cursor()
        db.execute(f"DELETE FROM aws_realtime WHERE date < {cutoff_date}")
    for (aws, station_name, region) in ARGOS:
        data = read_data(get_data_url(aws))
        params = [process_datapoint(station_name, region, row) for row in data]
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
                                %(region)s)
                           ON CONFLICT (station_name, date, time) DO NOTHING""", params)

if __name__ == "__main__":
    print(f"{datetime.now()}\tStarting realtime database update")
    update_realtime_table()
    print(f"{datetime.now()}\tDone")
