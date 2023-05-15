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
