from unittest import TestCase, main as test_all
from typing import Tuple
from config import postgres

##########################
## api_tools unit tests ##
##########################

def query_database(query_string: str, args: Tuple = ()) -> dict:
    with postgres:
        database = postgres.cursor()
        database.execute(query_string, args)
        header = tuple(col[0] for col in database.description)
        data = database.fetchall()
    return {"header": header, "data": data}

class TestAPI(TestCase):
    def test_query_database(self):
        ## aws_10min Tests
        aws_10min_schema = {'header': ('column_name', 'data_type'),
                            'data':   [('delta_t', 'real'), 
                                       ('date', 'date'),
                                       ('time', 'time without time zone'),
                                       ('temperature', 'real'),
                                       ('pressure', 'real'),
                                       ('wind_speed', 'real'),
                                       ('wind_direction', 'real'),
                                       ('humidity', 'real'),
                                       ('station_name', 'character varying')]}
        schema_query = """select column_name, data_type from information_schema.columns
                          where table_name = 'aws_10min'"""
        self.assertEqual(query_database(schema_query), aws_10min_schema)

        stations_list = query_database("SELECT DISTINCT(station_name) FROM aws_10min ORDER BY station_name")
        self.assertIsNotNone(stations_list['data'])
        years_list = query_database("SELECT DISTINCT(date_part('year', date)) as date FROM aws_10min ORDER BY date")
        self.assertIsNotNone(years_list['data'])

        test_result = query_database("""SELECT * FROM aws_10min
                                        WHERE station_name = 'Byrd' 
                                        AND to_char(date, 'YYYYMMDD') = '20160101'""")
        self.assertEqual(len(test_result), 2)
        header = ('station_name', 'date', 'time', 'temperature', 'pressure', 'wind_speed', 'wind_direction', 'humidity', 'delta_t')
        self.assertEqual(test_result['header'], header)
        self.assertIsNotNone(test_result['data'])

        ## aws_realtime tests
        aws_realtime_schema = {'header': ('column_name', 'data_type'),
                               'data':   [('humidity', 'real'),
                                          ('date', 'date'),
                                          ('time', 'time without time zone'),
                                          ('temperature', 'real'),
                                          ('pressure', 'real'),
                                          ('wind_speed', 'real'),
                                          ('wind_direction', 'real'),
                                          ('region', 'character varying'),
                                          ('latitude', 'character varying'),
                                          ('longitude', 'character varying'),
                                          ('station_name', 'character varying')]}
        schema_query = """select column_name, data_type from information_schema.columns
                          where table_name = 'aws_realtime'"""
        self.assertEqual(query_database(schema_query), aws_realtime_schema)
        test_result = query_database("SELECT * FROM aws_realtime")
        self.assertEqual(len(test_result), 2)
        header = ('station_name', 'date', 'time', 'temperature', 'pressure', 'wind_speed', 'wind_direction', 'humidity', 'latitude', 'longitude', 'region')
        self.assertEqual(test_result['header'], header)
        self.assertGreater(len(test_result['data']), 0)

        ## aws_realtime_aggregate tests
        aws_aggregate_schema = {'header': ('column_name', 'data_type'),
                                'data':   [('date', 'date'),
                                           ('time', 'time without time zone'),
                                           ('datapoint', 'real'),
                                           ('agg_type', 'character varying'),
                                           ('variable', 'character varying'),
                                           ('station_name', 'character varying')]}
        schema_query = """select column_name, data_type from information_schema.columns
                          where table_name = 'aws_realtime_aggregate'"""
        self.assertEqual(query_database(schema_query), aws_aggregate_schema)
        test_result = query_database("SELECT * FROM aws_realtime_aggregate")
        self.assertEqual(len(test_result), 2)
        header = ('date', 'time', 'agg_type', 'variable', 'station_name', 'datapoint')
        self.assertEqual(test_result['header'], header)
        self.assertGreater(len(test_result['data']), 0)

if __name__ == "__main__":
    test_all()
