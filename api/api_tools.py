from os import environ
from typing import Tuple
from datetime import datetime
from psycopg2 import sql, pool
from fastapi.responses import StreamingResponse

## Set DB credentials
DB_NAME = environ.get("POSTGRES_DB")
DB_USER = environ.get("CLIENT_USER")
DB_PASSWORD = environ.get("CLIENT_PASSWORD")
DB_HOST = environ.get("POSTGRES_HOST")
DB_PORT = environ.get("POSTGRES_PORT")

## Define Postgres connection pool for concurrent connections
CONNECTION_POOL = None

def create_connection_pool():
    global CONNECTION_POOL
    CONNECTION_POOL = pool.SimpleConnectionPool(
        minconn=1,
        maxconn=100,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )

def get_connection():
    global CONNECTION_POOL
    if CONNECTION_POOL is None:
        create_connection_pool()
    return CONNECTION_POOL.getconn()

def return_connection(conn):
    global CONNECTION_POOL
    CONNECTION_POOL.putconn(conn)

def query_database(query_string: str, args: Tuple = ()) -> dict:
    postgres = get_connection()
    with postgres:
        database = postgres.cursor()
        database.execute(query_string, args)
        header = tuple(col[0] for col in database.description)
        data = database.fetchall()
    return {"header": header, "data": data}

def generate_query(query_type: str, stations: list, interval: int, startdate: datetime, enddate: datetime,
                   variable: int, grouping: str, download: bool) -> Tuple[sql.SQL, Tuple] | Tuple[None, None]:

    match query_type:
        ## Returns all datapoints (no aggregating) for time period by interval
        case "all":
            limit_stmt = sql.SQL(f"LIMIT {10**5}") if download else sql.SQL(f"LIMIT {10**4}")
            ## If variable is supplied, only return that column's values
            if variable:
                variable = sql.Identifier(variable)
                return sql.SQL("""SELECT
                                    station_name as Name,
                                    TO_CHAR(date, 'YYYY-MM-DD') as Date,
                                    TO_CHAR(time, 'HH24:MI') as Time,
                                    CAST({} as TEXT)
                                  FROM aws_10min
                                  WHERE
                                    station_name IN %s 
                                    AND date >= %s 
                                    AND date <= %s 
                                    AND MOD((date_part('hour', time) * 100 + date_part('minute', time))::int, %s) = 0
                                  ORDER BY date, time
                                  {}""").format(variable,
                                                limit_stmt),(stations, startdate, enddate, interval)
            ## Else, return all columns
            return sql.SQL("""SELECT
                                station_name as Name,
                                TO_CHAR(date, 'YYYY-MM-DD') as Date,
                                TO_CHAR(time, 'HH24:MI') as Time,
                                CAST(temperature as TEXT),
                                CAST(pressure as TEXT),
                                CAST(wind_speed as TEXT),
                                CAST(wind_direction as TEXT),
                                CAST(humidity as TEXT),
                                CAST(delta_t as TEXT)
                            FROM aws_10min
                            WHERE
                                station_name IN %s 
                                AND date >= %s 
                                AND date <= %s 
                                AND MOD((date_part('hour', time) * 100 + date_part('minute', time))::int, %s) = 0
                            ORDER BY date, time
                            {}""").format(limit_stmt), (stations, startdate, enddate, interval)

        ## Max/min reading for a given variable from selected stations between two dates,
        ## grouped by station and a given time period
        case "max" | "min":
            variable = sql.Identifier(variable)
            aggregator = sql.SQL('ASC') if query_type == "min" else sql.SQL('DESC')
            query_type = sql.SQL(query_type)

            ## Select overall max/min from entire database.
            if "all" in stations and grouping == "station":
                return sql.SQL("""SELECT station_name, TO_CHAR(date, 'YYYY-MM-DD') as date, TO_CHAR(time, 'HH24:MI') as time, {}
                                  FROM aws_10min WHERE {} != 444 AND date >= %s AND date <= %s
                                  ORDER BY {} {} LIMIT 1""").format(variable, variable, variable, aggregator), (startdate, enddate)

            ## Select overall max/min from entire database, grouped by interval
            if "all" in stations and grouping in ("year", "month", "day"):
                grouping = sql.SQL(grouping)
                return sql.SQL("""SELECT
                                    aws.station_name as Name,
                                    TO_CHAR(aws.date, 'YYYY-MM-DD') as Date,
                                    TO_CHAR(aws.time, 'HH24:MI') as Time,
                                    CAST(aws.{} as TEXT) as {}
                                FROM (
                                    SELECT
                                        station_name,
                                        date,
                                        time,
                                        {},
                                        ROW_NUMBER() OVER (
                                        PARTITION BY
                                            date_trunc('{}', date)
                                        ORDER BY
                                            {} {}
                                        ) as row_num
                                    FROM
                                        aws_10min
                                    WHERE
                                        date >= %s AND 
                                        date <= %s AND 
                                        {} != 444
                                ) aws
                                WHERE
                                    row_num = 1
                                ORDER BY
                                    date""").format(variable,
                                                    variable,
                                                    variable,
                                                    grouping,
                                                    variable,
                                                    aggregator,
                                                    variable), (startdate, enddate)

            ## Select individual station max/min for all time
            if grouping == "station":
                return sql.SQL("""SELECT
                                    aws.station_name as Name,
                                    TO_CHAR(aws.date, 'YYYY-MM-DD') as Date,
                                    TO_CHAR(aws.time, 'HH24:MI') as Time,
                                    CAST(aws.{} as TEXT) as {}
                                FROM (
                                    SELECT
                                        station_name,
                                        date,
                                        time,
                                        {},
                                        ROW_NUMBER() OVER (
                                        PARTITION BY
                                            station_name
                                        ORDER BY
                                            {} {}
                                        ) as row_num
                                    FROM
                                        aws_10min
                                    WHERE
                                        station_name IN %s AND 
                                        date >= %s AND 
                                        date <= %s AND 
                                        {} != 444
                                ) aws
                                WHERE
                                    row_num = 1
                                ORDER BY
                                    aws.station_name, date""").format(variable,
                                                                variable,
                                                                variable,
                                                                variable,
                                                                aggregator,
                                                                variable), (stations, startdate, enddate)

            ## Select individual station max/min, grouped by interval
            if grouping in ("year", "month", "day"):
                grouping = sql.SQL(grouping)
                return sql.SQL("""SELECT
                                    aws.station_name as Name,
                                    TO_CHAR(aws.date, 'YYYY-MM-DD') as Date,
                                    TO_CHAR(aws.time, 'HH24:MI') as Time,
                                    CAST(aws.{} as TEXT) as {}
                                FROM (
                                    SELECT
                                        station_name,
                                        date,
                                        time,
                                        {},
                                        ROW_NUMBER() OVER (
                                        PARTITION BY
                                            station_name,
                                            date_trunc('{}', date)
                                        ORDER BY
                                            {} {}
                                        ) as row_num
                                    FROM
                                        aws_10min
                                    WHERE
                                        station_name IN %s AND 
                                        date >= %s AND 
                                        date <= %s AND 
                                        {} != 444
                                ) aws
                                WHERE
                                    row_num = 1
                                ORDER BY
                                    aws.station_name, date""").format(variable,
                                                    variable,
                                                    variable,
                                                    grouping,
                                                    variable,
                                                    aggregator,
                                                    variable), (stations, startdate, enddate)


        ## Calculate mean for a given variable for selected stations between two dates,
        ## grouped by station and a given time period
        case "mean":
            variable = sql.Identifier(variable)
            if "all" in stations and grouping == "station":
                return sql.SQL("""SELECT AVG({}) FROM aws_10min
                                  WHERE date >= %s AND date <= %s 
                                  AND {} != 444""").format(variable, variable), (startdate, enddate)

            if "all" in stations and grouping in ("year", "month", "day"):
                date_format = sql.SQL("YYYY") if grouping == "year" else\
                            sql.SQL("YYYY-MM") if grouping == "month" else\
                            sql.SQL("YYYY-MM-DD")
                grouping = sql.SQL(grouping)
                return sql.SQL("""SELECT
                                    TO_CHAR(avg.timeperiod, '{}') as Duration,
                                    CAST(avg.{} as TEXT) as avg {}
                                FROM (
                                    SELECT DISTINCT
                                        date_trunc('{}', date) as timeperiod,
                                        ROUND(AVG({})::numeric, 2)::float as {}
                                    FROM
                                        aws_10min 
                                    WHERE
                                        date >= %s AND 
                                        date <= %s AND 
                                        {} != 444 
                                    GROUP BY
                                        date_trunc('{}', date)
                                ) avg
                                ORDER BY
                                    avg.timeperiod""").format(date_format,
                                                            variable,
                                                            variable,
                                                            grouping,
                                                            variable,
                                                            variable,
                                                            variable,
                                                            grouping), (startdate, enddate)


            if grouping == "station":
                return sql.SQL("""SELECT
                                    avg.station_name as Name, 
                                    CAST(avg.{} as TEXT) as AVG
                                FROM (
                                    SELECT DISTINCT
                                        station_name,
                                        ROUND(AVG({})::numeric, 2)::float as {}
                                    FROM
                                        aws_10min 
                                    WHERE
                                        station_name IN %s AND 
                                        date >= %s AND 
                                        date <= %s AND 
                                        {} != 444 
                                    GROUP BY
                                        station_name
                                ) avg
                                ORDER BY
                                    avg.timeperiod""").format(variable,
                                                              variable,
                                                              variable,
                                                              variable), (stations, startdate, enddate)

            if grouping in ("year", "month", "day"):
                date_format = sql.SQL("YYYY") if grouping == "year" else\
                            sql.SQL("YYYY-MM") if grouping == "month" else\
                            sql.SQL("YYYY-MM-DD")
                grouping = sql.SQL(grouping)
                return sql.SQL("""SELECT
                                    avg.station_name as Name, 
                                    TO_CHAR(avg.timeperiod, '{}') as Duration,
                                    CAST(avg.{} as TEXT) as AVG
                                FROM (
                                    SELECT DISTINCT
                                        station_name,
                                        date_trunc('{}', date) as timeperiod,
                                        ROUND(AVG({})::numeric, 2)::float as {}
                                    FROM
                                        aws_10min 
                                    WHERE
                                        station_name IN %s AND 
                                        date >= %s AND 
                                        date <= %s AND 
                                        {} != 444 
                                    GROUP BY
                                        station_name, 
                                        date_trunc('{}', date)
                                ) avg
                                ORDER BY
                                    avg.timeperiod""").format(date_format,
                                                            variable,
                                                            grouping,
                                                            variable,
                                                            variable,
                                                            variable,
                                                            grouping), (stations, startdate, enddate)

        case other:
            return None, None

def serve_csv(data: dict, startdate: str, enddate: str) -> StreamingResponse:
    citation = create_citation(startdate.strftime('%Y-%m'), enddate.strftime('%Y-%m'))
    filename = f"AMRDC Data Warehouse {datetime.now().date()}.csv"
    async def csv_generator(data):
        yield citation
        yield ','.join(data["header"])
        for row in data["data"]:
            yield ','.join(row)
    return StreamingResponse(csv_generator(data),
                             media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename={filename}"})

def create_citation(startdate: str, enddate: str) -> str:
    date = f"{startdate} - {enddate}"
    citation = "Antarctic Meteorological Research and Data Center: Automatic Weather Station " +\
               "quality-controlled observational data. AMRDC Data Repository. Subset used: " +\
               f"{date}, accessed {datetime.now().date()}, https://doi.org/10.48567/1hn2-nw60."
    return citation

def verify_input(query_type: str,
                 stations: str,
                 variable: str,
                 grouping: str) -> str | None:
    if query_type == "all" and stations is None:
        return "Data query requires following variables: stations (comma-separated)"
    elif query_type in ("max", "min", "mean") and None in (stations, variable, grouping):
        return "Aggregate query requires following variables: stations (comma-separated), " +\
               "measurement variable (i.e. 'temperature'), grouping (day, month, year, or station)"
    elif query_type not in ("all", "max", "min", "mean"):
        return "Unrecognized query type. Must be one of: 'all', 'max', 'min', 'mean'"
    return None
