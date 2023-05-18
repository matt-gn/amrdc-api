from datetime import datetime
from psycopg2 import sql
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from api_tools import query_database, generate_query, serve_csv, verify_input

## Define a FastAPI application which accepts all incoming requests
## and mount a publicly accessible /static directory for static content
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory="static"), name="static")

                        #######################
                        #### API ENDPOINTS ####
                        #######################
################################################################################
## Each of the following functions exposes an HTTP-accessible API endpoint to ##
## execute a SQL query and return data via JSON. We are using psycopg2 to     ##
## generate queries and access Postgres. The client db user is  READ-ONLY.    ##
################################################################################
                ###########################################
                ###  REALTIME AWS DATA  : `/realtime`   ###
                ###########################################

@app.get("/realtime/maxmin/{variable}", response_class=ORJSONResponse)
def current_maxmin_endpoint(variable: str) -> ORJSONResponse:
    max_q = sql.SQL("""SELECT station_name, TO_CHAR(date, 'YYYY-MM-DD'), TO_CHAR(time, 'HH24:MI:SS'), {}
                       FROM aws_realtime
                       ORDER BY {} DESC LIMIT 1""",).format(sql.Identifier(variable),
                                                            sql.Identifier(variable))
    min_q = sql.SQL("""SELECT station_name, TO_CHAR(date, 'YYYY-MM-DD'), TO_CHAR(time, 'HH24:MI:SS'), {}
                       FROM aws_realtime
                       ORDER BY {} ASC LIMIT 1""",).format(sql.Identifier(variable),
                                                           sql.Identifier(variable))
    data = {
        "max": query_database(max_q)["data"],
        "min": query_database(min_q)["data"], 
    }
    return ORJSONResponse(content=data)


@app.get("/realtime/station_list", response_class=ORJSONResponse)
def station_list_endpoint() -> ORJSONResponse:
    query = """SELECT DISTINCT(station_name), region
               FROM aws_realtime ORDER BY region, station_name"""
    query_results = query_database(query)["data"]
    station_dict = {}
    for station, region in query_results:
        station_dict.setdefault(region, []).append(station)
    return ORJSONResponse(content=station_dict)


@app.get("/realtime/station/{station}", response_class=ORJSONResponse)
def current_station_data_endpoint(station: str) -> ORJSONResponse:
    query = """SELECT station_name, TO_CHAR(date, 'YYYY-MM-DD') as date,
                TO_CHAR(time, 'HH24:MI:SS') as time, temperature, pressure,
                wind_speed, wind_direction, humidity
                FROM aws_realtime
                WHERE station_name = %s ORDER BY date DESC, time DESC LIMIT 1"""
    query_results = query_database(query, (station,))["data"]
    return ORJSONResponse(content=query_results)


##@app.get("/realtime/daily-maxmin/{variable}", response_class=ORJSONResponse)
##def daily_maxmin_endpoint(variable: str = None) -> ORJSONResponse:
##    query = ("""SELECT station_name, TO_CHAR(date, 'YYYY-MM-DD'), TO_CHAR(time, 'HH24:MI:SS'),
##                variable, datapoint
##                FROM aws_realtime_aggregate
##                WHERE variable=%s ORDER BY variable DESC""", (variable,))
##    query_results = query_database(query[0], query[1])["data"]
##    maximum, minimum = query_results
##    daily_aggregates = {"max":maximum,"min":minimum}
##    return ORJSONResponse(content=daily_aggregates)


                #######################################
                ###  HISTORICAL AWS DATA  : `/aws`  ###
                #######################################

@app.get("/aws/list", response_class=ORJSONResponse)
def list_stations_and_years_endpoint() -> ORJSONResponse:
    stations_list_query = "SELECT DISTINCT(station_name) FROM aws_10min ORDER BY station_name"
    years_list_query = "SELECT DISTINCT(date_part('year', date)::int) as date FROM aws_10min ORDER BY date"
    stations = query_database(stations_list_query)
    years = query_database(years_list_query)
    data = {
        "stations": stations["data"],
        "years": years["data"]
    }
    return ORJSONResponse(content=data)


@app.get("/aws/list/stations={stations}", response_class=ORJSONResponse)
def list_station_years_endpoint(stations: str) -> ORJSONResponse:
    station_list = tuple(station.replace('%20', ' ') for station in stations.split(','))
    query = ("""SELECT DISTINCT(date_part('year', date)::int) as date
             FROM aws_10min WHERE station_name IN %s ORDER BY date""", (station_list,))
    data = query_database(query[0], query[1])["data"]
    return ORJSONResponse(content=data)


@app.get("/aws/list/years={years}", response_class=ORJSONResponse)
def list_yearly_stations_endpoint(years: str) -> ORJSONResponse:
    years_list = tuple(year for year in years.split(','))
    query = ("""SELECT DISTINCT(station_name)
             FROM aws_10min WHERE date_part('year', date) IN %s ORDER BY station_name""", (years_list,))
    data = query_database(query[0], query[1])["data"]
    return ORJSONResponse(content=data)


@app.get("/aws/data", response_class=ORJSONResponse)
def query_data_endpoint(query_type: str = "all",
                        stations: str = None,
                        interval: int = 2400,
                        startdate: str = "19000101",
                        enddate: str = "99991231",
                        variable: str = None,
                        grouping: str = None,
                        download: bool = False) -> ORJSONResponse or StreamingResponse:
    input_error = verify_input(query_type, stations, variable, grouping)
    if input_error:
        return ORJSONResponse({'error': input_error})

    stations = tuple(station.replace('%20', ' ') for station in stations.split(','))
    startdate = datetime.strptime(startdate, '%Y') if len(startdate) == 4 else datetime.strptime(startdate.replace('-',''), '%Y%m%d')
    enddate = datetime.strptime(enddate, '%Y') if len(enddate) == 4 else datetime.strptime(enddate.replace('-',''), '%Y%m%d')
    query, params = generate_query(query_type, stations, interval, startdate, enddate,
                                   variable, grouping, download)
    data = query_database(query, params)
    if download:
        csv_stream = serve_csv(data, startdate, enddate)
        return csv_stream
    return ORJSONResponse(content=data)
