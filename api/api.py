from datetime import datetime
from psycopg2 import sql
from fastapi import FastAPI
from fastapi.responses import JSONResponse, StreamingResponse
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

@app.get("/realtime/maxmin/{variable}")
def current_maxmin_endpoint(variable: str) -> JSONResponse:
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
    return JSONResponse(content=data)


@app.get("/realtime/station_list")
def station_list_endpoint() -> JSONResponse:
    query = """SELECT station_name, region
               FROM aws_realtime ORDER BY region, station_name"""
    query_results = query_database(query)["data"]
    station_dict = {}
    for station, region in query_results:
        station_dict.setdefault(region, []).append(station)
    return JSONResponse(content=station_dict)


@app.get("/realtime/station/{stations}")
def current_station_data_endpoint(stations: str) -> JSONResponse:
    query_list = tuple(station.replace('%20', ' ') for station in stations.split(','))
    query = ("""SELECT station_name, TO_CHAR(date, 'YYYY-MM-DD'), TO_CHAR(time, 'HH24:MI:SS'),
                temperature, pressure, wind_speed, wind_direction, humidity, latitude, longitude
                FROM aws_realtime 
                WHERE station_name IN %s ORDER BY station_name""", (query_list,))
    query_results = query_database(query[0], query[1])["data"]
    return JSONResponse(content=query_results)


##@app.get("/realtime/daily-maxmin/{variable}")
##def daily_maxmin_endpoint(variable: str = None) -> JSONResponse:
##    query = ("""SELECT station_name, TO_CHAR(date, 'YYYY-MM-DD'), TO_CHAR(time, 'HH24:MI:SS'),
##                variable, datapoint
##                FROM aws_realtime_aggregate
##                WHERE variable=%s ORDER BY variable DESC""", (variable,))
##    query_results = query_database(query[0], query[1])["data"]
##    maximum, minimum = query_results
##    daily_aggregates = {"max":maximum,"min":minimum}
##    return JSONResponse(content=daily_aggregates)


                #######################################
                ###  HISTORICAL AWS DATA  : `/aws`  ###
                #######################################

@app.get("/aws/list")
def list_stations_and_years_endpoint() -> JSONResponse:
    stations_list_query = "SELECT DISTINCT(station_name) FROM aws_10min ORDER BY station_name"
    years_list_query = "SELECT DISTINCT(date_part('year', date)) as date FROM aws_10min ORDER BY date"
    stations = query_database(stations_list_query)
    years = query_database(years_list_query)
    data = {
        "stations": stations["data"],
        "years": years["data"]
    }
    return JSONResponse(content=data)


@app.get("/aws/list/stations={stations}")
def list_station_years_endpoint(stations: str) -> JSONResponse:
    station_list = tuple(station.replace('%20', ' ') for station in stations.split(','))
    query = ("""SELECT DISTINCT(date_part('year', date)) as date
             FROM aws_10min WHERE station_name IN %s ORDER BY date""", (station_list,))
    data = query_database(query[0], query[1])["data"]
    return JSONResponse(content=data)


@app.get("/aws/list/years={years}")
def list_yearly_stations_endpoint(years: str) -> JSONResponse:
    years_list = tuple(year for year in years.split(','))
    query = ("""SELECT DISTINCT(station_name)
             FROM aws_10min WHERE date_part('year', date) IN %s ORDER BY station_name""", (years_list,))
    data = query_database(query[0], query[1])["data"]
    return JSONResponse(content=data)


@app.get("/aws/data")
def query_data_endpoint(query_type: str = "all",        ## Default: return 'all' datapoints
                        stations: str = None,
                        interval: int = 2400,           ## Default: daily intervals
                        startdate: str = "19000101",    ## Default to 'any' time period
                        enddate: str = "99991231",
                        variable: str = None,
                        grouping: str = None,
                        download: bool = False) -> JSONResponse or StreamingResponse:    ## Default: Display data
    input_error = verify_input(query_type, stations, variable, grouping)
    if input_error:
        return JSONResponse({'error': input_error})

    stations = tuple(station.replace('%20', ' ') for station in stations.split(','))
    startdate = datetime.strptime(startdate, '%Y') if len(startdate) == 4 else datetime.strptime(startdate.replace('-',''), '%Y%m%d')
    enddate = datetime.strptime(enddate, '%Y') if len(enddate) == 4 else datetime.strptime(enddate.replace('-',''), '%Y%m%d')
    query, params = generate_query(query_type, stations, interval, startdate, enddate,
                                   variable, grouping, download)
    data = query_database(query, params)
    if download:
        csv_stream = serve_csv(data, startdate, enddate)
        return csv_stream
    return JSONResponse(content=data)
