# amrdc-api

## Installation

- Copy `.env.sample` to `.env` and supply password for main user.
- `docker compose build` followed by `docker compose up -d`.
- The API application is mapped to port 8000 on host machine by default. You can change the port on the host machine in `docker-compose.yml` via the `ports` variable.
- The database build process takes a long time. It will only rebuild when the application detects an update to the data repo.

## API endpoints

### AWS Data query: `/aws/data`
`query_type: str (default="all"), stations: str, interval: int (default=2400), startdate: int (default=any), enddate: int (default=any), variable: str, grouping: str, download: bool (default=False)`

Returns a JSON object with query results contained in 'header' and 'data' keys.

Valid entries for specific parameters:
`query_type`: "all", "max", "min", "mean"
`interval`: 10 (10min), 100 (hourly), 300 (three-hourly), 2400 (daily @ 0000)
`variable`: "temperature", "pressure", "wind_speed", "wind_direction", "humidity", "delta_t"
`grouping`: "station", "year", "month", "day"

`stations` accepts a comma-separated list of AWS station names.

`variable` and `grouping` are required for "max", "min", and "mean" queries and represent the aggregated column and the grouping applied to the aggregate algorithm, respectively.

`variable` is optional for "all" queries if user wants only one measurement variable.

Setting `download=True` will initiate a streaming object with the requested data.

Responses are limited to 10k rows.

Examples:

```

## All data for Byrd station between 2020/01/01 and 2020/12/31 at 3 hr. intervals
localhost:8000/aws/data?query_type=all&stations=Byrd&interval=300&startdate=20200101&enddate=20201231

## Monthly maximum temperature for Margaret and Nico stations from 2015/01/01 to 2016/12/31
localhost:8000/aws/data?query_type=max&stations=Margaret,Nico&startdate=20150101&enddate=20161231&variable=temperature&grouping=month

## Historical average temperature for AGO-4 and AGO-5
localhost:8000/aws/data?query_type=mean&stations=AGO-4,AGO-5&variable=temperature&grouping=station

```

### List AWS stations & years: `/aws/list`

Returns a JSON object with a list of AWS stations and years with available data.

### List years per AWS station(s): `/aws/list/stations={stations}`
`stations: str`

Returns a JSON object with a list of years covered by the supplied station list.

`stations` accepts a comma-separated list of AWS station names.

### List AWS stations per year(s): `/aws/list/years={years}`
`years: str`

Returns a JSON object with a list of stations with available data within the span of any of the supplied years.

`years` accepts a comma-separated list of years.

### Realtime data per station(s): `/realtime/station/{stations}`
`stations: str`

Returns the most recent datapoint for each supplied station.

`stations` accepts a comma-separated list of AWS station names.

### Realtime max/min readings: `/realtime/maxmin/{variable}`
`variable: str`

Returns the current maximum and minumum datapoints from all realtime data.

Valid entries:
`variable`: "temperature", "pressure", "wind_speed", "wind_direction", "humidity", "delta_t"

### Daily max/min readings: `/realtime/daily-maxmin/{variable}`
`variable: str`

Returns the daily maximum and minumum datapoints from all realtime data.

Valid entries:
`variable`: "temperature", "pressure", "wind_speed", "wind_direction", "humidity", "delta_t"

### Realtime station list: `/realtime/station_list`

Returns a JSON object with a list of realtime AWS stations with available data.
