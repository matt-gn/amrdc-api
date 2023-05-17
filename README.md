# amrdc-api

## Installation

- Copy `.env.sample` to `.env` and supply passwords for main user and (read-only) client.
- `docker compose build` followed by `docker compose up -d`.
- The API application is mapped to port 8000 on host machine by default. You can change the port on the host machine in `docker-compose.yml` via the `ports` variable.
- The database build process takes a long time. It will only rebuild when the application detects an update to the data repo.

## API endpoints

### AWS Data query: `/aws/data`
Parameters
query_type: str (default="all"), stations: str, interval: int (default=2400), startdate: int (default=any), enddate: int (default=any), variable: str, grouping: str, download: bool (default=False)

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

Responses are limited to 10k lines.

### List AWS stations & years: `/aws/list`
Parameters
None

Returns a JSON object with a list of AWS stations and years with available data.

### List years per AWS station(s): `/aws/list/stations={stations}`
Parameters
stations: str

Returns a JSON object with a list of years covered by the supplied station list.

`stations` accepts a comma-separated list of AWS station names.

### List AWS stations per year(s): `/aws/list/years={years}`
Parameters
years: str

Returns a JSON object with a list of stations with available data within the span of any of the supplied years.

`years` accepts a comma-separated list of years.

### Realtime data per station(s): `/realtime/station/{stations}`
Parameters
stations: str

Returns the most recent datapoint for each supplied station.

`stations` accepts a comma-separated list of AWS station names.

### Realtime max/min readings: `/realtime/maxmin/{variable}`
Parameters
variable: str

Returns the current maximum and minumum datapoints from all realtime data.

Valid entries:
`variable`: "temperature", "pressure", "wind_speed", "wind_direction", "humidity", "delta_t"

### Daily max/min readings: `/realtime/daily-maxmin/{variable}`
Parameters
variable: str

Returns the daily maximum and minumum datapoints from all realtime data.

Valid entries:
`variable`: "temperature", "pressure", "wind_speed", "wind_direction", "humidity", "delta_t"

### Realtime station list: `/realtime/station_list`
Parameters
None

Returns a JSON object with a list of realtime AWS stations with available data.
