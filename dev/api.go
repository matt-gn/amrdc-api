package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
)

// Database struct, constructor and methods

type DBContext struct {
	db *sql.DBContext
}

func NewDBContext() (*DBContext, error) {
	// TODO Fix this
	db, err := sql.Open("postgres", "postgres://amrdc_api:postgres@5432/amrdc_api")
	if err != nil {
		return nil, err
	}
	return &DBContext{db: db}, nil
}

func (ctx *DBContext) Close() error {
	return ctx.db.Close()
}

func (ctx *DBContext) Query(query string, args ...string) (*sql.Rows, error) {
	return ctx.db.Query(query, args...)
}

func realtime_maxmin_endpoint(c *gin.Context, db *DBContext) map[string][]Row {
	variable := pq.QuoteIdentifier(c.Param("variable"))
	max_q := "SELECT station_name, TO_CHAR(date, 'YYYY-MM-DD'), TO_CHAR(time, 'HH24:MI:SS'), "
	+variable + " FROM aws_realtime ORDER BY " + variable + " DESC LIMIT 1"
	min_q := "SELECT station_name, TO_CHAR(date, 'YYYY-MM-DD'), TO_CHAR(time, 'HH24:MI:SS'), "
	+variable + " FROM aws_realtime ORDER BY " + variable + " ASC LIMIT 1"

	type Row struct {
		station_name string
		date         string
		time         string
		variable     float64
	}

	max := make([]Row, 0)
	results, err := db.Query(max_q)
	if err != nil {
		panic(err)
	}
	defer results.Close()
	for results.Next() {
		newRow := Row{}
		err := results.Scan(&newRow.station_name, &newRow.date, &newRow.time, &newRow.variable)
		if err != nil {
			panic(err)
		}
		max = append(max, newRow)
	}

	min := make([]Row, 0)
	results, err := db.Query(min_q)
	if err != nil {
		panic(err)
	}
	defer results.Close()
	for results.Next() {
		newRow := Row{}
		err := results.Scan(&newRow.station_name, &newRow.date, &newRow.time, &newRow.variable)
		if err != nil {
			panic(err)
		}
		min = append(min, newRow)
	}

	json_response := map[string][]Row{"max": max, "min": min}

	c.JSON(http.StatusOK, json_response)
}

func realtime_list_endpoint(c *gin.Context, db *DBContext) map[string][]string {
	query := "SELECT DISTINCT(station_name), region FROM aws_realtime ORDER BY region, station_name"
	station_map := make(map[string][]string, 0)
	results, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	defer results.Close()
	for results.Next() {
		var station_name string
		var region string
		err := results.Scan(&station_name, &region)
		if err != nil {
			panic(err)
		}
		if _, ok := station_map[region]; !ok {
			station_map[region] = []string{station}
		} else {
			station_map[region] = append(station_map[region], station)
		}
	}
	c.JSON(http.StatusOK, station_map)
}

func realtime_station_data_endpoint(c *gin.Context, db *DBContext) []Row {
	station := c.Param("station")
	query := "SELECT station_name, TO_CHAR(date, 'YYYY-MM-DD') as date, "
	+"TO_CHAR(time, 'HH24:MI:SS') as time, temperature, pressure, "
	+"wind_speed, wind_direction, humidity "
	+"FROM aws_realtime "
	+"WHERE station_name = ? ORDER BY date DESC, time DESC LIMIT 1"

	type Row struct {
		station_name   string
		date           string
		time           string
		temperature    float64
		pressure       float64
		wind_speed     float64
		wind_direction float64
		humidity       float64
	}

	data := make([]Row, 0)

	results, err := db.Query(query, station)
	if err != nil {
		panic(err)
	}
	defer results.Close()

	for results.Next() {
		newRow := Row{}
		err := results.Scan(
			&newRow.station_name,
			&newRow.date,
			&newRow.time,
			&newRow.temperature,
			&newRow.pressure,
			&newRow.wind_speed,
			&newRow.wind_direction,
			&newRow.humidity,
		)
		if err != nil {
			panic(err)
		}
		data = append(data, newRow)
	}

	json_response := map[string]Row{"data": data}
	c.JSON(http.StatusOK, json_response)
}

func aws_stations_and_years_endpoint(c *gin.Context, db *DBContext) {
	stations_list_query := "SELECT DISTINCT(station_name) FROM aws_10min ORDER BY station_name"
	years_list_query := "SELECT DISTINCT(date_part('year', date)::int) as date FROM aws_10min ORDER BY date"

	stations := make([]string, 0)
	results, err := db.Query(stations_list_query)
	if err != nil {
		panic(err)
	}
	defer results.Close()
	for results.Next() {
		var station string
		err := results.Scan(&station)
		if err != nil {
			panic(err)
		}
		stations = append(stations, station)
	}

	years := make([]int, 0)
	results, err := db.Query(years_list_query)
	if err != nil {
		panic(err)
	}
	defer results.Close()
	for results.Next() {
		var year string
		err := results.Scan(&year)
		if err != nil {
			panic(err)
		}
		years = append(years, year)
	}

	data := map[string][]interface{}{
		"stations": stations,
		"years":    years,
	}

	c.JSON(http.StatusOK, data)
}

func aws_station_years_endpoint(c *gin.Context, db *DBContext) {
	user_input := c.Param("stations")
	station_list := strings.Split(user_input, ",")
	query := "SELECT DISTINCT(date_part('year', date)::int) as date "
	+"FROM aws_10min WHERE station_name = ANY($1) ORDER BY date"
	years := make([]int, 0)
	results, err := db.Query(query, station_list)
	if err != nil {
		panic(err)
	}
	defer results.Close()
	for results.Next() {
		var year int
		err := results.Scan(&year)
		if err != nil {
			panic(err)
		}
		years = append(years, year)
	}
	data := map[string][]int{"year": years}
	c.JSON(http.StatusOK, data)
}

func aws_yearly_stations_endpoint(c *gin.Context, db *DBContext) {
	user_input := c.Param("years")
	year_list := strings.Split(user_input, ",")
	query := "SELECT DISTINCT(station_name) "
	+"FROM aws_10min WHERE date_part('year', date) = ANY($1) ORDER BY station_name"
	stations := make([]string, 0)
	results, err := db.Query(query, pq.Array(year_list))
	if err != nil {
		panic(err)
	}
	defer results.Close()
	for results.Next() {
		var station string
		err := results.Scan(&station)
		if err != nil {
			panic(err)
		}
		stations = append(stations, station)
	}
	data := map[string][]string{"stations": stations}
	c.JSON(http.StatusOK, data)
}

func aws_data_endpoint(c *gin.Context, db *DBContext) {
	stations := c.Param("stations")
	interval := c.Param("interval")
	startdate := c.Param("startdate")
	enddate := c.Param("enddate")
	variable := c.Param("variable")
	download := c.Param("download")

	if stations == "" {
		c.JSON(500, gin.H{"Query error": "Query parameter 'stations' required"})
		return
	} else {
		stations_list = pq.Array(strings.Split(stations, ","))
	}
	if startdate == "" {
		startdate := time.Parse("20060102", "19000101")
	} else {
		startdate, err := time.Parse("20060102", startdate)
		if err != nil {
			c.JSON(500, gin.H{"Query error": "Unable to parse start date"})
			return
		}
	}
	if enddate == "" {
		enddate := time.Parse("20060102", "20991231")
	} else {
		enddate, err := time.Parse("20060102", enddate)
		if err != nil {
			c.JSON(500, gin.H{"Query error": "Unable to parse end date"})
			return
		}
	}
	if interval == "" {
		interval := 2400
	}
	if download == "True" {
		limit_statement := 10 * *5
	} else {
		limit_statement := 10 * *4
	}

	switch variable {
	case "temperature", "pressure", "wind_speed", "wind_direction", "humidity", "delta_t":
		query := "SELECT station_name as Name, TO_CHAR(date, 'YYYY-MM-DD') as Date, TO_CHAR(time, 'HH24:MI') as Time, CAST(" + variable + ") as TEXT) "
		+"FROM aws_10min WHERE station_name = ANY($1) AND date >= ? AND date <= ? AND MOD((date_part('hour', time) * 100 + date_part('minute', time))::int, ?) = 0 "
		+"ORDER BY date, time " + limit_statement

		type Row struct {
			station_name string
			date         string
			time         string
			variable     string
		}
		data := make([]Row, 0)
		results, err := db.Query(query, stations_list, startdate, enddate, interval)
		if err != nil {
			panic(err)
		}
		defer results.Close()
		for results.Next() {
			newRow := Row{}
			err := results.Scan(&newRow.station_name, &newRow.date, &newRow.time, &newRow.variable)
			if err != nil {
				panic(err)
			}
			data = append(max, newRow)
		}

	case "":
		query := "SELECT station_name as Name, TO_CHAR(date, 'YYYY-MM-DD') as Date, TO_CHAR(time, 'HH24:MI') as Time, "
		+"CAST(temperature as TEXT), CAST(pressure as TEXT), CAST(wind_speed as TEXT), CAST(wind_direction as TEXT), "
		+"CAST(humidity as TEXT), CAST(delta_t as TEXT) FROM aws_10min "
		+"WHERE station_name = ANY($1) AND date >= ? AND date <= ? AND MOD((date_part('hour', time) * 100 + date_part('minute', time))::int, ?) = 0 "
		+"ORDER BY date, time " + limit_statement

		type Row struct {
			station_name   string
			date           string
			time           string
			temperature    string
			pressure       string
			wind_speed     string
			wind_direction string
			humidity       string
			delta_t        string
		}
		data := make([]Row, 0)
		results, err := db.Query(query, stations_list, startdate, enddate, interval)
		if err != nil {
			panic(err)
		}
		defer results.Close()
		for results.Next() {
			newRow := Row{}
			err := results.Scan(
				&newRow.station_name,
				&newRow.date,
				&newRow.time,
				&newRow.temperature,
				&newRow.pressure,
				&newRow.wind_speed,
				&newRow.wind_direction,
				&newRow.humidity,
				&newRow.delta_t,
			)
			if err != nil {
				panic(err)
			}
			data = append(max, newRow)
		}

	default:
		c.JSON(500, gin.H{"Query error": "Invalid variable supplied"})
		return
	}

	json_response := map[string][]Row{"data": data}
	c.JSON(http.StatusOK, json_response)
}

func aws_data_aggregate_endpoint(c *gin.Context, db *DBContext, agg_type string) {
	stations := c.Param("stations")
	startdate := c.Param("startdate")
	enddate := c.Param("enddate")
	variable := c.Param("variable")
	grouping := c.Param("grouping")
	download := c.Param("download")

	if stations == "" || variable == "" {
		c.JSON(500, gin.H{"Query error": "Query parameters 'stations', 'variable' required"})
		return
	} else {
		stations_list := pq.Array(strings.Split(stations, ","))
	}
	if startdate == "" {
		startdate := time.Parse("20060102", "19000101")
	} else {
		startdate, err := time.Parse("20060102", startdate)
		if err != nil {
			c.JSON(500, gin.H{"Query error": "Unable to parse start date"})
			return
		}
	}
	if enddate == "" {
		enddate := time.Parse("20060102", "20991231")
	} else {
		enddate, err := time.Parse("20060102", enddate)
		if err != nil {
			c.JSON(500, gin.H{"Query error": "Unable to parse end date"})
			return
		}
	}
	if download == "True" {
		limit_statement := 10 * *5
	} else {
		limit_statement := 10 * *4
	}
	switch variable {
	case "temperature", "wind_speed", "wind_direction", "humidity", "pressure", "delta_t":
		column := pq.QuoteIdentifier(variable)
	default:
		c.JSON(500, gin.H{"Query error": "Invalid variable supplied"})
		return
	}
	if agg_type == "max" {
		aggregator := "DESC"
	} else {
		aggregator := "ASC"
	}

	switch grouping {
	case "year", "month", "day":
		query := "SELECT aws.station_name as Name, TO_CHAR(aws.date, 'YYYY-MM-DD') as Date, TO_CHAR(aws.time, 'HH24:MI') as Time, CAST(aws." + column + " as TEXT) "
		+"FROM (SELECT station_name, date, time, " + column + ", ROW_NUMBER() OVER ( PARTITION BY station_name, date_trunc('" + grouping + "', date) "
		+"ORDER BY " + column + " " + aggregator + ") as row_num FROM aws_10min WHERE station_name = ANY($1) AND date >= ? AND  date <= ? AND  " + column + " != 444"
		+") aws WHERE row_num = 1 ORDER BY aws.station_name, date"
	default:
		query := "SELECT aws.station_name as Name, TO_CHAR(aws.date, 'YYYY-MM-DD') as Date, TO_CHAR(aws.time, 'HH24:MI') as Time, CAST(aws." + column + " as TEXT) "
		+"FROM (SELECT station_name, date, time, " + column + ", ROW_NUMBER() OVER ( PARTITION BY station_name ORDER BY " + column + " " + aggregator + " "
		+") as row_num FROM aws_10min WHERE station_name = ANY($1) AND date >= ? AND  date <= ? AND  " + column + " != 444 ) aws WHERE row_num = 1 ORDER BY aws.station_name, date"
	}

	type Row struct {
		station_name string
		date         string
		time         string
		variable     string
	}

	data := make([]Row, 0)

	results, err := db.Query(query, station_list, startdate, enddate)
	if err != nil {
		panic(err)
	}
	defer results.Close()

	for results.Next() {
		newRow := Row{}
		err := results.Scan(
			&newRow.station_name,
			&newRow.date,
			&newRow.time,
			&newRow.variable,
		)
		if err != nil {
			panic(err)
		}
		data = append(data, newRow)
	}

	json_response := map[string]Row{"data": data}
	c.JSON(http.StatusOK, json_response)

}

func main() {
	api := gin.Default()
	db, err := NewDBContext{}
	if err != nil {
		fmt.Println("Failed to connect to database")
		return
	}
	defer db.Close()

	api.Use(gin.Static("/static"))

	// Test endpoint
	api.GET("/test", func(c *gin.Contenxt) {
		timestamp := time.Now().Format("2006-01-02 15:04:05")
		c.JSON(http.StatusOK, gin.H{
			"Status OK": timestamp + "AMRDC Web API is up and running",
		})
	})

	// Realtime data
	api.GET("/realtime/maxmin/:variable", func(c *gin.Context) {
		realtime_maxmin_endpoint(c, db)
	})
	api.GET("/realtime/station_list", func(c *gin.Context) {
		realtime_list_endpoint(c, db)
	})
	api.GET("/realtime/station/:station", func(c *gin.Context) {
		realtime_station_data_endpoint(c, db)
	})

	// Historical AWS data
	api.GET("/aws/list", func(c *gin.Context) {
		aws_stations_and_years_endpoint(c, db)
	})
	api.GET("/aws/list/stations=:stations", func(c *gin.Context) {
		aws_station_years_endpoint(c, db)
	})
	api.GET("/aws/list/years=:years", func(c *gin.Context) {
		aws_yearly_stations_endpoint(c, db)
	})
	api.GET("/aws/data", func(c *gin.Context) {
		aws_data_endpoint(c, db)
	})
	api.GET("/aws/max", func(c *gin.Context) {
		aws_data_aggregate_endpoint(c, db, "max")
	})
	api.GET("/aws/min", func(c *gin.Context) {
		aws_data_aggregate_endpoint(c, db, "min")
	})

	api.Run("localhost:80")
}
