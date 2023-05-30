package main

import (
	"encoding/json"
	"net/http"
	"time"
)

func extract_resource_list(dataset map[string][]string) [][]string {
	name, _ := Strings.split(dataset["title"], " Automatic Weather Station,")
	resource_list := make([][]string, 0)
	for _, resource := range dataset["resources"] {
		if resource["name"].contains("10min") {
			var resource = []string{name, resource["url"]}
			resource_list = append(resource_list, resource)
		}
	}
	return resource_list
}

func get_resource_urls() (map[string]interface{}, error) {
	api_url := "https://amrdcdata.ssec.wisc.edu/api/action/package_search?q=title:\"quality-controlled+observational+data\"&rows=1000"
	response, err := http.Get(api_url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	datasets := json_loads(response)
	att := "Alexander Tall Tower"
}

func json_loads(data []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}
	for key, value := range result {
		if nestedData, ok := value.(map[string]interface{}); ok {
			nestedResult, err := recursivelyLoadJSON(nestedData)
			if err != nil {
				return nil, err
			}
			result[key] = nestedResult
		}
	}
	return result, nil
}

func process_datapoint(name string, line string) []interface{} {
	row := Strings.split(line)
	date := row[0] + row[2] + row[3]
	formatted_date := time.Parse("20060102", date)
	params := []interface{}{
		name,
		formatted_date,
		row[4],
		row[5],
		row[6],
		row[7],
		row[8],
		row[9]}
	return params
}

func process_datafile(resource []interface{}) ([][]interface{}, error) {
	name, url := resource[0], resource[1]
	formatted_datafile := make([][]interface{}, 0)
	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	//read response

	//process data line by line into
}
