####################################
## API Endpoint Integration tests ##
## ((Must be run after launch!))  ##
####################################

    ## @app.get("/realtime/maxmin/{variable}")
## Normal input
http://localhost:8000/realtime/maxmin/temperature
## Empty input
http://localhost:8000/realtime/maxmin/
## Incorrect input
http://localhost:8000/realtime/maxmin/foobar
http://localhost:8000/realtime/maxmin/20201225
http://localhost:8000/realtime/maxmin/temperature/20220101


    ## @app.get("/realtime/station_list")
## Normal
http://localhost:8000/realtime/station_list
## Passing args where none expected
http://localhost:8000/realtime/station_list?query_type=all
http://localhost:8000/realtime/station_list/2008


    ## @app.get("/realtime/station/{stations}")     ## NOTE Is this correct? is it stations or station (singular)?
## Regular
http://localhost:8000/realtime/station/Byrd,AGO-4,Nico
## Incorrect input
http://localhost:8000/realtime/station/Byrd,Foobar
http://localhost:8000/realtime/station/Byrd,2022
http://localhost:8000/realtime/station/Byrd,Nico/2022
## Empty
http://localhost:8000/realtime/station/


    ## @app.get("/realtime/daily-maxmin/{variable}")
http://localhost:8000/realtime/daily-maxmin/temperature
http://localhost:8000/realtime/daily-maxmin/Foobar
http://localhost:8000/realtime/daily-maxmin/20220101
http://localhost:8000/realtime/daily-maxmin/temperature/Byrd
http://localhost:8000/realtime/daily-maxmin/



    ## @app.get("/aws/list")
http://localhost:8000/aws/list/station_list
## Passing args where none expected
http://localhost:8000/aws/list/station_list?query_type=all
http://localhost:8000/aws/list/station_list/2008


    ## @app.get("/aws/list/stations={stations}")



    ## @app.get("/aws/list/years={years}")




    ## @app.get("/aws/data")
## ALL, w/VARIABLE
http://localhost:8000/aws/data?stations=Byrd&variable=temperature

## ALL, normal
http://localhost:8000/aws/data?stations=Byrd&startdate=20150101&enddate=20161231&query_type=max&grouping=month&variable=temperature

### Max, all stations, group by station
http://localhost:8000/aws/data?query_type=max&stations=all&variable=temperature&grouping=station
http://localhost:8000/aws/data?query_type=min&stations=all&variable=temperature&grouping=year
http://localhost:8000/aws/data?query_type=min&stations=all&variable=temperature&grouping=month&startdate=2012&enddate=2014
http://localhost:8000/aws/data?query_type=all&variable=temperature&grouping=month&startdate=2012&enddate=2014
