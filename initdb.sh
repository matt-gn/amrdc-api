#!/bin/sh

psql -U ${POSTGRES_USER} <<-END

    ALTER SYSTEM SET work_mem = '128MB';

    CREATE TABLE IF NOT EXISTS aws_10min (
        station_name VARCHAR(18),
        date DATE,
        time TIME,
        temperature REAL,
        pressure REAL,
        wind_speed REAL,
        wind_direction REAL,
        humidity REAL,
        delta_t REAL
    );

    CREATE TABLE IF NOT EXISTS aws_10min_last_update (
        last_update TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS aws_realtime (
        station_name VARCHAR(18),
        date DATE,
        time TIME,
        temperature REAL,
        pressure REAL,
        wind_speed REAL,
        wind_direction REAL,
        humidity REAL,
        region VARCHAR(24)
    );

    CREATE USER ${CLIENT_USER} WITH PASSWORD ${CLIENT_PASSWORD};
    GRANT CONNECT ON DATABASE amrdc_api TO ${CLIENT_USER};
    GRANT SELECT ON aws_10min TO ${CLIENT_USER};
    GRANT SELECT ON aws_realtime TO ${CLIENT_USER};

END
