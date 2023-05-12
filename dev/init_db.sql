"""TODO: Restrict user access, set work_mem"""

CREATE USER amrdc_api WITH PASSWORD 'postgres';
CREATE DATABASE amrdc_api;
GRANT ALL PRIVILEGES ON DATABASE amrdc_api TO amrdc_api;
CREATE USER api_client WITH PASSWORD 'test1234';
CREATE ROLE readaccess;
GRANT CONNECT ON DATABASE amrdc_api TO readaccess;
GRANT USAGE ON SCHEMA public TO readaccess;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readaccess;
GRANT readaccess TO amrdc_api;

CREATE TABLE aws_10min (
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

CREATE TABLE aws_10min_backup (LIKE aws_10min INCLUDING ALL);

CREATE TABLE aws_realtime (
    station_name VARCHAR(18),
    date DATE,
    time TIME,
    temperature REAL,
    pressure REAL,
    wind_speed REAL,
    wind_direction REAL,
    humidity REAL,
    latitude VARCHAR(3),
    longitude VARCHAR(3),
    region VARCHAR(24)
);
