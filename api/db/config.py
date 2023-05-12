from os import environ
import psycopg2

## Set DB credentials
DB_NAME = environ.get("POSTGRES_DB")
DB_USER = environ.get("POSTGRES_USER")
DB_PASSWORD = environ.get("POSTGRES_PASSWORD")
DB_HOST = environ.get("POSTGRES_HOST")
DB_PORT = environ.get("POSTGRES_PORT")
postgres = psycopg2.connect(
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT)
