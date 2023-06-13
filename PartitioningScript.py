import psycopg2
from psycopg2 import sql
from func import *

DB_NAME = ""
DB_USER = ""
DB_PASS = ""
DB_HOST = ""
DB_PORT = ""

# Connect to the db
try:
    conn = psycopg2.connect(
        database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )
    print("Database connected successfully")

    # def partition(conn, source_table, partition_column, interval, premake, start_partition)

except Exception as e:
    print("Database not connected successfully")
    print(e)
