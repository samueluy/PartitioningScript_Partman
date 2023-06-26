import psycopg2
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

    # partition(conn, source_table, partition_column, interval, premake, start_partition)
    try:
        file = open('tables.txt', 'r')
        for table in file.readlines():
            partition(conn, table, 'created_date', 'monthly', 1, "now() - interval '16 months'")

    except Exception as e:
        print("Error opening file: ", file)

except Exception as e:
    print("Database not connected successfully: ", e)
