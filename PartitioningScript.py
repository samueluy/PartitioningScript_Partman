import psycopg2
import os
from func import *

os.environ["PGOPTIONS"] = "-c statement_timeout=0"  # remove timeout for large db dump

DB_NAME = ""
DB_USER = ""
DB_PASS = ""
DB_HOST = ""
DB_PORT = ""  # can be left blank

# Connect to the db
try:
    conn = psycopg2.connect(
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        connect_timeout=0,
    )
    print("Database connected successfully")

    # partition(conn, source_table, partition_column, interval, premake, start_partition)

    # sample run. partition_column is the column to partition on, interval is the interval to partition by, premake is the number of partitions to create, and start_partition is the date to start partitioning from
    try:
        file = open("tables.txt", "r")
        for table in file.readlines():
            print(
                "-------------------------------"
                + table
                + "-------------------------------"
            )
            partition(
                conn,
                table,
                "created_date",
                "monthly",
                1,
                "now() - interval '16 months'",
            )

    except Exception as e:
        print("Error opening file: ", file)
    # end sample run

except Exception as e:
    print("Database not connected successfully: ", e)
