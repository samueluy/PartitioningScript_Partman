import psycopg2
from psycopg2 import sql

DB_NAME = ""
DB_USER = ""
DB_PASS = ""
DB_HOST = ""
DB_PORT = ""

# creates a new table named new_table by replicating the structure of source_table 
def replicate_table(conn, source_table, new_table, partition_column):
    try:
        cur = conn.cursor() # creating a cursor
        cur.execute(sql.SQL("""
            CREATE TABLE {new_table}(
            LIKE {source_table}
            ) PARTITION BY RANGE ({partition_column});
        """
        ).format(
            new_table = sql.SQL(new_table),
            source_table = sql.SQL(source_table),
            partition_column = sql.SQL(partition_column)
            )
        )

        conn.commit()
        print("Success replicating table")
    except Exception as e:
        print("Error replicating table structure: ", e)

# replicates the data from source_table to new_table
def dump_data(conn, source_table, new_table):
    try:
        cur = conn.cursor() # creating a cursor
        cur.execute(sql.SQL("""
            INSERT INTO {new_table}
            SELECT *
            FROM {source_table};
        """
        ).format(
            new_table = sql.SQL(new_table),
            source_table = sql.SQL(source_table)
            )
        )
        conn.commit()
        print("Success dumping data")
    except Exception as e:
        print("Error dumping data: ", e)

def create_parent(conn, parent_table, control, interval, premake, start_partition):
    try:
        cur = conn.cursor() # creating a cursor
        cur.execute(sql.SQL("""
            SELECT partman.create_parent(
                p_parent_table => '{parent_table}',
                p_control => '{control}',
                p_type => 'native',
                p_interval => '{interval}',
                p_premake => %s,
                p_start_partition => ({start_partition})::text
            );
        """
        ).format(
            parent_table = sql.SQL(parent_table),
            control = sql.SQL(control),
            interval = sql.SQL(interval),
            start_partition = sql.SQL(start_partition),
            ), (premake,)
        )
        conn.commit()
        print("Success creating parent")

    except Exception as e:
        print("Error creating parent: ", e)

# Connect to the db
try:
    conn = psycopg2.connect(database=DB_NAME,
                            user=DB_USER,
                            password=DB_PASS,
                            host=DB_HOST,
                            port=DB_PORT)
    print("Database connected successfully")
    
except Exception as e:
    print("Database not connected successfully")
    print(e)