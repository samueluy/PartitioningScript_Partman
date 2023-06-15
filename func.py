# creates a new table named new_table by replicating the structure of source_table
import psycopg2
from psycopg2 import sql


def replicate_table(conn, source_table, new_table, partition_column):
    try:
        cur = conn.cursor()  # creating a cursor
        cur.execute(
            sql.SQL(
                """
            CREATE TABLE {new_table}(
            LIKE {source_table}
            ) PARTITION BY RANGE ({partition_column});
        """
            ).format(
                new_table=sql.SQL(new_table),
                source_table=sql.SQL(source_table),
                partition_column=sql.SQL(partition_column),
            )
        )
        conn.commit()
        print("Success replicating table")
    except Exception as e:
        print("Error replicating table structure: ", e)


# replicates the data from source_table to new_table
def dump_data(conn, source_table, new_table):
    try:
        cur = conn.cursor()  # creating a cursor
        cur.execute(
            sql.SQL(
                """
            INSERT INTO {new_table}
            SELECT *
            FROM {source_table};
        """
            ).format(new_table=sql.SQL(new_table), source_table=sql.SQL(source_table))
        )
        conn.commit()
        print("Success dumping data")
    except Exception as e:
        print("Error dumping data: ", e)


def create_parent(conn, parent_table, control, interval, premake, start_partition):
    try:
        cur = conn.cursor()  # creating a cursor
        cur.execute(
            sql.SQL(
                """
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
                parent_table=sql.SQL(parent_table),
                control=sql.SQL(control),
                interval=sql.SQL(interval),
                start_partition=sql.SQL(start_partition),
            ),
            (premake,),
        )
        conn.commit()
        print("Success creating parent")

    except Exception as e:
        print("Error creating parent: ", e)


def rename_table(conn, source_table, new_name):
    try:
        cur = conn.cursor()  # creating a cursor
        cur.execute(
            sql.SQL(
                """
            ALTER TABLE {source_table}
            RENAME TO {new_name};
        """
            ).format(source_table=sql.SQL(source_table), new_name=sql.SQL(new_name))
        )
        conn.commit()
        print("Success renaming table")
    except Exception as e:
        print("Error renaming table: ", e)


def drop_table(conn, source_table):
    try:
        cur = conn.cursor()  # creating a cursor
        cur.execute(
            sql.SQL(
                """
            DROP TABLE {source_table};
        """
            ).format(source_table=sql.SQL(source_table))
        )
        conn.commit()
        print("Success dropping table")
    except Exception as e:
        print("Error dropping table: ", e)


def get_tables(conn):
    try:
        cur = conn.cursor()
        cur.execute(
            """
        SELECT * FROM pg_tables WHERE schemaname = 'rec'
        """
        )
        for table in cur.fetchall():
            print(table[1])

    except Exception as e:
        print("Error retrieving tables: ", e)


def partition(conn, source_table, partition_column, interval, premake, start_partition):
    raw_name = source_table.split(".")[-1]  # split '.' to get name without schema
    raw_temp_name = raw_name + "_temp"
    temp_name = source_table + "_temp"

    rename_table(conn, source_table, raw_temp_name)
    replicate_table(
        conn, temp_name, source_table, partition_column
    )  # replicate table structure with original table name
    create_parent(
        conn, source_table, partition_column, interval, premake, start_partition
    )
    dump_data(conn, temp_name, source_table)
    drop_table(conn, temp_name)