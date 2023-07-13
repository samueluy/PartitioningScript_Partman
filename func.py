import psycopg2
from psycopg2 import sql
import os

def replicate_table(conn, source_table, new_table, partition_column):
    print("Replicating table: " + source_table + "...")
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
        conn.rollback()

def create_view(conn, view_definitions):
    print("Creating views...")
    try:
        cur = conn.cursor()  # creating a cursor
        for view_name, view_definition in view_definitions:
            print("Creating: " + view_name)
            create_view_statement = "CREATE VIEW {view_name} AS {view_definition};"
            cur.execute(sql.SQL(create_view_statement).format(
                view_definition=sql.SQL(view_definition),
                view_name=sql.SQL(view_name))
            )

        conn.commit()
        print("Success creating view")
    except Exception as e:
        print("Error creating view: ", e)
        conn.rollback()


# replicates the data from source_table to new_table
def dump_data(conn, source_table, new_table):
    print("Dumping data from: " + source_table + " to: " + new_table + "...")
    try:
        cur = conn.cursor()
        batch_size = 10000
        current_offset = 0
        current_batch = 0
        
        # Get the total number of rows
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.SQL(source_table)))
        total_rows = cur.fetchone()[0]
        # Loop until all rows are processed
        while current_offset < total_rows:
            # Increment the current batch
            current_batch += 1
            
            # Dump the current batch using the COPY command
            copy_query = sql.SQL("COPY (SELECT * FROM {} OFFSET %s LIMIT %s) TO {}").format(
                sql.SQL(source_table), sql.SQL(new_table)
            )
            cur.execute(copy_query, (current_offset, batch_size))
            
            # Increment the offset for the next batch
            current_offset += batch_size
        
        conn.commit()
        print("Success dumping data")
    except Exception as e:
        print("Error dumping data: ", e)
        conn.rollback()


def create_parent(conn, parent_table, control, interval, premake, start_partition):
    print("Creating parent...")
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
        conn.rollback()

def rename_table(conn, source_table, new_name):
    print("Renaming " + source_table + " to: " + new_name + "...")
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
        conn.rollback()


def drop_table(conn, source_table):
    print("Dropping: " + source_table + "...")
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
        conn.rollback()

def drop_table_cascade(conn, source_table):
    print("Dropping cascade: " + source_table + "...")
    try:
        cur = conn.cursor()  # creating a cursor
        cur.execute(
            sql.SQL(
                """
            DROP TABLE {source_table} CASCADE;
        """
            ).format(source_table=sql.SQL(source_table))
        )
        conn.commit()
        print("Success dropping table")
    except Exception as e:
        print("Error dropping table: ", e)
        conn.rollback()

def drop_views(conn, schema):
    print("Dropping all views...")
    try:
        cur = conn.cursor() # creating a cursor
        cur.execute(sql.SQL("""
    SELECT 'DROP VIEW IF EXISTS {schema}.' || table_name || ' CASCADE;'
    FROM information_schema.views
    WHERE table_schema = '{schema}';
    """).format(schema=sql.SQL(schema)))
        
        drop_statements = cur.fetchall()
        for drop_statement in drop_statements:
            try:
                print("Dropping: " + drop_statement[0])
                cur.execute(sql.SQL(drop_statement[0]))
                
            except Exception as e:
                print("Error dropping view:", e)
        conn.commit()
    except Exception as e:
        print("Error dropping views: ", e)
        conn.rollback()



def ret_view_def(conn, schema):
    print("Retrieving view definitions...")
    try:
        cur = conn.cursor()  # creating a cursor
        cur.execute(
            sql.SQL(
                """
        SELECT table_name, view_definition
        FROM information_schema.views
        WHERE table_schema = '{schema}';
        """
            ).format(schema=sql.SQL(schema))
        )
        view_definitions = cur.fetchall()
        conn.commit()
        return view_definitions
    except Exception as e:
        print("Error dropping view: ", e)
        conn.rollback()

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
        conn.rollback()

def get_dependencies(conn, source_table):
    print("Getting dependencies of table " + source_table + ":")
    tables = []
    try:
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
        """
SELECT DISTINCT v.oid::regclass AS view
FROM pg_depend AS d      -- objects that depend on the table
   JOIN pg_rewrite AS r  -- rules depending on the table
      ON r.oid = d.objid
   JOIN pg_class AS v    -- views for the rules
      ON v.oid = r.ev_class
WHERE v.relkind = 'v'    -- only interested in views
  AND d.classid = 'pg_rewrite'::regclass
  AND d.refclassid = 'pg_class'::regclass
  AND d.deptype = 'n'    -- normal dependency
  AND d.refobjid = '{source_table}'::regclass;
        """
            ).format(source_table=sql.SQL(source_table))
        )
        for table in cur.fetchall():
            print(table[0])
            tables.append(table[0])
        return tables

    except Exception as e:
        print("Error retrieving dependencies: ", e)
        return tables

def partition(conn, source_table, partition_column, interval, premake, start_partition):
    os.environ['PGOPTIONS'] = '-c statement_timeout=0'
    source_table = source_table.strip() # remove leading/trailing whitespace
    raw_name = source_table.split(".")[-1]  # split '.' to get name without schema
    raw_temp_name = raw_name + "_temp"
    temp_name = source_table + "_temp"
    source_c13mos = source_table +"_c3mos"
    source_h13mos = source_table + "_h13mos"
    rename_table(conn, source_table, raw_temp_name)
    replicate_table(
        conn, temp_name, source_table, partition_column
    )  # replicate table structure with original table name
    create_parent(
        conn, source_table, partition_column, interval, premake, start_partition
    )
    dump_data(conn, temp_name, source_table)
    get_dependencies(conn, temp_name)
    view_definitions = ret_view_def(conn, 'rec')
    drop_views(conn, 'rec')
#   Drop table dependencies
    drop_table_cascade(conn, source_c13mos)
    drop_table_cascade(conn, source_h13mos)
#   Drop temp table (original)
    drop_table_cascade(conn, temp_name)
    create_view(conn, view_definitions)