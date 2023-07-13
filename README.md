The following code aims to automate table partitioning on a PostgreSQL database given a list of tables using psycopg.

## Prerequisites

Before running the code, ensure that the following prerequisites are met:
- PostgreSQL database is installed and running.
- `psycopg2` library is installed. You can install it using `pip install psycopg2`.
- clone `PartitioningScript_Partman` repository
```git clone https://github.com/samueluy/PartitioningScript_Partman```
- wheel error, download missing dependencies:
```
pip install cmake
sudo apt-get install gcc libpq-dev -y
sudo apt-get install python-dev  python-pip -y
sudo apt-get install python3-dev python3-pip python3-venv python3-wheel -y
```

## Configuration

### PartitioningScript.py
The code requires the following configuration variables to be set before running:
- `DB_NAME` (str): The name of the target database.
- `DB_USER` (str): The username to connect to the database.
- `DB_PASS` (str): The password for the database user.
- `DB_HOST` (str): The host address of the database.
- `DB_PORT` (str): The port number for the database.



The `partition` function accepts the following parameters:
- `conn` (psycopg2.extensions.connection): The connection object representing the connection to the PostgreSQL database.
- `source_table` (str): The name of the source table that needs to be partitioned.
- `partition_column` (str): The column of the `source_table` used for partitioning.
- `interval` (str): The interval for partitioning. Valid values include "monthly", "weekly", "daily", etc.
- `premake` (int): The number of partitions to create in advance.
- `start_partition` (str): The starting date or timestamp for partitioning. It specifies the date from which the partitions should be created.

Make sure to provide the correct values for these variables.

### tables.txt
Add here the list of tables to be partitioned. Each line is a new table.

## Functions

### `replicate_table()`

Replicates the structure of `source_table` to `new_table`.

- **`conn`** (psycopg2.extensions.connection): The connection object representing the connection to the PostgreSQL database.
- **`source_table`** (str): The name of the source table.
- **`new_table`** (str): The name of the new table.
- **`partition_column`** (str): The column used for partitioning.

### `create_view()`

Creates views in the `view_definitions` list.

- **`conn`** (psycopg2.extensions.connection): The connection object representing the connection to the PostgreSQL database.
- **`view_definitions`** (list): A list of tuples containing the view name and its definition.

### `dump_data()`

Dumps the data from `source_table` to `new_table`.

- **`conn`** (psycopg2.extensions.connection): The connection object representing the connection to the PostgreSQL database.
- **`source_table`** (str): The name of the source table.
- **`new_table`** (str): The name of the new table.

### `create_parent()`

Creates a parent table for partitioning.

- **`conn`** (psycopg2.extensions.connection): The connection object representing the connection to the PostgreSQL database.
- **`parent_table`** (str): The name of the parent table.
- **`control`** (str): The control value.
- **`interval`** (str): The interval.
- **`premake`** (int): The number of partitions to create in advance.
- **`start_partition`** (str): The starting partition.

### `rename_table()`

Renames `source_table` to `new_name`.

- **`conn`** (psycopg2.extensions.connection): The connection object representing the connection to the PostgreSQL database.
- **`source_table`** (str): The name of the source table.
- **`new_name`** (str): The new name for the table.

### `drop_table()`

Drops `source_table`.

- **`conn`** (psycopg2.extensions.connection): The connection object representing the connection to the PostgreSQL database.
- **`source_table`** (str): The name of the table to drop.

### `drop_table_cascade()`

Drops `source_table` and all its dependencies.

- **`conn`** (psycopg2.extensions.connection): The connection object representing the connection to the PostgreSQL database.
- **`source_table`** (str): The name of the table to drop.

### `drop_views()`

Drops all views in `schema`.

- **`conn`** (psycopg2.extensions.connection): The connection object representing the connection to the PostgreSQL database.
- **`schema`** (str): The name of the schema.

### `ret_view_def()`

Returns the view definitions in `schema`.

- **`conn`** (psycopg2.extensions.connection): The connection object representing the connection to the PostgreSQL database.
- **`schema`** (str): The name of the schema.

### `get_tables()`

Returns the tables in `schema`.

- **`conn`** (psycopg2.extensions.connection): The connection object representing the connection to the PostgreSQL database.
- **`schema`** (str): The name of the schema.

### `get_dependencies()`

Returns the dependencies of `source_table`.

- **`conn`** (psycopg2.extensions.connection): The connection object representing the connection to the PostgreSQL database.
- **`source_table`** (str): The name of the source table.
