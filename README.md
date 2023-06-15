# Code Readme

The code demonstrates the usage of the `psycopg2` library to interact with a PostgreSQL database. It includes functions to replicate table structure, dump data from one table to another, and create a parent table using the `partman` extension.

## Prerequisites

Before running the code, ensure that the following prerequisites are met:
- PostgreSQL database is installed and running.
- `psycopg2` library is installed. You can install it using `pip install psycopg2`.
- wheel error, download missing dependencies:
```
pip install cmake
sudo apt-get install gcc libpq-dev -y
sudo apt-get install python-dev  python-pip -y
sudo apt-get install python3-dev python3-pip python3-venv python3-wheel -y
```

## Configuration

The code requires the following configuration variables to be set before running:
- `DB_NAME`: The name of the target database.
- `DB_USER`: The username to connect to the database.
- `DB_PASS`: The password for the database user.
- `DB_HOST`: The host address of the database.
- `DB_PORT`: The port number for the database.

Make sure to provide the correct values for these variables.

## Functionality

The code provides the following functions:

### 1. `replicate_table(conn, source_table, new_table, partition_column)`

This function creates a new table named `new_table` by replicating the structure of `source_table`. The `partition_column` parameter specifies the column used for partitioning the table. If successful, it prints "Success replicating table." Otherwise, it prints the encountered error.

### 2. `dump_data(conn, source_table, new_table)`

This function replicates the data from `source_table` to `new_table`. It inserts all the records from the source table into the new table. If successful, it prints "Success dumping data." Otherwise, it prints the encountered error.

### 3. `create_parent(conn, parent_table, control, interval, premake, start_partition)`

This function creates a parent table using the `partman` extension. The `parent_table` parameter specifies the name of the parent table to be created. The `control` parameter specifies the name of the control table. The `interval` parameter determines the interval for automatic partition creation. The `premake` parameter specifies whether to create the partitions in advance. The `start_partition` parameter specifies the starting partition. If successful, it prints "Success creating parent." Otherwise, it prints the encountered error.

### Connecting to the Database

The code connects to the PostgreSQL database using the provided configuration variables. If the connection is successful, it prints "Database connected successfully." Otherwise, it prints "Database not connected successfully" along with the encountered error.

Make sure to provide valid configuration values and handle any errors that may occur during the execution of the code.
