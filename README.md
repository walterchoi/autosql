# Lazy_SQL
Documentation coming soon.

NodeJS repository to automate inserts from JSON data into SQL
Current support only for MySQL and PostgreSQL

This repository aims to help automate commonly used SQL processes.

The primary function provided is "auto_sql" which should be provided a configuration variable and a data variable.
The configuration variable must be an object and an example (with all the bells and whistles) can be seen below:
CONFIGURATION = {
    "host": [REQUIRED STRING],
    "username": [REQUIRED STRING],
    "password": [REQUIRED STRING],
    "database": [REQUIRED STRING],
    "table": [REQUIRED STRING],
    "sql_dialect": [REQUIRED STRING],
    "metaData": [OPTIONAL ARRAY -- if none is provided, one will be created automatically],
    "headers": [OPTIONAL ARRAY -- if none is provided, one will be created automatically],
    "max_key_length": [OPTIONAL NUMBER],
    "primary": [OPTIONAL ARRAY],
    "auto_id": [OPTIONAL BOOLEAN],
    "sampling": [OPTIONAL NUMBER],
    "sampling_minimum": [OPTIONAL NUMBER],
    "minimum_unique": [OPTIONAL NUMBER],
    "pseudo_unique": [OPTIONAL NUMBER],
    "collation": [OPTIONAL STRING],
    "create_table": [OPTIONAL BOOLEAN],
    "insert_type": [OPTIONAL STRING],
    "safe_mode": [OPTIONAL BOOLEAN],
    "max_insert": [OPTIONAL NUMBER],
    "insert_stack": [OPTIONAL NUMBER],
    "max_insert_size": [OPTIONAL NUMBER]
}

Each of these options will be explained below. (in a future update)

This auto_sql function will call the other functions in order to automatically create (if needed -- and if 'create_table' boolean is set to true) and automatically alter (if needed) to allow insertion of data.
This is done after parsing (some or all) of the provided JSON data to identify for each column what type of data, length of data, nullable status and index status.
Then the data is divided into stacks (if needed) and each insert/replace statement will be divided into these limits of both max number of rows and max size of insert/replace.
This insert/replace statement is also passed through as a TRANSACTION with automatic ROLLBACK on single failure if safe_mode has been set to true

Life of available functions: 
predict_type,
collate_types,
get_headers,
initialize_meta_data,
get_meta_data,
predict_indexes,
auto_alter_table,
auto_create_table,
auto_sql,
auto_configure_table,
insert_data,
validate_database,
validate_query