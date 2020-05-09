# AutoSQL - SQL insertions automated and simplified

[![NPM](https://nodei.co/npm/autosql.png)](https://nodei.co/npm/autosql/)


## Simplify inserts through AutoSQL

AutoSQL is designed to help automate data insertions by: 
- predicting data types of each column
- predicting primary keys / unique indexes / useful indexes (mainly date/time fields)
- creating the target schema or table (if needed)
- altering the target table to handle the newly provided data (if needed)
    -- such as changing length of columns
    -- allowing null values to be inserted
- separating data into manageable chunks
    -- limiting number of rows being inserted at once
    -- keep insert queries below maximum insert size of server
- catching special characters and converting data to conform with the requirements for the SQL dialect being used
    -- e.g. special characters such as ' is changed to '' 
    -- boolean values provided are converted to tinyint equivalents for mysql
Built to be additive and not destructive, changes made by this repository on a database/table (if allowed via config) should not affect existing data by only allowing increases in length / new null columns etc

To simply insert data - provide a config object and an array of JSON Objects to be inserted
```js
const autosql = require('autosql');
var insert_data = await autosql.auto_sql(config, data).catch((err) => {
    console.log(err)
    })
```

This repository and documentation are still in development.
If you have any feedback, please contact me via email at w@walterchoi.com

## Table of contents

- [Supported languages and Dependencies](#Supported-languages-and-Dependencies)
- [Configuration and defaults](#Configuration-and-defaults)
- [Convenience methods](#Convenience-methods)

---


## Supported languages and Dependencies

Currently AutoSQL only supports MySQL and pgSQL.
To support these SQL dialects, this repository has two optional dependencies
- mysql2 (https://www.npmjs.com/package/mysql2)
- pg (https://www.npmjs.com/package/pg)


---


## Configuration and defaults

The configuration variable must be an object and an example (with all the bells and whistles) can be seen below:

Many aspects of this configuration are optional and defaults for this can be found at ./config/defaults.json

```js 
CONFIGURATION = {
    // REQUIRED SECTION
    // HANDLES SQL connection to run queries/insertions
    "host": [REQUIRED STRING],
    "username": [REQUIRED STRING],
    "password": [REQUIRED STRING],
    "database": [REQUIRED STRING],
    "table": [REQUIRED STRING],
    "sql_dialect": [REQUIRED STRING], // currently can only be "mysql" or "pgsql"

    // OPTIONAL SECTION
    "meta_data": [OPTIONAL ARRAY -- if none is provided, one will be created automatically],
    "primary": [OPTIONAL ARRAY],

    // OPTIONAL SETTINGS
    "max_key_length": [OPTIONAL NUMBER],
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

    // 
}
```

<details>
<summary>meta_data - is a list of each column to be inserted and is an array of objects</summary>

    ```js
    [
        {
            COLUMN_1: {
            type: 'datetime',
            length: 0,
            allowNull: true,
            unique: false,
            index: true,
            pseudounique: false,
            primary: false,
            auto_increment: false,
            default: "CURRENT_TIMESTAMP",
            decimal: 0
            }
        },
        {
            COLUMN_2: {
            type: 'varchar',
            length: 8,
            allowNull: false,
            unique: true,
            index: true,
            pseudounique: true,
            primary: true,
            auto_increment: false,
            default: undefined,
            decimal: 0
            }
        }
    ]
    ```

</details>  

<details>
<summary>primary - is an optional array, listing column names used for the primary key</summary>

EXAMPLE: 

    ```js
    config.primary = ["column_1", "column_2"]
    ```

DEFAULTS TO:

    ```js
    config.primary = ["ID"]
    ```


</details>  

<details>
<summary>The remaining optional settings change small aspects of how this repository affects the data insertion</summary>

 - minimum_unique: changes the minimum number of rows needed to identify a column as unique
    -- defaults to 50
 - pseudo_unique: changes the percentage of rows that are unique to be considered to be pseudo_unique
    -- defaults to 0.95 (95% | two standard deviations)

 - sampling: option to only check/sample a percentage of all data provided. Provided a float between 0 and 1, this will then select a number of random rows to use in finding data types/lengths/uniqueness etc
    -- defaults to 0 (or off/sample everything)
        --- if you are inserting 1000 rows and sampling is set to 0.5, 500 random rows will be selected and used for checks
 - sampling_minimum: minimum number of data required for sampling to be enabled
    -- defaults to 100 
        --- if provided less than X rows or if sampling is set to a % where the selected number of sampled rows would be less than this row count, disables sampling
    
 - max_key_length: maximum key length - used for preventing unique long-text fields from being included in an automatically predicted primary key
    -- defaults to 255
 - auto_indexing: toggles the prediction and creation of indexes
    -- defaults to true
 - auto_id: toggles the creation of an auto_incremental ID column - if an ID column is also provided, will not have any action
    -- defaults to false

 - insert_type: changes action of insert on duplicate key error
    -- defaults to "REPLACE"
        --- available options: 
            ---- "REPLACE" - replace/update all non-primary-key columns
            ---- "IGNORE" - ignore and do not replace/update
    
 - collation: collation of the databases/tables to use on creation
    -- defaults to "utf8mb4_unicode_ci"

 - max_insert: maximum number of rows to insert per query
    -- defaults to 5000
 - max_insert_size: maximum amount of data (bytes) to attempt to insert per query
    -- defaults to 1048576 (default max-allowed-packet for MySQL servers)
 - insert_stack: minimum number of rows to stack up per query
    -- defaults to 100
        --- e.g. if provided 6000 rows of data and at row 4444 the data being sent would exceed max_insert_size, the data will be split into two stacks (4400 and 1600) to be inserted as separate queries

 - safe_mode: toggles the usage of transactions, rollback on any single error and commit only on no errors
    -- defaults to true

 - wait_for_approval: 
    -- defaults to false
    locale: en-US,
    timezone: UTC,
    convert_timezone: true
</p>
</details>  

<details>
<summary>These configuration options (included within the defaults.json file) are not yet used and are included for planned future features</summary>

 - wait_for_approval: before any change to table structure - output changes and wait for approval
    -- defaults to false

 - convert_timezone: convert all datetime values (with timezone) to a specific timezone using Date.prototype.toLocaleString()
    -- defaults to true
 - convert_all_timezone: convert all datetime values (even if no timezone is provided - assuming UTC) to a specific timezone using Date.prototype.toLocaleString()
    -- defaults to false
 - locale: sets the output format used for Date.prototype.toLocaleString()
    -- defaults to "en-US"
 - timezone: sets the output timezone used for Date.prototype.toLocaleString()
    -- defaults to "UTC"
</p>
</details>  

[back to top](#table-of-contents)


---


## Convenience methods

Currently AutoSQL exposes a number of functions to help automate certain aspects of the data insertion process.
auto_sql (automatic insertion) relies on each of these used in conjunction however there are cases where separating out these functions may be useful.

 - ***auto_sql***
    -- runs each of these other functions in conjunction to automatically insert provided data
    -- in order of operation:
        --- get_meta_data
        --- auto_configure_table
        --- insert_data
 - **get_meta_data**
    -- when provided data, this function uses predict_type, collate_types, get_headers, initialize_meta_data, predict_indexes to create a meta_data object (for more information on the meta_data object please check (#Configuration and defaults))
 - predict_type
    -- when provided a single data point, predicts the type of data that has been provided.
    -- relies on regex (./helpers/regex.js)
 - collate_types
    -- when provided two types of data, compares the two types provided to determine the additive column type that should be able to handle both data sets.
 - get_headers
    -- when provided data - creates an array of column names
 - initialize_meta_data
    -- creates an initial config.meta_data object from provided headers
 - predict_indexes
    -- when provided the meta_data object (or list of columns with types, lengths, unique-ness, nullability), provides a list of columns that should be combined into a primary key, list of unique columns and probable index columns
 - **auto_configure_table**
    -- checks existence of tables/databases and creates them if they do not exist (using auto_create_table) or alters them if they do exist (using auto_alter_table)
 - auto_alter_table
    -- when provided with meta_data, checks the existing table to determine changes required to allow this new data set to be inserted
 - auto_create_table
    -- when provided with meta_data, creates (if does not exist) a table that would allow this data set to be inserted
 - **insert_data**
    -- when provided with data, creates a set of insert statements and runs them
    -- returns the number of rows affected
 - validate_database
    -- attempt to connect to the provided database connection and run 'SELECT 1 as SOLUTION'
 - validate_query
    -- attempt to connect to the provided database connection and run 'EXPLAIN ' + provided SQL query
 - run_sql_query
    -- runs a provided SQL query on the provided database connection


---

[back to top](#table-of-contents)