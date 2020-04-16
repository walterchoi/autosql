// Using regex, when provided a data point, predict what data type this will be
async function predict_type (data) {
    return new Promise((resolve, reject) => {
        var reg = require('./helpers/regex.js')
        var currentType = null
        if (data.search(reg.boolean) >= 0) {
            currentType = 'boolean'
        } else if (data.search(reg.binary) >= 0) {
            currentType = 'binary'
        } else if (data.search(reg.number) >= 0) {
            currentType = 'int'
        } else if (data.search(reg.decimal) >= 0) {
            currentType = 'decimal'
        } else if (data.search(reg.exponential) >= 0) {
            currentType = 'exponential'
        } else if (data.search(reg.datetime) >= 0) {
            currentType = 'datetime'
        } else if (data.search(reg.date) >= 0) {
            currentType = 'date'
        } else if (data.search(reg.time) >= 0) {
            currentType = 'time'
        } else if (data.search(reg.json) >= 0) {
            currentType = 'json'
        } else {
            currentType = 'varchar'
        }
        if(currentType == 'int') {
            if(data <= 255 || data >= 0) {
                currentType = 'tinyint'
            } else if(data <= 32767 || data >= -32768) {
                currentType = 'smallint'
            } else if(data <= 2147483647 || data >= -2147483648) {
                currentType = 'int'
            } else if(data <= 9223372036854775807 || data >= -9223372036854775808) {
                currentType = 'bigint'
            } else {
                currentType = 'varchar'
            }
        }
        if(currentType == 'json' || currentType == 'varchar') {
            if(data.length < 6553) {
                currentType = currentType
            } else if(data.length >= 6553 && data.length < 65535) {
                currentType = 'text'
            } else if(data.length >= 65535 && data.length < 16777215) {
                currentType = 'mediumtext'
            } else if(data.length >= 16777215 && data.length < 4294967295) {
                currentType = 'longtext'
            } else if(data.length >= 4294967295) {
                currentType = 'longtext'
                reject({
                    err: 'data_too_long',
                    description: 'data is too long for longtext field',
                    data: data,
                    length: data.length
                })
            }
        }
        resolve(currentType)
    })
}

// When provided two types of data, compare both to find the data type that would catch and allow both data types to be entered
async function collate_types (currentType, overallType) {
    return new Promise((resolve, reject) => {
    var collated_type = null
    if(!overallType && !currentType) {
        reject({
            err: 'no data types provided',
            step: 'collate_types',
            description: 'no data types entered into collation',
            data: [overallType, currentType]
        })
    }
    // If there's no equivalent column already existing, default to current type
    if (!overallType) {
        collated_type = currentType
        resolve(collated_type)
    }
    if (!currentType) {
        collated_type = overallType
        resolve(collated_type)
    }
    // If there's a conflict in types, compare types and collate them into one
    if(currentType != overallType) {
    var currentType_grouping = null
    var overallType_grouping = null
    // Group types of data - ordered by most exclusive to most inclusive
    var groupings = require('./helpers/groupings.json')
    int_group = groupings.int_group
    special_int_group = groupings.special_int_group
    text_group = groupings.text_group
    special_text_group = groupings.special_text_group
    date_group = groupings.date_group
    // Set group type of current data
    if(int_group.includes(currentType)) {
        currentType_grouping = 'int'
    } else if (special_int_group.includes(currentType)) {
        currentType_grouping = 'special_int'
    } else if (text_group.includes(currentType)) {
        currentType_grouping = 'text'
    } else if (special_text_group.includes(currentType)) {
        currentType_grouping = 'special_text'
    } else if (date_group.includes(currentType)) {
        currentType_grouping = 'date'
    }
    
    // Set group type of overall data set
    if(int_group.includes(overallType)) {
        overallType_grouping = 'int'
    } else if (special_int_group.includes(overallType)) {
        overallType_grouping = 'special_int'
    } else if (text_group.includes(overallType)) {
        overallType_grouping = 'text'
    } else if (special_text_group.includes(overallType)) {
        overallType_grouping = 'special_text'
    } else if (date_group.includes(overallType)) {
        overallType_grouping = 'date'
    }
    
    // Section for handling if the data types have different groupings
    if(overallType_grouping != currentType_grouping) {
        if ((currentType == 'exponent' && overallType_grouping == 'int') || (overallType == 'exponent' && currentType_grouping == 'int')) {
            // One set of data is an exponent type column while the other is an integer type column
            collated_type = 'exponent'
        }
        if ((currentType == 'decimal' && overallType_grouping == 'int') || (overallType == 'decimal' && currentType_grouping == 'int')) {
            // One set of data is a decimal type column while the other is an integer type column
            collated_type = 'decimal'
        }
        
        if (overallType_grouping == 'text' || currentType_grouping == 'text') {
            // Either set of data is a text type column
            var done = false
            for(var i = text_group.length -1; i >= 0 && done == false; i--) {
                if(text_group[i] == currentType || text_group[i] == overallType) {
                    done = true
                    collated_type = text_group[i]
                }
            }
        } else if (overallType_grouping == 'special_text' || overallType_grouping == 'date' || currentType_grouping == 'special_text' || currentType_grouping == 'date') {
            collated_type = 'varchar'
        }
    }
    
    // Section for handling if the data types have similar groupings
    if(overallType_grouping == currentType_grouping) {
        // Compares exponent to decimals
        if(overallType_grouping == 'special_int') {
            // Either set of data is a special integer type column
            var done = false
            // Backwards for loop - most inclusive data column takes priority
            for(var i = special_int_group.length -1; i >= 0 && done == false; i--) {
                if(special_int_group[i] == currentType || special_int_group[i] == overallType) {
                done = true
                collated_type = special_int_group[i]
                }
            }
        }
        if(overallType_grouping == 'int') {
            // Either set of data is an integer type column
            var done = false
            // Backwards for loop - most inclusive data column takes priority
            for(var i = special_int_group.length -1; i >= 0 && done == false; i--) {
                if(int_group[i] == currentType || int_group[i] == overallType) {
                done = true
                collated_type = int_group[i]
                }
            }
        }
        if(overallType_grouping == 'date') {
            // Either set of data is a date type column
            // This one is special as date and time fields are completely different but can still be added to a datetime field.
            collated_type = 'datetime'
        }
    }
    if(collated_type) {
        resolve(collated_type)
    } else {
        reject({
            err: 'unknown data type collation',
            step: 'collate_types',
            description: 'unknown data types entered into collation',
            data: [overallType, currentType]
        })
    }
}})
}

// Find column headers for JSON data provided
async function get_headers (data) {
    return new Promise((resolve, reject) => {
    var all_columns = []
    for (var i = 0; i < data.length; i++) {
      for (column in data[i]){
        all_columns.push(column)
      }
    }
    var headers = new Set (all_columns)
    headers = Array.from(headers)
    resolve(headers)
    })
}

// Initialize meta data object to false/null/0 for all columnns
async function initialize_meta_data (headers) {
    var metaData = []
    for (var h = 0; h < headers.length; h++) {
        var metaObj = {
            [headers[h]]: {
                'type': null,
                'length': 0,
                'allowNull': false,
                'unique': false,
                'index': false,
                'pseudounique': false,
                'primary': false,
                'auto_increment': false,
                'default': undefined
            }
            // default value is optional
            // default value if provided, must be the full default value required (including quote marks if needed)
            // e.g. 'lorem ipsum' or 123 or NULL
        }
        metaData.push(metaObj)
    }
    return (metaData)
}

// This function goes through provided headers to identify indexes or primary keys
async function predict_indexes (headers, primary_key) {
    return new Promise((resolve, reject) => {
        var primary_key_found = false
        // Now that for each data row, a type, length and nullability has been determined, collate this into what this means for a database set (indexes).
        var groupings = require('./helpers/groupings.json')
        for (var h = 0; h < headers.length; h++) {
            var header_name = (Object.getOwnPropertyNames(headers[h])[0])
            // Dates or Datetimes or Timestamps should be considered to be an index
            date_group = groupings.date_group
            if(date_group.includes(headers[h][header_name]['type'])) {
                headers[h][header_name]['index'] = true
            }
            if(headers[h][header_name]['pseudounique'] || headers[h][header_name]['unique']) {
                headers[h][header_name]['index'] = true
            }
            // If a primary key column(s) have been specified, this will take priority,
            if(primary_key) {
                if(primary_key.includes(header_name)) {
                    headers[h][header_name]['primary'] = true
                    primary_key_found = true
                }
            }
        }
        keys_group = groupings.keys_group
        // If no such column exists, then a composite primary key will be made from non-nullable unique/pseudounique keys that have a valid key data type.
        if(!primary_key_found) {
            for (var h = 0; h < headers.length; h++) {
                var header_name = (Object.getOwnPropertyNames(headers[h])[0])
                if(!headers[h][header_name]['allowNull'] && keys_group.includes(header_name) && 
                (headers[h][header_name]['unique'] || headers[h][header_name]['pseudounique'])
                ) {
                    headers[h][header_name]['primary'] = true
                }
            }
        }
    resolve(headers)
    })
}

// This function when provided data, will find the most likely type, length, indexes etc.
async function get_meta_data (data, headers, config) {
    return new Promise(async (resolve, reject) => {
        var defaults = require('./config/defaults.json')
        // Variable for minimum number of datapoints required for unique-ness -- default 50        
        var minimum_unique = defaults.minimum_unique

        // Variable for % of values that need to be unique to be considered a pseudo-unique value -- default 95% (or 2 standard deviations)
        var pseudo_unique = defaults.pseudo_unique

        // Variable for primary key column -- default 'ID' only applies if such a column exists
        var primary = defaults.primary

        // Variable for auto-indexing data, set as false to disable, defaults to true
        var auto_indexing = defaults.auto_indexing

        // Variable for auto-creation of autoincrementing ID column, set as false to enable, defaults to false
        var auto_id = defaults.auto_id

        // Let user override this default via config object
        if(config) {
            // Error when Primary key is specified but auto_id has also been set
            if(config.primary && config.auto_id && config.primary != ['ID']) {
                reject({
                    err: 'primary key and auto_id was specified',
                    step: 'get_meta_data',
                    description: 'invalid configuration was provided to get_meta_data step',
                    resolution: 'please only use ONE of primary OR auto_id configuration for this step, not both'
                })
            }
            if(config.minimum_unique) {minimum_unique = config.minimum_unique}
            if(config.pseudo_unique) {pseudo_unique = config.pseudo_unique}
            if(config.primary) {primary = config.primary}
            if(config.auto_id) {auto_id = config.auto_id}
            if(config.auto_indexing === true || config.auto_indexing === false) {auto_indexing = config.auto_indexing}
        }

        /* Example config object to be provided
        config = {
            minimum_unique: 100,
            pseudo_unique: 97,
            primary: ['key_1', 'key_2'],
            auto_indexing: true,
            auto_id: false
        }
        */ 

        // Check if headers object/array was provided, and if not create a default header object for use
        if(!headers) {
            headers = await get_headers(data)
            headers = await initialize_meta_data(headers)
        } else {
            var meta_data_columns = ['type','length','allowNull','unique','pseudounique','index']
            if(headers.length == 0) {
                reject({
                    err: 'invalid header',
                    step: 'get_meta_data',
                    description: 'invalid header object provided to get_meta_data step',
                    data: headers
                })
            }
            for (var h = 0; h < headers.length; h++) {
                for(var m = 0; m < meta_data_columns.length; m++) {
                    // Check if current meta_data column of header contains data, if not, set to default (null/0/false)
                    if(headers[h][meta_data_columns[m]] === undefined) {
                        if(meta_data_columns[m] == 'type') {headers[h][meta_data_columns[m]] = null} 
                        else if(meta_data_columns[m] == 'default') {headers[h][meta_data_columns[m]] = undefined} 
                        else if(meta_data_columns[m] == 'length') {headers[h][meta_data_columns[m]] = 0} 
                        else {headers[h][meta_data_columns[m]] = false} 
                    }
                }
            }
        }

        // If no ID field was included and auto_id config field was set to true, then create an auto_incrementing numeric ID column
        if(!headers.includes('ID') && auto_id) {
            headers.push({
                'type': 'int',
                'length': 8,
                'allowNull': false,
                'unique': true,
                'index': true,
                'pseudounique': true,
                'primary': true,
                'auto_increment': true,
                'default': undefined
            })
        }
        
        // Reset uniqueCheck array to null for a fresh test of the uniqueness of dataset
        var uniqueCheck = {}
        for (var h = 0; h < headers.length; h++) {
            uniqueCheck[headers[h]] = (new Set())
        }

        // Repeat for each data row provided
        for (var i = 0; i < data.length; i++) {
            for (var h = 0; h < headers.length; h++) {
                var header_name = (Object.getOwnPropertyNames(headers[h])[0])
                var dataPoint = (data[i][header_name])
                if(dataPoint === null || dataPoint === undefined) {
                    var dataPoint = ''
                } else {
                    if(isObject(dataPoint)) {dataPoint = JSON.stringify(dataPoint)}
                        dataPoint = (dataPoint).toString()
                    }
                // Add data point to uniqueCheck array for particular header
                uniqueCheck[headers[h]].add(dataPoint)
                var overallType = headers[h][header_name]['type']
                // If a data point is null, set this column as nullable 
                if (dataPoint == '') {
                    headers[h][header_name]['allowNull'] = true
                } else {
                    // Else attempt to 
                    var currentType = await predict_type(dataPoint).catch(err => {catch_errors(err)})
                    if(currentType != overallType) {
                        var new_type = await collate_types(currentType, overallType).catch(err => {catch_errors(err)})
                        headers[h][header_name]['type'] = new_type
                    }
                    var len = dataPoint.length
                    var curLen = headers[h][header_name]['length']
                    if (len > curLen) {
                        headers[h][header_name]['length'] = len
                    }
                }
            }
        }

        // Find unique or pseudounique columns
        for (var h = 0; h < headers.length; h++) {
            if(uniqueCheck[headers[h]].size == data.length && data.length > 0 && data.length >= minimum_unique) {
                headers[h][header_name]['unique'] = true
            }
            if(uniqueCheck[headers[h]].size >= (data.length * pseudo_unique/100) && data.length > 0 && data.length >= minimum_unique) {
                headers[h][header_name]['pseudounique'] = true
            }            
        }

        if(auto_indexing) {
            // Now that for each data row, a type, length and nullability has been determined, collate this into what this means for a database set.
            headers = await predict_indexes(headers, primary).catch(err => {catch_errors(err)})
        }

        resolve(headers)
    })
}

// Create table from meta data
async function create_table (config, meta_data) {
    return new Promise (async (resolve, reject) => {
        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_helper = require(sql_dialect_lookup_object[config.sql_dialect].helper).exports
        var defaults = require('./config/defaults.json')

        // Set default collation
        var collation = defaults.collation

        // If no config or meta data has been provided, return an error
        if(!config || !meta_data) {
            // Error when no config or meta_data is provided
                reject({
                    err: `no ${config ? 'config' : ''} ${meta_data ? 'meta_data' : ''}  object(s) provided`,
                    step: 'create_table',
                    description: 'invalid configuration or meta_data was provided to create_table step',
                    resolution: 'please provide configuration object, additional details can be found in the documentation'
                })
            }

        if(config.collation) {collation = config.collation}

    })
}

function isObject(val) {
    if (val === null) { return false;}
    return ( (typeof val === 'function') || (typeof val === 'object') );
}

// This function when provided the database/table and headers object will find changes
async function catch_database_changes (database, table, headers) {
    
}

async function insert_data (config, data) {
    return new Promise (async (resolve, reject) => {

        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_helper = require(sql_dialect_lookup_object[config.sql_dialect].helper).exports

        // Check if the target schema exists
        var check_database_sql = sql_helper.check_database_exists(config.database)
        check_database_results = await sql_helper.run_query(config.connection, check_database_sql).catch(err => catch_errors)
        if (check_database_results.results[0][config.database] == 0) {
            create_database_sql = sql_helper.create_database(config.database)
            create_database = await sql_helper.run_query(config.connection, create_database_sql).catch(err => catch_errors)
            // As this database is new, always create the tables regardless of current set option
            config.create_table = true
        }

        // If create_tables is set to true, then don't bother checking if the table exists else, check if table exists (and overwrite create_tables)
        if(!config.create_table) {
            check_tables_sql = sql_helper.check_tables_exists(config.database, config.table)
            check_tables_results = await sql_helper.run_query(config.connection, check_tables_sql).catch(err => catch_errors)
            if(check_tables_results.results[0][config.table] == 0) {config.create_table = true}
        }
        
        // Get provided data's meta data
        if(config.minimum_unique || config.pseudo_unique || config.primary || config.auto_indexing) {
            var meta_data_config = {
                "minimum_unique": config.minimum_unique,
                "pseudo_unique": config.pseudo_unique,
                "primary": config.primary,
                "auto_indexing": config.auto_indexing,
                "auto_id": config.auto_id
            }
        } else { var meta_data_config = null }
        var new_meta_data = await get_meta_data(data, config.headers, meta_data_config).catch(err => {catch_errors(err)})

        // Now that the meta data associated with this data has been found, 
        if(config.create_table) {
            await create_table(config, new_meta_data).catch(err => {catch_errors(err)})
        }
    })
}

async function lazy_sql (config, data) {
    return new Promise (async (resolve, reject) => {
        if(!config) {
            reject({
                err: 'no configuration was set on automatic mode',
                step: 'lazy_sql',
                description: 'invalid configuration object provided to lazy_sql automated step',
                resolution: 'please provide configuration object, additional details can be found in the documentation'
            })
        }
    
        if(!config.create_table) {
            config.create_table = null
        }

        if(!config.sql_dialect || !config.database || !config.table) {
            reject({
                err: 'required configuration options not set on automatic mode',
                step: 'lazy_sql',
                description: 'invalid configuration object provided to lazy_sql automated step',
                resolution: `please provide supported value for ${!config.sql_dialect ? 'sql_dialect' : ''} ${!config.database ? 'database (target database)' : ''}
                ${!config.table ? 'table (target table)' : ''} in configuration object, additional details can be found in the documentation`
            })
        }
                
        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        if(!sql_dialect_lookup_object[config.sql_dialect]) {
            reject({
                err: 'no supported sql dialect was set on automatic mode',
                step: 'lazy_sql',
                description: 'invalid configuration object provided to lazy_sql automated step',
                resolution: 'please provide supported sql_dialect value in configuration object, additional details can be found in the documentation'
            })
        }
        
        var sql_helper = require(sql_dialect_lookup_object[config.sql_dialect].helper).exports

        // Establish a connection to the database (if no )
        if(!config.connection) {
            config.connection = await sql_helper.establish_connection(config).catch(err => {catch_errors(err)})
        }

        // From here begins the actual data insertion process
        await insert_data(config, data).catch(err => {catch_errors(err)})
    })
}

catch_errors = async function (err) {

}

module.exports = {
    predict_type,
    collate_types,
    get_headers,
    initialize_meta_data,
    get_meta_data,
    predict_indexes,
    catch_database_changes,
    lazy_sql,
    insert_data
}