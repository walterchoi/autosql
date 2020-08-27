// Using regex, when provided a data point, predict what data type this will be
async function predict_type (data) {
    return new Promise((resolve, reject) => {
        var reg = require('./helpers/regex.js')
        var currentType = null
        if(typeof data != 'string') {
            data = String(data)
        }
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
            if(data <= 127 && data >= -128) {
                currentType = 'tinyint'
            } else if(data <= 32767 && data >= -32768) {
                currentType = 'smallint'
            } else if(data <= 2147483647 && data >= -2147483648) {
                currentType = 'int'
            } else if(data <= 9223372036854775807 && data >= -9223372036854775808) {
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
        if(!currentType) {
            currentType == 'varchar'
        }
        if((currentType == 'datetime' || currentType == 'date' || currentType == 'time') && data.toString() === 'Invalid Date') {
            currentType = null
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
        if ((currentType == 'double' && overallType_grouping == 'int') || (overallType == 'double' && currentType_grouping == 'int')) {
            // One set of data is an exponent type column while the other is an integer type column
            collated_type = 'double'
        }
        if ((currentType == 'decimal' && overallType_grouping == 'int') || (overallType == 'decimal' && currentType_grouping == 'int')) {
            // One set of data is a decimal type column while the other is an integer type column
            collated_type = 'decimal'
        }
        
        if (overallType_grouping == 'text' || currentType_grouping == 'text') {
            // Either set of data is a text type column
            var done = false
            for(var i = text_group.length -1; i >= 0; i--) {
                if((text_group[i] == currentType || text_group[i] == overallType) && !done) {
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
            // Both sets of data are special integer type columns
            var done = false
            // Backwards for loop - most inclusive data column takes priority
            for(var i = special_int_group.length -1; i >= 0; i--) {
                if((special_int_group[i] == currentType || special_int_group[i] == overallType) && !done) {
                done = true
                collated_type = special_int_group[i]
                }
            }
        }
        if(overallType_grouping == 'int') {
            // Both sets of data are an integer type column
            var done = false
            // Backwards for loop - most inclusive data column takes priority
            for(var i = int_group.length -1; i >= 0; i--) {
                if((int_group[i] == currentType || int_group[i] == overallType) && !done) {
                done = true
                collated_type = int_group[i]
                }
            }
        }
        if(overallType_grouping == 'text') {
            // Both sets of data are a text type column
            var done = false
            // Backwards for loop - most inclusive data column takes priority
            for(var i = text_group.length -1; i >= 0; i--) {
                if((text_group[i] == currentType || text_group[i] == overallType) && !done) {
                done = true
                collated_type = text_group[i]
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
        // For checking the collation of different types
        // console.log('overallType: ' + overallType + '\ncurrentType: ' + currentType + '\ncollatedType: ' + collated_type)
        resolve(collated_type)
    } else {
        reject({
            err: 'unknown data type collation',
            step: 'collate_types',
            description: 'unknown data types entered into collation',
            data: [overallType, overallType_grouping, currentType, currentType_grouping]
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
                'default': undefined,
                'decimal': 0
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
async function predict_indexes (config, primary_key) {
    return new Promise((resolve, reject) => {
        var headers = config.meta_data
        
        // Max key length prevents long strings from becoming a part of a primary key
        var defaults = require('./config/defaults.json')
        var max_key_length = defaults.max_key_length
        if(config.max_key_length) {
            max_key_length = config.max_key_length
        }
        
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
                if(!headers[h][header_name]['allowNull'] && keys_group.includes(headers[h][header_name]['type']) && 
                (headers[h][header_name]['unique'] || headers[h][header_name]['pseudounique'])
                && headers[h][header_name]['length'] < max_key_length
                ) {
                    headers[h][header_name]['primary'] = true
                }
            }
        }
    resolve(headers)
    })
}

// This function when provided data, will find the most likely type, length, indexes etc.
async function get_meta_data (config, data) {
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

        // Variable for random sampling for getting meta data (only use for very large datasets) -- defaults to 0 or OFF
        // Sampling must be between 0 and 1 (decimal) which sets a threshold percentage of data points to test for this meta data
        // 1 (or 100%) means test all data points, whereas 0.5 means test only 50% (rounded up to nearest whole number data point - randomly chosen)
        // Sampling minimum prevents sampling from occurring if the data set provided is too low -- defaults to minimum of 100 data points AFTER sampling
        var sampling = defaults.sampling
        var sampling_minimum = defaults.sampling_minimum

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
            if(config.sampling) {sampling = config.sampling}
            if(config.sampling_minimum) {sampling_minimum = config.sampling_minimum}
            if(config.auto_indexing === true || config.auto_indexing === false) {auto_indexing = config.auto_indexing}
            if(!config.sql_dialect) {
                reject({
                    err: 'no sql dialect',
                    step: 'get_meta_data',
                    description: 'invalid configuration was provided to get_meta_data step',
                    resolution: 'please provide a sql_dialect (such as pgsql, mysql) to use as part of the configuration object'
                })
            }
        }

        /* Example config object to be provided
        config = {
            minimum_unique: 100,
            pseudo_unique: 0.97,
            primary: ['key_1', 'key_2'],
            auto_indexing: true,
            auto_id: false
        }
        */ 

        var headers = config.meta_data

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
                        else if(meta_data_columns[m] == 'length' || meta_data_columns[m] == 'decimal') {headers[h][meta_data_columns[m]] = 0} 
                        else {headers[h][meta_data_columns[m]] = false} 
                    }
                }
            }
        }

        // If no ID field was included and auto_id config field was set to true, then create an auto_incrementing numeric ID column
        if(!headers.includes('ID') && auto_id) {
            headers.push({"ID": {
                'type': 'int',
                'length': 8,
                'allowNull': false,
                'unique': true,
                'index': true,
                'pseudounique': true,
                'primary': true,
                'auto_increment': true,
                'default': undefined
            }})
        }
        
        // Reset uniqueCheck array to null for a fresh test of the uniqueness of dataset
        var uniqueCheck = {}
        for (var h = 0; h < headers.length; h++) {
            var header_name = (Object.getOwnPropertyNames(headers[h])[0])
            uniqueCheck[header_name] = (new Set())
        }
        
        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_lookup_table = require(sql_dialect_lookup_object[config.sql_dialect].helper_json)

        // Repeat for each data row provided
        if(sampling > 0 && sampling < 1) {
            var sampling_number = Math.round(data.length * sampling)
            // Check if the sampling number is larger than the minimum number for sampling
            if(sampling_number < sampling_minimum) {
                sampling_number = sampling_minimum    
            }
            data = shuffle(data)
            data = data.slice(0, sampling_number)
        }
        for (var i = 0; i < data.length; i++) {
            for (var h = 0; h < headers.length; h++) {
                var header_name = (Object.getOwnPropertyNames(headers[h])[0])
                var dataPoint = (data[i][header_name])
                if(dataPoint === null || dataPoint === undefined || dataPoint === '\\N' || dataPoint === 'null') {
                    var dataPoint = ''
                } else {
                    if(isObject(dataPoint)) {dataPoint = JSON.stringify(dataPoint)}
                        dataPoint = (dataPoint).toString()
                    }
                // Add data point to uniqueCheck array for particular header
                uniqueCheck[header_name].add(dataPoint)
                var overallType = headers[h][header_name]['type']
                // If a data point is null, set this column as nullable 
                if (dataPoint === '' || dataPoint === null || dataPoint === undefined || dataPoint === '\\N' || dataPoint === 'null') {
                    headers[h][header_name]['allowNull'] = true
                } else {
                    // Else attempt to 
                    var currentType = await predict_type(dataPoint).catch(err => {reject(err)})
                    if(currentType === null) {
                        headers[h][header_name]['allowNull'] = true
                    }
                    if(currentType != overallType && currentType !== null) {
                        var new_type = await collate_types(currentType, overallType).catch(err => {reject(err)})
                        if(new_type != headers[h][header_name]['type']) {
                            if(!sql_lookup_table.no_length.includes(new_type) && sql_lookup_table.no_length.includes(headers[h][header_name]['type'])) {
                                if(headers[h][header_name]['length'] < dataPoint.length + 5) {
                                    headers[h][header_name]['length'] = dataPoint.length + 5
                                }}
                            headers[h][header_name]['type'] = new_type
                    }}
                    if(sql_lookup_table.decimals.includes(headers[h][header_name]['type'])) {
                        if(Math.floor(dataPoint) == dataPoint) {var decimal_len = 0}
                        else {var decimal_len = dataPoint.toString().split(".")[1].length + 1}
                        
                        if(headers[h][header_name]['decimal']) {
                            if(decimal_len > headers[h][header_name]['decimal']) {
                                headers[h][header_name]['decimal'] = decimal_len  
                            }
                        } else {
                            headers[h][header_name]['decimal'] = decimal_len
                        }
                    }
                    var len = dataPoint.length
                    var curLen = headers[h][header_name]['length']
                    if(headers[h][header_name]['decimal']) {
                        var len = dataPoint.length + 3
                    }
                    if (len > curLen) {
                        headers[h][header_name]['length'] = len
                    }
                }
            }
        }

        config.meta_data = headers

        // Find unique or pseudounique columns
        for (var h = 0; h < headers.length; h++) {
            var header_name = (Object.getOwnPropertyNames(headers[h])[0])
            if(headers[h][header_name]['type'] == null) {
                headers[h][header_name]['type'] == 'varchar'
            }
            if(uniqueCheck[header_name].size == data.length && data.length > 0 && data.length >= minimum_unique) {
                headers[h][header_name]['unique'] = true
            }
            if(uniqueCheck[header_name].size >= (data.length * pseudo_unique) && data.length > 0 && data.length >= minimum_unique) {
                headers[h][header_name]['pseudounique'] = true
            }            
        }

        config.meta_data = headers

        if(auto_indexing) {
            // Now that for each data row, a type, length and nullability has been determined, collate this into what this means for a database set.
            headers = await predict_indexes(config, primary).catch(err => {reject(err)})
        }

        resolve(headers)
    })
}

// function to shuffle an array
function shuffle(array) {
    for (var i = array.length - 1; i > 0; i--) {
        var j = Math.floor(Math.random() * (i + 1))
        [array[i], array[j]] = [array[j], array[i]]
    }
    return (array)
}

// Create table from meta data
async function auto_create_table (config, meta_data) {
    return new Promise (async (resolve, reject) => {
        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_helper = require(sql_dialect_lookup_object[config.sql_dialect].helper).exports
        var defaults = require('./config/defaults.json')

        // Set default collation -- defaults to utf8mb4_unicode_ci
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
        create_table_sql = await sql_helper.create_table(config, meta_data).catch(err => {reject(err)})
        create_table = await run_sql_query(config, create_table_sql).catch(err => {reject(err)})
        resolve(create_table)
    })
}

function isObject(val) {
    if (val === null) { return false;}
    return ( (typeof val === 'function') || (typeof val === 'object') );
}

// This function when provided the database/table and headers object will find changes
async function auto_alter_table (config, new_headers) {
    return new Promise (async (resolve, reject) => {
        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_helper = require(sql_dialect_lookup_object[config.sql_dialect].helper).exports
        if(!new_headers && config.meta_data) {
            new_headers = config.meta_data
        }

        var get_table_description_sql = sql_helper.get_table_description(config)
        table_description = await run_sql_query(config, get_table_description_sql).catch(err => catch_errors(err))
        var old_headers = await convert_table_description(config, table_description)
        var table_changes = await compare_two_headers(config, old_headers, new_headers).catch(err => catch_errors(err))
        // Update config.meta_data to reflect the altered table
        for(var nh = 0; nh < config.meta_data.length; nh++) {
            var header_name = (Object.getOwnPropertyNames(config.meta_data[nh])[0])
            for (var oh = 0; oh < old_headers.length; oh++) {
                var oldheader_name = (Object.getOwnPropertyNames(old_headers[oh])[0])
                if(oldheader_name === header_name) {
                    if(config.meta_data[nh][header_name].type != old_headers[oh][oldheader_name].type) {
                        var new_type = await collate_types(config.meta_data[nh][header_name].type, old_headers[oh][oldheader_name].type).catch(err => {reject(err)})
                        if(new_type !== config.meta_data[nh][header_name].type) {
                            config.meta_data[nh][header_name].type = new_type
                        }
                    }
                }
            }
        }

        if(table_changes) {
            if(table_changes.new.length > 0 || table_changes.alter.length > 0) {
                table_alter_sql = await sql_helper.alter_table(config, table_changes).catch(err => catch_errors(err))
                altered_table = await run_sql_query(config, table_alter_sql).catch(err => catch_errors(err))
                resolve(altered_table)
            } else {
                resolve(null)
            }
        }
        else {
            resolve(null)
        }
    })
}

// Compare two sets of headers to identify changes
async function compare_two_headers (config, old_headers, new_headers) {
    return new Promise(async (resolve, reject) => {

        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_lookup_table = require(sql_dialect_lookup_object[config.sql_dialect].helper_json)

        // Currently the ALTER statements only support NEW columns, ALTER lengths, ALTER types and to ALLOW NULL
        // This compare headers function also only supports these

        // Check for NEW
        var new_columns = []
        var alter_columns = []
    
        // Get list of just column names for both old headers and new heeaderes
        var old_headers_list = []
        if(old_headers) {
            old_headers.map(header => 
                old_headers_list.push(Object.getOwnPropertyNames(header)[0])
            )
        }
        var new_headers_list = []
        if(new_headers) {
            new_headers.map(header => 
                new_headers_list.push(Object.getOwnPropertyNames(header)[0])
            )
        }

        for(var oh = 0; oh < old_headers_list.length; oh++) {
            var column_name = old_headers_list[oh]
            var old_header_obj = old_headers[oh][column_name]
            if(new_headers_list.includes(old_headers_list[oh])) {
                // Section to handle if this 'old header' is present in the new data too

                // At this point, due to the symmetrical handling of the new headers object, the old headers object will not have this function run anything at all.

            } else {
                // If this column does not exist in the new dataset, this column must be NULLABLE to allow this new dataset to be entered
                if(!old_header_obj.allowNull) {
                    old_headers[oh][column_name].allowNull = true
                    alter_columns.push(old_headers[oh])
                }
            }
        }

        for(var nh = 0; nh < new_headers_list.length; nh++) {
            var column_name = new_headers_list[nh]
            if(old_headers_list.includes(new_headers_list[nh])) {
                // Section to handle if this 'new header' is present in the current table too
                var old_header_obj = old_headers[old_headers_list.indexOf(new_headers_list[nh])][column_name]
                var new_header_obj = new_headers[nh][column_name]
                var changes = {
                    changed: false
                }
                var collated_type = old_header_obj.type
                // If the types do not match, find the new collated type
                if(new_header_obj.type != old_header_obj.type) {
                    collated_type = await collate_types(new_header_obj.type, old_header_obj.type).catch(err => {reject(err)})
                    if(collated_type != old_header_obj.type) {
                        changes.type = collated_type
                        changes["length"] = old_header_obj["length"]
                        changes.decimal = old_header_obj.decimal
                        changes.changed = true
                    }
                }

                if(!sql_lookup_table.no_length.includes(collated_type)) {
                    if(new_header_obj["length"] > old_header_obj["length"] && old_header_obj["length"] !== null) {
                        changes["length"] = new_header_obj["length"]
                        changes.changed = true
                    }

                    if(new_header_obj.decimal > old_header_obj.decimal) {
                        changes.decimal = new_header_obj.decimal
                        changes.changed = true
                    }
                }

                if(new_header_obj.allowNull && !old_header_obj.allowNull) {
                    changes.allowNull = new_header_obj.allowNull
                    changes.changed = true
                }

                // If any change was found, add this to the 'ALTER_COLUMNS' array
                if(changes.changed) {
                    if(!changes.type) {
                        changes.type = old_header_obj["type"]
                    }
                    // if the value is changed but no length or decimal length changes are found, but are required for ALTER statements
                    if(sql_lookup_table.decimals.includes(changes.type) && changes.decimal === undefined) {
                        changes.decimal = old_header_obj["decimal"]
                    }
                    if(changes["length"] === undefined) {
                        changes["length"] = old_header_obj["length"]
                    }
                    delete changes.changed
                    alter_columns.push({
                        [column_name]: changes
                    })
                }

            } else {
                // Because this new column with associated data does not exist on the existing database, add this as a new column that IS NULLABLE
                var new_header_obj = new_headers[nh][column_name]
                // Check if allow null is false, and set as true - as there is likely already existing data that does not feature this column
                if(!new_header_obj.allowNull) {
                    new_headers[nh][column_name].allowNull = true
                }
                new_columns.push(new_headers[nh])
            }
        }
        resolve({
            new: new_columns,
            alter: alter_columns
        })
    })
}

// Returned keys are lowercase on pgsql while uppercase on mysql
function changeKeysToUpper(obj) {
    var key, upKey;
    for (key in obj) {
        if (obj.hasOwnProperty(key)) {
            upKey = key.toUpperCase();
            if (upKey !== key) {
                obj[upKey] = obj[key];
                delete(obj[key]);
            }
            // recurse
            if (typeof obj[upKey] === "object") {
                changeKeysToUpper(obj[upKey]);
            }
        }
    }
    return obj;
}

// Translate description provided by SQL server into header object used by this repository
async function convert_table_description (config, table_description) {
    var sql_dialect_lookup_object = require('./config/sql_dialect.json')
    var sql_lookup_table = require(sql_dialect_lookup_object[config.sql_dialect].helper_json)
    var table_desc = table_description
    table_desc = changeKeysToUpper(table_desc)
    var old_headers = []
    for (var c = 0; c < table_desc.length;  c++) {
        var column_name = table_desc[c].COLUMN_NAME
        var data_type = table_desc[c].DATA_TYPE
        var old_length = table_desc[c]['LENGTH']
        var nullable = table_desc[c].IS_NULLABLE
        if(old_length) {
            if(old_length.includes(',')) {
                var decimal = old_length.toString().split(",")[1].length
                old_length = old_length.toString().split(",")[0].length
            }
        }
        if(nullable == 'NO') {
            nullable = false
        } else if (nullable == 'YES') {
            nullable = true
        }
        if(sql_lookup_table.translate.server_to_local[data_type]) {
            data_type = sql_lookup_table.translate.server_to_local[data_type]
        }
        var header_obj = {
            [column_name]: {
                "type": data_type,
                "length": old_length,
                "decimal": decimal,
                "allowNull": nullable
            }
        }
        old_headers.push(header_obj)
    }
    return(old_headers)
}

async function auto_configure_table (config, data) {
    return new Promise (async (resolve, reject) => {

        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_helper = require(sql_dialect_lookup_object[config.sql_dialect].helper).exports

        // Check if the target schema exists
        var check_database_sql = sql_helper.check_database_exists(config)
        var check_database_results = await run_sql_query(config, check_database_sql)
        if (check_database_results[0][config.database] == 0) {
            create_database_sql = sql_helper.create_database(config)
            create_database = await run_sql_query(config, create_database_sql).catch(err => {reject(err)})
            // As this database is new, always create the tables regardless of current set option
            config.create_table = true
        }

        // If create_tables is set to true, then don't bother checking if the table exists else, check if table exists (and overwrite create_tables)
        if(!config.create_table) {
            check_tables_sql = sql_helper.check_tables_exists(config)
            check_tables_results = await run_sql_query(config, check_tables_sql).catch(err => catch_errors(err))
            if(check_tables_results[0][config.table] == 0) {config.create_table = true}
        }
        
        // Get provided data's meta data
        if(!config.meta_data) {
            var new_meta_data = await get_meta_data(config, data).catch(err => {reject(err)})
            config.meta_data = new_meta_data
        } else {
            var new_meta_data = config.meta_data
        }

        // Now that the meta data associated with this data has been found, 
        if(config.create_table) {
            var auto_create_table_results = await auto_create_table(config, new_meta_data).catch(err => {reject(err)})
            resolve(auto_create_table_results)
        } else {
            await auto_alter_table(config, new_meta_data).catch(err => {reject(err)})
            resolve(auto_create_table_results)
        }
    })
}

async function validate_database (provided_config) {
    return new Promise (async (resolve, reject) => {
        var config = JSON.parse(JSON.stringify(provided_config))
        config = await check_config(config, false).catch(err => {reject(err)})
        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_helper = require(sql_dialect_lookup_object[config.sql_dialect].helper).exports

        var check = await sql_helper.test_connection(config).catch(err => {
            reject(err)
        })
        resolve(null)
    })
}

async function validate_query (provided_config, query) {
    return new Promise (async (resolve, reject) => {
        var config = JSON.parse(JSON.stringify(provided_config))
        config = await check_config(config, false).catch(err => {reject(err)})
        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_helper = require(sql_dialect_lookup_object[config.sql_dialect].helper).exports

        var check = await sql_helper.test_query(config, query).catch(err => {
            reject(err)
        })
        resolve(null)
    })
}

async function insert_data (config, data) {
    return new Promise (async (resolve, reject) => {
        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_helper = require(sql_dialect_lookup_object[config.sql_dialect].helper).exports

        var defaults = require('./config/defaults.json')
        // Insert type (or style), determines how to insert this data set. -- default - REPLACE (which means ON DUPLICATE KEY REPLACE)
        // Available options: 'REPLACE', 'IGNORE'
        var insert_type = defaults.insert_type
        if(config.insert_type) {
            insert_type = config.insert_type
        } else {
            config.insert_type = insert_type
        }

        // If this is a REPLACE type insert, check for Primary Keys -- only needed for pgsql
        if(config.sql_dialect == 'pgsql' && !config.keys) {
            var constraints_sql = sql_helper.find_constraint(config)
            var constraints = await run_sql_query(config, constraints_sql).catch(err => {reject(err)})
            config.keys = {}
            if(constraints) {
                for(var c = 0; c < constraints.length; c++) {
                    var constraint_name = constraints[c].constraint_name
                    var column_name = constraints[c].column_name
                    if(!config.keys[constraint_name]) {
                        config.keys[constraint_name] = []
                    }
                    config.keys[constraint_name].push(column_name)
                }
            }
        }

        // Safe mode determines if the insert statement relies on autocommit or uses a rollback on failure -- defaults to true
        var safe_mode = defaults.safe_mode
        if(config.safe_mode) {
            safe_mode = config.safe_mode
        }

        // Group data into groups of rows for smaller inserts
        data = await sqlize(config, data)
        var stacked_data = await stack_data(config, data)
        var insert_statements = []
        
        for(var s = 0; s < stacked_data.length; s++) {
            var insert_statement = await sql_helper.create_insert_string(config, stacked_data[s]).catch(err => {reject(err)})
            insert_statements.push(insert_statement)
        }

        var query_result = await run_sql_query(config, insert_statements).catch(err => {reject(err)})
        
        resolve(query_result)
    })
}

// Stack data into sets of arrays for smaller insert statements
async function stack_data (config, data) {
    return new Promise ((resolve, reject) => {

        var defaults = require('./config/defaults.json')

        // max_insert size determines the largest number of rows to attempt to insert at one time -- defaults to 5000
        var max_insert = defaults.max_insert
        if(config.max_insert) {
            max_insert = config.max_insert
        }

        // insert_stack determines the number of rows to attempt to add to insert query before checking if insertion size will exceed query max -- defaults to 100
        var insert_stack = defaults.insert_stack
        if(config.insert_stack) {
            insert_stack = config.insert_stack
        }

        // max_insert_size determinnes maximum insert query size  -- defaults to 1048576 bytes (1MB) which is the default max_allowed_packet for MySQL databases
        var max_insert_size = defaults.max_insert_size
        if(config.max_insert_size) {
            max_insert_size = config.max_insert_size
        }

        if(insert_stack > max_insert) {
            reject({
                err: 'invalid configuration was provided',
                step: 'stack_data',
                description: 'minimum insert size was larger than maximum insert size',
                resolution: 'please provide insert_stack value (default 50) in configuration that is smaller than the max_insert value (default 1000)'
            })
        }

        // Stacked_data = [stacked_data_groups(1), stacked_data_groups(2) ...] where each data_group will be smaller than the max_insert sizes
        // Stacked_data_group = [row(1), row(2) ...] where each row is just the object (with headers)
        // Stacked_data_array_part is just a working array that is used to check sizes per 'insert_stack'
        var stacked_data = []
        var stacked_data_group = []
        var stacked_data_array_part = []
        var stacked_data_array_group_string = ''
        var stacked_data_array_part_string = ''

        for (var d = 0; d < data.length; d += insert_stack) {
            var stacked_data_array_part = data.slice(d, d + insert_stack)
            stacked_data_array_part.map(obj => 
                stacked_data_array_part_string += Object.values(obj).join(', ')
            )

            var group_str_size = getBinarySize(stacked_data_array_group_string)
            var group_size = stacked_data_group.length
            var part_str_size = getBinarySize(stacked_data_array_part_string)
            var part_size = stacked_data_array_part.length
            var combined_str_size = group_str_size + part_str_size
            var combined_array_size = group_size + part_size
            // Check if adding this new (minimum stack) would push this group of data over the maximum insert limits (or if this is the last stack being inserted)
            if(combined_str_size > max_insert_size || combined_array_size > max_insert || (d + insert_stack) >= data.length) {
                // If it does not, keep adding this to this group
                if(stacked_data_group.length > 0) {
                    stacked_data.push(stacked_data_group)   
                }  
                stacked_data_group = Array.from(stacked_data_array_part)
                stacked_data_array_part = []
                stacked_data_array_group_string = stacked_data_array_part_string
                stacked_data_array_part_string = '' 
                if(combined_str_size <= max_insert_size || combined_array_size <= max_insert && (d + insert_stack) >= data.length) {
                    stacked_data.push(stacked_data_group)   
                    stacked_data_group = Array.from(stacked_data_array_part)
                    stacked_data_array_part = []
                    stacked_data_array_group_string = stacked_data_array_part_string
                    stacked_data_array_part_string = '' 
                }
            } else {
                stacked_data_group = stacked_data_group.concat(stacked_data_array_part)
                stacked_data_array_group_string += stacked_data_array_part_string
            }
        }
        resolve(stacked_data)
    })
}

function getBinarySize (str) {
    return Buffer.byteLength(str, 'utf8')
}

// Function to run SQL queries - and run single or arrays of queries with/without transactions
async function run_sql_query (config, sql_query) {
    return new Promise (async (resolve, reject) => {
        config = await check_config(config, false).catch(err => {reject(err)})
        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_helper = require(sql_dialect_lookup_object[config.sql_dialect].helper).exports
        var query_results = []
        var query_rows_count = 0
        var query_errors = []
        var safe_mode = config.safe_mode
        var insert_check = false
        // Pass 'DISABLE_SAFE_MODE into the SQL queries array if you wish to turn off safe mode for single queries irregardless of the config
        if(Array.isArray(sql_query)) {
            if(sql_query.includes('DISABLE_SAFE_MODE')) {
                sql_query.splice(sql_query.findIndex('DISABLE_SAFE_MODE'),1)
                safe_mode = false
            }
            if(safe_mode) {
                for (var s = 0; s < sql_query.length && !insert_check; s++) {
                    _insert_check = await check_if_insert(sql_query[s])
                    if(_insert_check) {
                        insert_check = _insert_check
                    }
                }
            }
        } else {
            insert_check = await check_if_insert(sql_query)
        }

        if(safe_mode && insert_check) {
            var start = sql_helper.start_transaction()
            await sql_helper.run_query(config, start).catch(err => {reject(err)})
        }
    
        if(Array.isArray(sql_query)) {
            for(var sql = 0; sql < sql_query.length && query_errors.length == 0; sql++) {
                var query_result = await sql_helper.run_query(config, sql_query[sql], 0, 1).catch(err => {query_errors.push(err)})
                if(Array.isArray(query_result)) {
                    query_results = query_results.concat(query_result)
                } else {
                    if(typeof query_result == 'number') {
                        query_rows_count += query_result
                    }
                }
            }
        } else {
            var query_result = await sql_helper.run_query(config, sql_query).catch(err => {query_errors.push(err)})
            if(Array.isArray(query_result)) {
                query_results = query_results.concat(query_result)
            } else {
                if(typeof query_result == 'number') {
                    query_rows_count += query_result
                }
            }
        }

        if(safe_mode && insert_check && query_errors.length == 0) {
            var commit = sql_helper.commit()
            await sql_helper.run_query(config, commit).catch(err => {reject(err)})
            if(query_results.length > 0) {
                resolve(query_results)
            } else {
                resolve(query_rows_count)
            }
        }
        else if(safe_mode && insert_check && query_errors.length != 0) {
            var rollback = sql_helper.rollback()
            await sql_helper.run_query(config, rollback).catch(err => {reject(err)})
            console.log(query_errors)
            reject(query_errors)
        }
        if ((!safe_mode || !insert_check) && query_errors.length == 0) {
            if(query_results.length > 0) {
                resolve(query_results)
            } else {
                resolve(query_rows_count)
            }
        } else {
            reject(query_errors)
        }
    })
}

// Function to handle special characters such as ' or \ and replace with '' or \\
// And to handle columns that do not exist in certain rows, and add NULL to them
function sqlize (config, data) {
    return new Promise(async resolve => {
        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_lookup_table = require(sql_dialect_lookup_object[config.sql_dialect].helper_json)
        var groupings = require('./helpers/groupings.json')
        date_group = groupings.date_group
        var defaults = require('./config/defaults.json')
        var convert_timezone = defaults.convert_timezone
        if(config.convert_timezone) {convert_timezone = config.convert_timezone}
        if(convert_timezone) {
            var locale = defaults.locale
            if(config.locale) {locale = config.locale}
            var timezone = defaults.timezone
            if(config.timezone) {timezone = config.timezone}
        }

        var _sqlize = sql_lookup_table.sqlize
        if(!config.meta_data) {
            config.meta_data = await get_meta_data(config, data).catch(err => {reject(err)})
        }
        var metaData = config.meta_data
        var headers = []
        metaData.map(header => 
            headers.push(Object.getOwnPropertyNames(header)[0])
        )
    
        for (var d = 0; d < data.length; d++) {
            var row = data[d]
            for (key in row) {
                var index = headers.findIndex(column => column == key)
                var value = row[key]
                
                if(value === undefined || value === '\\N' || value === null || value === 'null') {
                    value = null
                    data[d][key] = value
                }
                else if(Object.prototype.toString.call(value) === '[object Date]' || (date_group.includes(metaData[index][key]["type"]) && date_group.includes(predict_type(value)))) {
                    if(value.toString() === 'Invalid Date') {
                        value = null
                    } else {
                        value = new Date(value)
                        value = value.toISOString()
                    }
                    data[d][key] = value
                } else if (typeof value === 'object') {
                    value = JSON.stringify(value)
                }
                for (var s = 0; s < _sqlize.length; s++) {
                    var regex = new RegExp(_sqlize[s].regex, 'gmi')
                    var type_req = _sqlize[s].type
                    if(type_req === true || type_req == metaData[index][key]["type"] || type_req.includes(metaData[index][key]["type"])) {
                        if(value !== undefined && value !== null) {
                            try {
                                value = value.toString().replace(regex, _sqlize[s].replace)
                            }
                            catch (e) {
                                console.log('errored on sqlizing - ' + value + ' for sqlize ' + JSON.stringify(_sqlize[s]))
                            }
                        }
                        data[d][key] = value
                    }
                }
            }
        }
        resolve(data)
    })
}

function sqlize_value (config, value) {
    return new Promise(async resolve => {
        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_lookup_table = require(sql_dialect_lookup_object[config.sql_dialect].helper_json)
        var groupings = require('./helpers/groupings.json')
        date_group = groupings.date_group
        var defaults = require('./config/defaults.json')
        var convert_timezone = defaults.convert_timezone
        if(config.convert_timezone) {convert_timezone = config.convert_timezone}
        if(convert_timezone) {
            var locale = defaults.locale
            if(config.locale) {locale = config.locale}
            var timezone = defaults.timezone
            if(config.timezone) {timezone = config.timezone}
        }

        var sqlize = sql_lookup_table.sqlize
        var type = await predict_type(value)
    
        if(value === undefined || value === '\\N' || value === null || value === 'null') {
            value = null
        }
        else if(Object.prototype.toString.call(value) === '[object Date]' || (date_group.includes(type) && date_group.includes(predict_type(value)))) {
            if(Object.prototype.toString.call(value) !== '[object Date]') {
                value = new Date(value)
            }
            value = value.toISOString()
        } else if (typeof value === 'object') {
            value = JSON.stringify(value)
        }
        for (var s = 0; s < sqlize.length; s++) {
            var regex = new RegExp(sqlize[s].regex, 'gmi')
            var type_req = sqlize[s].type
            if(type_req === true || type_req == type || type_req.includes(type)) {
                if(value !== undefined && value !== null) {
                    try {
                        value = value.toString().replace(regex, sqlize[s].replace)
                    }
                    catch (e) {
                        console.log('errored on sqlizing - ' + value + ' for sqlize ' + JSON.stringify(sqlize[s]))
                    }
                }
            }
        }
        resolve(value)
    })
}

async function auto_sql (provided_config, data) {
    return new Promise (async (resolve, reject) => {
        var start_time = new Date()
        console.log('provided_config')
        console.log(provided_config)
        console.log('provided_config')
        try {
            var config = JSON.parse(JSON.stringify(provided_config))
            console.log('config')
            console.log(config)
            console.log('config')
        } catch (err) {
            var config = provided_config
        }
        
        checked_config = await check_config(config, true).catch(err => {reject(err)})
        if(checked_config) {
            console.log('checked_config')
            console.log(checked_config)
            console.log('checked_config')
            config = checked_config
        }
        
        var sql_dialect_lookup_object = require('./config/sql_dialect.json')
        var sql_helper = require(sql_dialect_lookup_object[config.sql_dialect].helper).exports

        // From here begins the actual data insertion process
        // First let us get the provided data's meta data
        if(!config.meta_data) {
            config.meta_data = await get_meta_data(config, data).catch(err => {reject(err)})
        }
        // First let us make sure that the table exists or the table is compatible with the new data being inserted
        await auto_configure_table(config, data).catch(err => {reject(err)})
        // Now let us insert the data into the table
        var inserted = await insert_data(config, data).catch(err => {reject(err)})
        var completion_time = new Date()
        resolve({
            start: start_time,
            end: completion_time,
            results: inserted
        })
    })
}

async function check_config (provided_config, auto) {
    return new Promise(async (resolve, reject) => {
    if(!provided_config) {
        reject({
            err: 'no configuration was set on automatic mode',
            step: 'auto_sql',
            description: 'invalid configuration object provided to auto_sql automated step',
            resolution: 'please provide configuration object, additional details can be found in the documentation'
        })
    }

    if(!provided_config.create_table) {
        provided_config.create_table = null
    }

    if(!provided_config.sql_dialect || (!provided_config.database && auto) || (!provided_config.table && auto)) {
        reject({
            err: 'required configuration options not set on automatic mode',
            step: 'auto_sql',
            description: 'invalid configuration object provided to auto_sql automated step',
            resolution: `please provide supported value for ${!provided_config.sql_dialect ? 'sql_dialect' : ''} ${!provided_config.database ? 'database (target database)' : ''}
            ${!provided_config.table ? 'table (target table)' : ''} in configuration object, additional details can be found in the documentation`
        })
    }
            
    var sql_dialect_lookup_object = require('./config/sql_dialect.json')
    if(!sql_dialect_lookup_object[provided_config.sql_dialect]) {
        reject({
            err: 'no supported sql dialect was set on automatic mode',
            step: 'auto_sql',
            description: 'invalid configuration object provided to auto_sql automated step',
            resolution: 'please provide supported sql_dialect value in configuration object, additional details can be found in the documentation'
        })
    }
    
    var sql_helper = require(sql_dialect_lookup_object[provided_config.sql_dialect].helper).exports

    if(provided_config.ssh_config && !provided_config.ssh_stream) {
        if(provided_config.ssh_config.username){
            provided_config.ssh_stream = await set_ssh(provided_config.ssh_config).catch(err => {
                reject(err)
            })
        } else {
            console.log(provided_config)
        }
    }

    // Establish a connection to the database (if not already existing)
    if(!provided_config.connection) {
        provided_config.connection = await sql_helper.establish_connection(provided_config).catch(err => {reject(err)})
    }

    resolve(provided_config)
    })
}

async function set_ssh (ssh_keys) {
    return new Promise(async (resolve, reject) => {
        try {
            var Client = require('ssh2').Client;
           }
           catch (e) {
            console.log('SSH tunnel config specified but ssh2 repository has not been installed. Please install ssh2 via "npm install ssh2"')
            reject(e)
           }
        var ssh = new Client();
        if(!ssh_keys || !ssh_keys.username) {
            reject('No ssh key or username provided')
        }
        if(ssh_keys.private_key_path && !ssh_keys.private_key) {
            ssh_keys.private_key = await readfile(ssh_keys.private_key_path)
        }
        var ssh_config = {
            "username": ssh_keys.username,
            "host": ssh_keys.host,
            "port": ssh_keys.port
        }
        if(ssh_keys.password) {
            ssh_config.password = ssh_keys.password
        }
        if(ssh_keys.debug) {
            var debug_function = function (message) {
                if(message.search('Outgoing') > 0 || message.search('Client') > 0) {
                    console.log(message)
                }
            }
            ssh_config.debug = debug_function
        }
        if(ssh_keys.private_key) {
            ssh_config.privateKey = ssh_keys.private_key
        }
        if(ssh_keys.timeout) { ssh_config.readyTimeout = ssh_keys.timeout }
        else { ssh_config.readyTimeout = 10000 }
        ssh.on('ready', function() {
            ssh.forwardOut(
                ssh_keys.source_address,
                ssh_keys.source_port,
                ssh_keys.destination_address,
                ssh_keys.destination_port,
                async function (err, stream) {
                    if (err) throw err;
                    stream.on('close', function() {
                      console.log('Stream :: close');
                      ssh.end();
                    })
                    resolve(stream)
                }
            );
        }).connect(ssh_config);
    })
}

async function catch_errors (err) {
    console.log(err)
    return(err)
}

async function readfile (path) {
    return new Promise(resolve => {
        try {
            var fs = require('fs');
           }
           catch (e) {
            console.log('SSH tunnel config specified, key path provided but fs repository has not been installed. Please install fs via "npm install fs" OR provide the parsed ssh_key instead of the path')
            reject(e)
           }
        fs.readFile(path, 'utf8', function(err, res){
            resolve(res)
        })
    })
}

async function check_if_insert (source_sql) {
    return new Promise(resolve => {
        if(source_sql) {
        var source_sql_statements = source_sql.split(';');
        var check = true
        for (var e = 0; e < source_sql_statements.length; e++) {
            source_sql_statements[e] = source_sql_statements[e].trim()
            if(source_sql_statements[e] != null && source_sql_statements[e] != undefined && source_sql_statements[e] != "") {
            if(!source_sql_statements[e].toLowerCase().startsWith('insert')) {
                check = false
            }
        }
        }
        resolve(check)
    } else {
        resolve(true)
    }
    })
}

async function export_sql_helper (provided_config) {
    var sql_dialect_lookup_object = require('./config/sql_dialect.json')
    var sql_helper = require(sql_dialect_lookup_object[provided_config.sql_dialect].helper).exports
    return sql_helper
}

module.exports = {
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
    validate_query,
    run_sql_query,
    set_ssh,
    export_sql_helper,
    sqlize,
    sqlize_value
}