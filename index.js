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
            for(var i = text_group.length; i > 0 && done == false; i--) {
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
            for(var i = special_int_group.length; i > 0 && done == false; i--) {
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
            for(var i = special_int_group.length; i > 0 && done == false; i--) {
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
                'pseudounique': false
            }
        }
        metaData.push(metaObj)
    }
    return (metaData)
}

// This function when provided data, will find the most likely type, 
async function get_meta_data (data, headers, config) {
    return new Promise((resolve, reject) => {
        // Variable for minimum number of datapoints required for unique-ness -- default 50        
        var minimum_unique = 50
        // Variable for % of values that need to be unique to be considered a pseudo-unique value -- default 95% (or 2 standard deviations)
        var pseudo_unique = 95
        // Let user override this default via config object
        if(config) {
            if(config.minimum_unique) {minimum_unique = config.minimum_unique}
            if(config.pseudo_unique) {pseudo_unique = config.pseudo_unique}
        }
        // Check if headers object/array was provided, and if not create a default header object for use
        if(!headers) {
            headers = get_headers(data)
            headers = initialize_meta_data(headers)
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
                        else if(meta_data_columns[m] == 'length') {headers[h][meta_data_columns[m]] = 0} 
                        else {headers[h][meta_data_columns[m]] = false} 
                    }
                }
            }
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
                    var currentType = await predict_type(dataPoint)
                    if(currentType != overallType) {
                        var new_type = await collate_types(currentType, overallType)
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
        // Now that for each data row, a type, length and nullability has been determined, collate this into what this means for a database set.
        for (var h = 0; h < headers.length; h++) {
            var header_name = (Object.getOwnPropertyNames(headers[h])[0])
            // If no type has been specified, this suggests that no values for this column have been specified, as such disregard this column
            if(!headers[h][header_name]['type']) {
                delete headers[h][header_name]
            } else {
                if(uniqueCheck[headers[h]].size == data.length && data.length >= minimum_unique && data.length > 0) {
                    headers[h][header_name]['unique'] = true
                    headers[h][header_name]['index'] = true
                }
                if(uniqueCheck[h].size >= (data.length * pseudo_unique/100) && data.length >= minimum_unique && data.length > 0) {
                    headers[h][header_name]['pseudounique'] = true
                    headers[h][header_name]['index'] = true
                }
                var groupings = require('./helpers/groupings.json')
                date_group = groupings.date_group
                if(date_group.includes(headers[h][header_name]['type'])) {
                    headers[h][header_name]['index'] = true
                }
            }
        }
        resolve(headers)
    })
}

module.exports = {
    predict_type,
    collate_types,
    get_headers,
    initialize_meta_data,
    get_meta_data,
    SQLize
}