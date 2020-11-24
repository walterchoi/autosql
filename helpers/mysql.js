var exports = {
    establish_connection : async function (key) {
        return new Promise((resolve, reject) => {
            mysql = require('mysql2');
            if(!key.host || !key.username || !key.password) {
                reject({
                    err: 'required configuration options not set for mysql connection',
                    step: 'establish_connection (mysql variant)',
                    description: 'invalid configuration object provided to autosql automated step',
                    resolution: `please provide supported value for ${!key.host ? 'host' : ''} ${!key.username ? 'username' : ''}
                    ${!key.password ? 'password' : ''} in configuration object, additional details can be found in the documentation`
                })
            }
            var mysql_config = {
                host: key.host,
                user: key.username,
                password: key.password,
                port: key.port,
                waitForConnections: true,
                connectionLimit: 20,
                queueLimit: 0
            }
            if(key.database) {
                mysql_config.database = key.database
            } else if(key.schema) {
                mysql_config.database = key.schema
            }
            if(key.ssh_stream) {
                mysql_config.stream = key.ssh_stream
            }
            pool = mysql.createPool(mysql_config)
            resolve(pool)
        })
    },
    test_connection : async function (key) {
        return new Promise(async (resolve, reject) => {
            var _err = null
            var pool = await this.establish_connection(key).catch(err => {
                _err = err
                if(err) {reject(err)}
            })
            var sql_query = "SELECT 1 AS SOLUTION;"
            var config = {
                "connection": pool
            }
            if(!_err) {
                var result = await this.run_query(config, sql_query, 0, 1).catch(err => {
                    _err = err
                    if(err) {reject(err)}
                })
                resolve(result)
            } else {
                resolve(_err)
            }
        })
    },
    test_query : async function (config, query) {
        return new Promise(async (resolve, reject) => {
            var _err = null
            var pool = await this.establish_connection(config).catch(err => {
                if(err) {reject(err)}
                _err = err
            })
            config.connection = pool
            sql_query = 'EXPLAIN ' + query
            if(!_err) {
            var result = await this.run_query(config, sql_query, 0, 1).catch(err => {
                _err = err
                if(err) {reject(err)}
            })
            resolve(result)
        } else {
            resolve(_err)
        }
            
        })
    },
    run_query : async function (config, sql_query, repeat_number, max_repeat) {
        return new Promise(async (resolve, reject) => {
            var pool = config.connection
            if(!pool) {
                pool = await this.establish_connection(config).catch(err => {
                    reject(err)
                })
            }
            // Automatically retry each query up to 25 times before erroring out
            if(!max_repeat) {max_repeat = 25}
            pool.getConnection((err, conn) => {
                if (err) {
                    reject({
                        "err": 'mysql connection errored',
                        "step": 'establish_connection (mysql variant)',
                        "description": err,
                        "resolution": `please check your SQL server authentication details and SQL server firewall`
                    })
                } else {
                conn.query(sql_query, async function (err, results) {
                    if (err) {
                        if(repeat_number) {repeat_number = repeat_number + 1}
                        else {repeat_number = 1}
                        if (repeat_number < max_repeat) {
                            var nested_err = null
                            var nested_query = await exports.run_query(config, sql_query, repeat_number).catch(err => {
                                if(repeat_number == max_repeat - 1) {{
                                    nested_err = err
                                    reject(err)
                                }}
                            })
                            if(!nested_err) {
                                resolve (nested_query)
                            }
                        } else {
                            conn.release()
                            console.log(sql_query.substring(0,50) + '... errored ' + repeat_number + ' times')
                            reject({
                                err: 'mysql query errored',
                                step: 'run_query (mysql variant)',
                                description: sql_query.substring(0,50) + '... errored ' + repeat_number + ' times',
                                resolution: `please check this query as an invalid query may have been passed. If this query was generated by the autosql module, 
                                please raise a bug report on https://github.com/walterchoi/autosql/issues`,
                                full_query: sql_query
                            })
                        }
                    } else {
                        if(repeat_number > 0) {
                            console.log(sql_query.substring(0,50) + '... errored ' + repeat_number + ' times but completed successfully')
                        }
                        conn.release()
                        if(Array.isArray(results)) {
                            resolve(results)
                        } else {
                            resolve(results.affectedRows)
                        }
                    }
                })
            }})
        })
    },
    check_database_exists : function (config) {
        if(config.schema) {
            var database = config.schema
        }
        else if (config.database) {
            var database = config.database
        }
        var sql_query_part = ""
        // Handle multiple databases being provided as an array
        if(Array.isArray(database)) {
            for (var d = 0; d < database.length; d++) {
                sql_query_part = sql_query_part +
                "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.`SCHEMATA` WHERE SCHEMA_NAME = '" + database[d] + "') THEN 1 ELSE 0 END) AS '" + database[d] + "'"
                if(d != database.length - 1) {sql_query_part = sql_query_part + ', '}
                else {sql_query_part = sql_query_part + ' '}
            }
        } else {
            // Handle multiple databases being provided
            sql_query_part = 
            "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.`SCHEMATA` WHERE SCHEMA_NAME = '" + database + "') THEN 1 ELSE 0 END) AS '" + database + "' "
        }
        
        var sql_query = "SELECT " + sql_query_part + "FROM DUAL;"
        return (sql_query)
    },
    create_database : function (config) {
        if(config.schema) {
            var database = config.schema
        }
        else if (config.database) {
            var database = config.database
        }
        var sql_query = "CREATE DATABASE `" + database + "`;"
        return(sql_query)
    },
    check_tables_exists : function (config) {
        if(config.schema) {
            var database = config.schema
        }
        else if (config.database) {
            var database = config.database
        }
        var table = config.table;

        var sql_query_part = ""
        // Handle multiple tables being provided as an array
        if(Array.isArray(table)) {
            for (var t = 0; t < table.length; t++) {
                sql_query_part = sql_query_part +
                "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table[t] + "') THEN 1 ELSE 0 END) AS '" + table[t] + "'"
                if(t != tables.length - 1) {sql_query_part = sql_query_part + ', '}
                else {sql_query_part = sql_query_part + ' '}
            }
        } else {
            // Handle multiple tables being provided
            sql_query_part = 
            "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table + "') THEN 1 ELSE 0 END) AS '" + table + "' "
        }
        
        var sql_query = "SELECT " + sql_query_part + "FROM DUAL;"
        return (sql_query)
    },
    create_table : function (config, headers, override) {
        return new Promise((resolve, reject) => {
            var sql_dialect_lookup_object = require('../config/sql_dialect.json')
            var sql_lookup_table = require('.' + sql_dialect_lookup_object[config.sql_dialect].helper_json)
    
            if(config.schema) {
                var database = config.schema
            }
            else if (config.database) {
                var database = config.database
            }
            var table = config.table;
            var collation = config.collation;

            var create_table_sql = "CREATE TABLE IF NOT EXISTS `" + database + "`.`" + table + "` (\n"
    
            var primary_sql_part = null
            var create_table_sql_part = null
            // Get each column's data and repeat for each meta_data row
            for (var h = 0; h < headers.length; h++) {
                var header_name = (Object.getOwnPropertyNames(headers[h])[0])
                var header_data = headers[h][header_name]
                
                // Set variables required for create table statement
                var column_name = header_name
                var type = header_data["type"]
                var length = header_data["length"]
                var decimal = header_data["decimal"]
                var allowNull = header_data["allowNull"]
                var unique = header_data["unique"]
                var primary = header_data["primary"]
                var index = header_data["index"]
                var auto_increment = header_data["auto_increment"]
                var def = header_data["default"]
                var comment = header_data["comment"]
                
                if(!header_data["type"]) {
                    header_data["type"] = 'varchar'
                    type = 'varchar'
                }

                if(sql_lookup_table.translate.local_to_server[type.toLowerCase()]) {
                    type = sql_lookup_table.translate.local_to_server[type.toLowerCase()]
                }
    
                if(create_table_sql_part) {
                    create_table_sql_part = ",\n `" + column_name + "` " + type 
                } else {
                    create_table_sql_part = "`" + column_name + "` " + type 
                }
    
                if(sql_lookup_table.decimals.includes(type)) {
                    if(!decimal) {decimal = 0}
                    create_table_sql_part += " (" + length + "," + decimal + ")"
                }
                else if(sql_lookup_table.require_length.includes(type) || (sql_lookup_table.optional_length.includes(type) && length)) {
                    create_table_sql_part += " (" + length + ")"
                }
                
                // If allowNull is true/false add (NOT) NULL to SQL part
                if(allowNull !== undefined) {
                    create_table_sql_part += ` ${!allowNull ? 'NOT' : ''} NULL`
                }
    
                // If unique is true then make this an unique contraint column
                if(unique === true) {
                    create_table_sql_part += " UNIQUE"
                }
    
                // If index is true then make column into an indexed column
                if(index === true) {
                    create_table_sql_part += ", INDEX (`" + column_name + "`) "
                }
    
                // If auto_increment is true then make column into an auto_incremental column
                if(auto_increment === true) {
                    create_table_sql_part += " AUTO_INCREMENT"
                }
    
                // If comment is provided then add comment to the table schema
                if(comment !== undefined) {
                    create_table_sql_part += " COMMENT '" + comment + "'"
                }
    
                // If default value is provided then add a default value to the column
                if(def !== undefined) {
                    create_table_sql_part += " DEFAULT " + def + ""
                }
    
                // If column is part of the primary key, then add column to the primary constraint index
                if(primary === true) {
                    if(primary_sql_part == null) {
                        primary_sql_part = ", PRIMARY KEY (`" + column_name + "`"
                    } else {
                        primary_sql_part += ", `" + column_name + "`"
                    }
                }
                create_table_sql += create_table_sql_part
            }
    
            // Close off primary_sql_part
            if(primary_sql_part) {
                    primary_sql_part += ")"
                    create_table_sql += primary_sql_part
            }
    
            create_table_sql = create_table_sql + ")"
            if(config.collation) {
                create_table_sql += ' COLLATE ' + config.collation + ';'
            }
            resolve (create_table_sql) 
        })
    },
    empty_table : function (config) {
        if(config.schema) {
            var database = config.schema
        }
        else if (config.database) {
            var database = config.database
        };
        var table = config.table;
        return `Delete FROM \`${database}\`.\`${table}\`;`
    },
    get_table_description : function (config, _schema, _table) {
        if(_schema && _table) {
            var database = _schema
            var table = _table
        } else {
            if(config.schema) {
                var database = config.schema
            }
            else if (config.database) {
                var database = config.database
            };
            var table = config.table;
        }

        var sql_query = "SELECT COLUMN_NAME, DATA_TYPE, " +
        "CASE WHEN NUMERIC_PRECISION IS NOT NULL AND NUMERIC_SCALE IS NOT NULL THEN CONCAT(NUMERIC_PRECISION,',',NUMERIC_SCALE) " +
        "WHEN NUMERIC_PRECISION IS NOT NULL AND NUMERIC_SCALE IS NULL THEN NUMERIC_PRECISION " + 
        "WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN CHARACTER_MAXIMUM_LENGTH ELSE NULL END AS LENGTH, " + 
        "IS_NULLABLE, COLUMN_KEY " +
        "FROM INFORMATION_SCHEMA.COLUMNS " + 
        "WHERE TABLE_SCHEMA = '" + database + "' " +
        "AND TABLE_NAME = '" + table + "';"
        return(sql_query)
    },
    alter_table : function (config, changed_headers) {
        return new Promise((resolve, reject) => {
            var sql_dialect_lookup_object = require('../config/sql_dialect.json')
            var sql_lookup_table = require('.' + sql_dialect_lookup_object[config.sql_dialect].helper_json)
    
            if(config.schema) {
                var database = config.schema
            }
            else if (config.database) {
                var database = config.database
            }
            var table = config.table;
            var sql_query = "ALTER TABLE `" + database + "`.`" + table + "` \n"
            var sql_query_part = null

            // New columns to add to table
            if(changed_headers.new) {
                for(var n = 0; n < changed_headers.new.length; n++) {
                    var column_name = Object.getOwnPropertyNames(changed_headers.new[n])[0]
                    var header_data = changed_headers.new[n][column_name]
                
                    // Set variables required for new column in alter table statement
                    var type = header_data["type"]
                    var length = header_data["length"]
                    var allowNull = header_data["allowNull"]
                    var unique = header_data["unique"]
                    var index = header_data["index"]
                    var auto_increment = header_data["auto_increment"]
                    var def = header_data["default"]
                    var comment = header_data["comment"]
                
                    if(sql_lookup_table.translate.local_to_server[header_data["type"]]) {
                        type = sql_lookup_table.translate.local_to_server[header_data["type"]]
                    }
    
                    if(sql_query_part) {
                        sql_query_part = ",\n ADD `" + column_name + "` " + type 
                    } else {
                        sql_query_part = "ADD `" + column_name + "` " + type 
                    }
    
                    if(sql_lookup_table.decimals.includes(type)) {
                        if(!decimal) {decimal = 0}
                        sql_query_part += " (" + length + "," + decimal + ")"
                    }
                    else if(sql_lookup_table.require_length.includes(type) || (sql_lookup_table.optional_length.includes(type) && length)) {
                        sql_query_part += " (" + length + ")"
                    }
                
                    // If allowNull is true/false add (NOT) NULL to SQL part
                    if(allowNull !== undefined) {
                        sql_query_part += ` ${!allowNull ? 'NOT' : ''} NULL`
                    }
    
                    // If unique is true then make this an unique contraint column
                    if(unique === true) {
                        sql_query_part += " UNIQUE"
                    }
    
                    // If index is true then make column into an indexed column
                    if(index === true) {
                        sql_query_part += ", INDEX (`" + column_name + "`) "
                    }
    
                    // If auto_increment is true then make column into an auto_incremental column
                    if(auto_increment === true) {
                        sql_query_part += " AUTO_INCREMENT"
                    }
    
                    // If comment is provided then add comment to the table schema
                    if(comment !== undefined) {
                        sql_query_part += " COMMENT '" + comment + "'"
                    }
    
                    // If default value is provided then add a default value to the column
                    if(def !== undefined) {
                        sql_query_part += " DEFAULT " + def + ""
                    }
                    sql_query += sql_query_part
                }
            }
            if(changed_headers.alter) {
                for(var a = 0; a < changed_headers.alter.length; a++) {
                    var column_name = Object.getOwnPropertyNames(changed_headers.alter[a])[0]
                    var header_data = changed_headers.alter[a][column_name]

                    // Set variables required for altering a column in alter table statement
                    var type = header_data["type"]
                    var length = header_data["length"]
                    var decimal = header_data["decimal"]
                    var allowNull = header_data["allowNull"]

                    if(sql_lookup_table.translate.local_to_server[header_data["type"]]) {
                        type = sql_lookup_table.translate.local_to_server[header_data["type"]]
                    }
                    
                    if(sql_query_part) {
                        sql_query_part = ",\n MODIFY COLUMN `" + column_name + "` " + type 
                    } else {
                        sql_query_part = " MODIFY COLUMN `" + column_name + "` " + type 
                    }
        
                    if(sql_lookup_table.decimals.includes(type)) {
                        if(!decimal) {decimal = 0}
                        sql_query_part += " (" + length + "," + decimal + ")"
                    }
                    else if(sql_lookup_table.require_length.includes(type) || (sql_lookup_table.optional_length.includes(type) && length)) {
                        sql_query_part += " (" + length + ")"
                    }
                    
                    // If allowNull is true/false add (NOT) NULL to SQL part
                    if(allowNull !== undefined) {
                        sql_query_part += ` ${!allowNull ? 'NOT' : ''} NULL`
                    }
                    sql_query += sql_query_part
                }
            }
            resolve(sql_query)
        })  
    },
    create_insert_string : function (config, data) {
        return new Promise(resolve => {
            var groupings = require('./groupings.json')
            var int_group = groupings.int_group
            var special_int_group = groupings.special_int_group

            if(config.schema) {
                var database = config.schema
            }
            else if (config.database) {
                var database = config.database
            }
            var table = config.table
            var insert_type = config.insert_type
            var metaData = config.meta_data

            var headers = []
            metaData.map(header => 
                headers.push(Object.getOwnPropertyNames(header)[0])
            )
            
            var sql_query = `INSERT ${insert_type == 'IGNORE' ? 'IGNORE' : ''} INTO ` + '`' + database + '`.`' + table + '` ' 
            var column_sql = "(`" + headers.join("`, `") + "`) "
            var replace_sql = ''

            if(insert_type == 'REPLACE') {
                replace_sql = '\nON DUPLICATE KEY UPDATE '
                for (var h = 0; h < headers.length; h++) {
                    replace_sql += "`" + headers[h] + "`=VALUES(`" + headers[h] + "`)"
                    if(h != headers.length - 1) {
                        replace_sql += ", "
                    }
                }
            }

            sql_query = sql_query + column_sql
            column_sql = ''

            var values_sql = " VALUES ("
            for(var d = 0; d < data.length; d++) {
                var row = data[d]
                for(var h = 0; h < headers.length; h++) {
                    var value = row[headers[h]]
                    if(value === null || value == '' || value == 'null') {
                        values_sql += 'null'
                    } else {
                        if(int_group.includes(metaData[h][headers[h]].type) || special_int_group.includes(metaData[h][headers[h]].type)) {
                            values_sql += value
                        } else {
                            values_sql += "'" + value + "'"
                        }
                    }
                    if(h != headers.length -1) {
                        values_sql += ", "
                    } else {
                        values_sql += ") \n"
                        if(d != data.length -1) {
                            values_sql += ", ("
                        }
                    }
                }
                sql_query += values_sql
                values_sql = ''
            }
            sql_query = sql_query + replace_sql
            resolve(sql_query)
        })
    },
    start_transaction : function () {
            return('START TRANSACTION;')
    },
    commit : function () {
        return('COMMIT;')
    },
    rollback : function () {
        return('ROLLBACK;')
    }
}

            module.exports = {
                exports
            }