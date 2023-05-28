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
                        await writefile('./failed_query.txt', sql_query)
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
    check_database_exists : function (config, schema_name) {
        if(schema_name) {
            var database = schema_name
        }
        else if(config.schema) {
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
    create_database : function (config, schema_name) {
        if(schema_name) {
            var database = schema_name
        }
        else if(config.schema) {
            var database = config.schema
        }
        else if (config.database) {
            var database = config.database
        }
        var sql_query = "CREATE DATABASE `" + database + "`;"
        return(sql_query)
    },
    check_tables_exists : function (config, table_name) {
        if(config.schema) {
            var database = config.schema
        }
        else if (config.database) {
            var database = config.database
        }
        var table = table_name || config.table;

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

                if(type == 'json') {
                    if(index) {
                        create_table_sql_part += ' , '
                    }
                    create_table_sql_part += ` CHECK (JSON_VALID(${column_name}))`
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
        return new Promise(async (resolve, reject) => {
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
                        sql_query_part += ", ADD INDEX (`" + column_name + "`) "
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
            await writefile('./alter_query_last.txt', sql_query)
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
                    if(value === undefined || value === 'undefined' || value === null || value == '' || value == 'null') {
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
    },
    tables_for_split : function () {
        return(`CREATE VIEW \`tables_for_split\` AS select \`cols\`.\`TABLE_NAME\` AS \`TABLE_NAME\`,\`cols\`.\`table_schema\` AS \`table_schema\`,\`cols\`.\`COLUMNS\` AS \`COLUMNS\`,group_concat(\`kcu\`.\`COLUMN_NAME\` separator ', ') AS \`key_name\`,count(\`kcu\`.\`COLUMN_NAME\`) AS \`key_count\` from (((select \`information_schema\`.\`columns\`.\`TABLE_NAME\` AS \`TABLE_NAME\`,\`information_schema\`.\`columns\`.\`TABLE_SCHEMA\` AS \`table_schema\`,count(0) AS \`COLUMNS\` from \`information_schema\`.\`columns\` group by \`information_schema\`.\`columns\`.\`TABLE_NAME\`,\`information_schema\`.\`columns\`.\`TABLE_SCHEMA\` having count(0) > 200 order by count(0) desc) \`cols\` join \`information_schema\`.\`table_constraints\` \`tc\` on(\`tc\`.\`TABLE_SCHEMA\` = \`cols\`.\`table_schema\` and \`tc\`.\`TABLE_NAME\` = \`cols\`.\`TABLE_NAME\` and \`tc\`.\`CONSTRAINT_TYPE\` = 'PRIMARY KEY')) join \`information_schema\`.\`key_column_usage\` \`kcu\` on(\`tc\`.\`CONSTRAINT_CATALOG\` = \`kcu\`.\`CONSTRAINT_CATALOG\` and \`tc\`.\`CONSTRAINT_SCHEMA\` = \`kcu\`.\`CONSTRAINT_SCHEMA\` and \`tc\`.\`CONSTRAINT_NAME\` = \`kcu\`.\`CONSTRAINT_NAME\` and \`tc\`.\`TABLE_SCHEMA\` = \`kcu\`.\`TABLE_SCHEMA\` and \`tc\`.\`TABLE_NAME\` = \`kcu\`.\`TABLE_NAME\`)) group by \`cols\`.\`TABLE_NAME\`,\`cols\`.\`table_schema\`,\`cols\`.\`COLUMNS\` ;`)
    },
    split_table_columns : function () {
        return(`CREATE VIEW \`split_table_columns\` AS select coalesce(\`st_c\`.\`split_table_name\`,concat(\`t\`.\`TABLE_NAME\`,'_part',truncate(row_number() over ( partition by \`t\`.\`TABLE_NAME\`,\`t\`.\`table_schema\` order by \`c\`.\`ORDINAL_POSITION\` desc) / (100 - \`t\`.\`key_count\`),0) + 1)) AS \`split_table_name\`,\`t\`.\`TABLE_NAME\` AS \`TABLE_NAME\`,\`t\`.\`table_schema\` AS \`table_schema\`,\`c\`.\`COLUMN_NAME\` AS \`COLUMN_NAME\`,coalesce(\`st_c\`.\`table_num\`,truncate(row_number() over ( partition by \`t\`.\`TABLE_NAME\`,\`t\`.\`table_schema\` order by \`c\`.\`ORDINAL_POSITION\` desc) / (100 - \`t\`.\`key_count\`),0) + 1) AS \`table_num\`,coalesce(\`st_c\`.\`col_num\`,(row_number() over ( partition by \`t\`.\`TABLE_NAME\`,\`t\`.\`table_schema\` order by \`c\`.\`ORDINAL_POSITION\` desc) - 1) MOD (100 - \`t\`.\`key_count\`) + 1 + \`t\`.\`key_count\`) AS \`col_num\`,\`c\`.\`CHARACTER_MAXIMUM_LENGTH\` AS \`CHARACTER_MAXIMUM_LENGTH\` from (((\`autosql_admin\`.\`tables_for_split\` \`t\` join \`information_schema\`.\`columns\` \`c\` on(\`t\`.\`TABLE_NAME\` = \`c\`.\`TABLE_NAME\` and \`t\`.\`table_schema\` = \`c\`.\`TABLE_SCHEMA\`)) left join \`information_schema\`.\`key_column_usage\` \`kcu\` on(\`kcu\`.\`TABLE_SCHEMA\` = \`t\`.\`table_schema\` and \`kcu\`.\`TABLE_NAME\` = \`t\`.\`TABLE_NAME\` and \`kcu\`.\`COLUMN_NAME\` = \`c\`.\`COLUMN_NAME\` and \`kcu\`.\`CONSTRAINT_NAME\` = 'PRIMARY')) left join \`autosql_admin\`.\`split_tables\` \`st_c\` on(\`st_c\`.\`table_name\` = \`t\`.\`TABLE_NAME\` and \`st_c\`.\`table_schema\` = \`t\`.\`table_schema\` and \`st_c\`.\`column_name\` = \`c\`.\`COLUMN_NAME\`)) where \`kcu\`.\`CONSTRAINT_NAME\` is null ;`)
    },
    id_columns_for_split : function () {
        return(`CREATE VIEW \`id_columns_for_split\` AS select \`st\`.\`split_table_name\` AS \`split_table_name\`,\`t\`.\`TABLE_NAME\` AS \`TABLE_NAME\`,\`t\`.\`table_schema\` AS \`table_schema\`,\`kcu\`.\`COLUMN_NAME\` AS \`COLUMN_NAME\`,coalesce(\`st_c\`.\`table_num\`,\`st\`.\`table_num\`) AS \`table_num\`,coalesce(\`st_c\`.\`col_num\`,row_number() over ( partition by \`t\`.\`TABLE_NAME\`,\`t\`.\`table_schema\`,\`st\`.\`table_num\` order by \`kcu\`.\`COLUMN_NAME\` desc)) AS \`col_num\` from (((\`autosql_admin\`.\`tables_for_split\` \`t\` join \`autosql_admin\`.\`split_table_columns\` \`st\` on(\`st\`.\`TABLE_NAME\` = \`t\`.\`TABLE_NAME\` and \`st\`.\`table_schema\` = \`t\`.\`table_schema\`)) join \`information_schema\`.\`key_column_usage\` \`kcu\` on(\`t\`.\`table_schema\` = \`kcu\`.\`TABLE_SCHEMA\` and \`t\`.\`TABLE_NAME\` = \`kcu\`.\`TABLE_NAME\` and \`kcu\`.\`CONSTRAINT_NAME\` = 'PRIMARY')) left join \`autosql_admin\`.\`split_tables\` \`st_c\` on(\`st_c\`.\`table_name\` = \`t\`.\`TABLE_NAME\` and \`st_c\`.\`table_schema\` = \`t\`.\`table_schema\` and \`kcu\`.\`COLUMN_NAME\` = \`st_c\`.\`column_name\` and \`st_c\`.\`table_num\` = \`st\`.\`table_num\`)) group by \`t\`.\`TABLE_NAME\`,\`t\`.\`table_schema\`,\`kcu\`.\`COLUMN_NAME\`,\`st\`.\`table_num\` ;`)
    },
    check_if_tables_require_split_sql : function () {
        return(`SELECT st.split_table_name, st.table_schema,
        CASE WHEN (SELECT t.TABLE_NAME
        FROM information_schema.tables t WHERE t.TABLE_SCHEMA = st.table_schema AND t.TABLE_NAME = st.split_table_name) IS NOT NULL THEN 1 ELSE 0 END AS _exists
         FROM autosql_admin.split_tables st
        GROUP BY st.split_table_name, st.table_schema
        HAVING _exists = 0;`)
    },
    create_split_tables_sql : function () {
        return(`SELECT 
        st.split_table_name,
        st.table_name,
        st.table_schema,
        GROUP_CONCAT(DISTINCT(st.column_name) ORDER BY st.col_num ASC SEPARATOR ', '),
        GROUP_CONCAT(DISTINCT(CASE WHEN sa.NON_UNIQUE = 1 THEN sa.COLUMN_NAME END) ORDER BY st.col_num ASC SEPARATOR ', ') AS indexes,
        GROUP_CONCAT(DISTINCT(CASE WHEN sa.NON_UNIQUE = 0 THEN sa.COLUMN_NAME END) ORDER BY st.col_num ASC SEPARATOR ', ') AS uniques,
        ts.key_name,
        CONCAT('CREATE TABLE ', st.table_schema, '.', st.split_table_name, ' (PRIMARY KEY (', ts.key_name, ')', 
        COALESCE(CASE WHEN SUM(CASE WHEN sa.NON_UNIQUE = 0 THEN 1 ELSE 0 END) > 0 OR SUM(CASE WHEN sa.NON_UNIQUE = 1 THEN 1 ELSE 0 END) > 0 THEN ', ' END, ''),
        COALESCE(CASE WHEN SUM(CASE WHEN sa.NON_UNIQUE = 0 THEN 1 ELSE 0 END) > 0 THEN GROUP_CONCAT(DISTINCT(CASE WHEN sa.NON_UNIQUE = 0 THEN CONCAT('UNIQUE(',sa.COLUMN_NAME,')') END) ORDER BY st.col_num ASC SEPARATOR ', ') END, ''),
        COALESCE(CASE WHEN SUM(CASE WHEN sa.NON_UNIQUE = 0 THEN 1 ELSE 0 END) > 0 AND SUM(CASE WHEN sa.NON_UNIQUE = 1 THEN 1 ELSE 0 END) > 0 THEN ', ' END, ''),
        COALESCE(CASE WHEN SUM(CASE WHEN sa.NON_UNIQUE = 1 THEN 1 ELSE 0 END) > 0 THEN GROUP_CONCAT(DISTINCT(CASE WHEN sa.NON_UNIQUE = 1 THEN CONCAT('INDEX(',sa.COLUMN_NAME,')') END) ORDER BY st.col_num ASC SEPARATOR ', ') END, ''),
        ') AS SELECT ', 
        GROUP_CONCAT(DISTINCT(st.column_name) ORDER BY st.col_num ASC SEPARATOR ', '), ' FROM ', st.table_schema, '.', st.table_name, ';') AS create_statement
         FROM autosql_admin.split_tables st
        JOIN information_schema.columns c ON st.table_name = c.TABLE_NAME
        AND st.table_schema = c.TABLE_SCHEMA
        AND st.column_name = c.COLUMN_NAME
        JOIN autosql_admin.tables_for_split ts ON ts.TABLE_NAME = st.table_name
        AND ts.table_schema = st.table_schema
        LEFT JOIN information_schema.statistics sa ON sa.table_name = st.table_name AND sa.table_schema = st.table_schema AND sa.COLUMN_NAME = st.column_name
        LEFT JOIN information_schema.tables t ON t.TABLE_SCHEMA = st.table_schema AND t.TABLE_NAME = st.split_table_name
        WHERE t.TABLE_NAME IS NULL
        GROUP BY st.split_table_name;`)
    },
    check_table_is_split_sql : function (table_name) {
        return(`SELECT st.* FROM autosql_admin.split_tables st
        WHERE st.table_name = '${table_name}';`)
    },
    next_table_split_sql : function (table_name) {
        return(`SELECT 
        CONCAT(SUBSTRING_INDEX(st.split_table_name, 'part', 1), 'part',
        CAST(SUBSTRING_INDEX(st.split_table_name, 'part', -1) AS SIGNED)+1) AS split_table_name,
        st.TABLE_NAME, 
        st.table_schema,
        st.COLUMN_NAME,
        st.table_num+1 AS table_num,
        st.col_num
         FROM autosql_admin.id_columns_for_split st
                WHERE st.split_table_name = '${table_name}';`)
    },
    get_next_col_number : function (table_name) {
        return(`
        SELECT
        split_table_name,
        table_name,
        table_schema,
        table_num,
        MIN(col_num) AS start_id,
        MAX(col_num) AS end_id
        FROM (
            SELECT
            split_table_name,
            table_name,
            table_schema,
            table_num,
            col_num,
            col_num - ROW_NUMBER() OVER (ORDER BY col_num) AS grp
        FROM autosql_admin.split_tables
        WHERE split_table_name = '${table_name}'
        ) t
        GROUP BY split_table_name, grp
        ORDER BY end_id asc;
        `)
    },
    insert_split_table_values : function (values) {
        var sql_query = `INSERT INTO autosql_admin.split_tables (split_table_name, table_name, table_schema, column_name, table_num, col_num) VALUES \n`
        for (var v = 0 ; v < values.length; v++) {
            var value_query = `('${values[v].split_table_name}', '${values[v].table_name}', '${values[v].table_schema}', '${values[v].column_name}', ${values[v].table_num}, ${values[v].col_num})`
            if(v < values.length-1) {
                value_query = value_query + ',\n'
            }
            sql_query = sql_query + value_query
        }
        return(sql_query)
    },
    split_tables : function () {
        var meta_data = [
            {
                "split_table_name": {
                    "type": "varchar",
                    "length": 64,
                    "allowNull": false,
                    "unique": false,
                    "primary": true,
                    "index": false,
                    "comment": "Name of the new table"
            }},
            {
                "table_name": {
                    "type": "varchar",
                    "length": 64,
                    "allowNull": false,
                    "unique": false,
                    "primary": true,
                    "index": false,
                    "comment": "Name of the original table"
            }},
            {
                "table_schema": {
                    "type": "varchar",
                    "length": 64,
                    "allowNull": false,
                    "unique": false,
                    "primary": true,
                    "index": false,
                    "comment": "schema for tables"
            }},
            {
                "column_name": {
                    "type": "varchar",
                    "length": 64,
                    "allowNull": false,
                    "unique": false,
                    "primary": true,
                    "index": false,
                    "comment": "Name of column"
            }},
            {
                "table_num": {
                    "type": "tinyint",
                    "allowNull": true,
                    "length": 3,
                    "default": false,
                    "unique": false,
                    "primary": false,
                    "index": false,
                    "comment": "Table number"
            }},
            {
                "col_num": {
                    "type": "tinyint",
                    "allowNull": true,
                    "length": 3,
                    "default": false,
                    "unique": false,
                    "primary": false,
                    "index": false,
                    "comment": "Table number"
            }}
        ]
        return meta_data
    }
}

var writefile = async function (path, file, encoding) {
    var fs = require('fs')
    if(!encoding) {encoding = 'utf8'}
    fs.writeFile(path, file,  encoding, function (err) {
        if(err) {console.log(err)}
        return(err)
    })
  }

            module.exports = {
                exports
            }