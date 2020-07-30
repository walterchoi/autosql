var exports = {
    establish_connection : async function (key) {
        return new Promise((resolve, reject) => {
            if(!key.host || !key.username || !key.password) {
                reject({
                    err: 'required configuration options not set for pgsql connection',
                    step: 'establish_connection (pgsql variant)',
                    description: 'invalid configuration object provided to autosql automated step',
                    resolution: `please provide supported value for ${!key.host ? 'host' : ''} ${!key.username ? 'username' : ''}
                    ${!key.password ? 'password' : ''} in configuration object, additional details can be found in the documentation`
                })
            }
            var { Pool, Client } = require('pg')
            var pg_config = {
                host: key.host,
                user: key.username,
                password: key.password,
                port: key.port,
                idleTimeoutMillis: 30000,
                connectionTimeoutMillis: 2000,
                max: 25
            }
            if(key.database) {
                pg_config.database = key.database
                if(!key.schema) {
                    pg_config.schema = key.database
                }
            }
            if(key.schema) {
                pg_config.schema = key.schema
                if(!key.database) {
                    pg_config.database = key.schema
                }
            }
            if(key.ssh_stream) {
                pg_config.stream = key.ssh_stream
            }
            var connection = new Pool(pg_config)
            connection.on('error', (err, client) => {
                reject({
                    err: 'pgsql connection was invalid',
                    step: 'establish_connection (pgsql variant)',
                    description: 'Unexpected error on idle client: ' + err,
                    resolution: `please check your connection to the pgsql client`
                    })
              })
            resolve(connection)
        })
    },
    run_query : async function (config, sql_query, repeat_number, max_repeat) {
        return new Promise(async (resolve, reject) => {
            var pool = config.connection
            if(!pool || typeof config.connection.connect != 'function') {
                pool = await this.establish_connection(config).catch(err => {
                    console.log(err)
                    reject(err)
                })
            }
            // Automatically retry each query up to 25 times before erroring out
            if(!max_repeat) {max_repeat = 25}
            //console.log(sql_query)
            pool.connect(async (err, client, release) => {
                if(err) {
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
                        release()
                        console.log(sql_query.substring(0,50) + '... errored ' + repeat_number + ' times')
                        reject({
                            err: err,
                            step: 'run_query (pgsql variant)',
                            description: sql_query.substring(0,50) + '... errored ' + repeat_number + ' times',
                            resolution: `please check this query as an invalid query may have been passed. If this query was generated by the autosql module, 
                            please raise a bug report on https://github.com/walterchoi/autosql/issues`
                        })
                    }
                } else {
                client.query(sql_query)
                    .then(results => {
                        if(repeat_number > 0) {
                            console.log(sql_query.substring(0,50) + '... errored ' + repeat_number + ' times but completed successfully')
                        }
                        release()
                        if(results.command == 'INSERT') {
                            resolve(results.rowCount)
                        }
                        else if(results.command == 'SELECT') {
                            resolve(results.rows)
                        } else {
                            resolve(results.rows)
                        }
                    })
                    .catch(async err => {
                        if(repeat_number) {repeat_number = repeat_number + 1}
                        else {repeat_number = 1}
                        if (repeat_number < max_repeat) {
                        release()
                        var nested_err = null
                        var nested_query = await exports.run_query(config, sql_query, repeat_number).catch(err => {
                            if(repeat_number == max_repeat - 1) {{
                                nested_err = err
                                console.log(err)
                                if(sql_query.length > 1000) {
                                    console.log(sql_query.substring(0, 1000))
                                    console.log(sql_query.substring(-200))
                                } else {
                                    console.log(sql_query)
                                }
                                reject(err)
                            }}
                        })
                        if(!nested_err) {
                        resolve (nested_query)
                    }
                    } else {
                        release()
                        var err_obj = {
                            err: err,
                            step: 'run_query (pgsql variant)',
                            description: sql_query.substring(0,50) + '... errored ' + repeat_number + ' times',
                            resolution: `please check this query as an invalid query may have been passed. If this query was generated by the autosql module, 
                            please raise a bug report on https://github.com/walterchoi/autosql/issues`
                        }
                        reject(err_obj)
                    }
                    })
                }}
            )}
        )
    },
    check_database_exists : function (config) {
        if(config.schema) {
                var database = config.schema
            }
            else if (config.database) {
                var database = config.database
            };
        var sql_query_part = ""
        // Handle multiple databases being provided as an array
        if(database.isArray) {
            for (var d = 0; d < database.length; d++) {
                sql_query_part = sql_query_part +
                "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '" + database[d] + `') THEN 1 ELSE 0 END) AS "` + database[d] + `"`
                if(d != database.length - 1) {sql_query_part = sql_query_part + ', '}
            }
        } else {
            // Handle multiple databases being provided
            sql_query_part = 
            "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '" + database + `') THEN 1 ELSE 0 END) AS "` + database + `"`
        }
        var sql_query = "SELECT " + sql_query_part + ";"
        return (sql_query)
    },
    test_connection : async function (key) {
        return new Promise(async (resolve, reject) => {
            var _err = null
            var pool = await this.establish_connection(key).catch(err => {
                if(err) {reject(err)}
                _err = err
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
                reject(_err)
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
    create_database : function (config) {
        if(config.schema) {
                var database = config.schema
            }
            else if (config.database) {
                var database = config.database
            };
        var sql_query = 'CREATE SCHEMA "' + database + '";'
        return(sql_query)
    },
    check_tables_exists : function (config) {
        if(config.schema) {
                var database = config.schema
            }
            else if (config.database) {
                var database = config.database
            };
        var table = config.table;
        var sql_query_part = ""
        // Handle multiple tables being provided as an array
        if(table.isArray) {
            for (var t = 0; t < table.length; t++) {
                sql_query_part = sql_query_part +
                "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table[t] + `') THEN 1 ELSE 0 END) AS "` + table[t] + '"'
                if(t != tables.length - 1) {sql_query_part = sql_query_part + ', '}
            }
        } else {
            // Handle multiple tables being provided
            sql_query_part = 
            "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table + `') THEN 1 ELSE 0 END) AS "` + table + '"'
        }
        var sql_query = "SELECT " + sql_query_part + ";"
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
            };
            var table = config.table;
            var collation = config.collation;

            var create_table_sql = `CREATE TABLE IF NOT EXISTS "` + database + `"."` + table + `" (\n`
    
            var primary_sql_part = null
            var create_table_sql_part = null
            var index_create_sql = ''
            var auto_increments_sql = ''
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
                
                if(sql_lookup_table.translate.local_to_server[header_data["type"]]) {
                    type = sql_lookup_table.translate.local_to_server[header_data["type"]]
                }
    
                if(create_table_sql_part) {
                    create_table_sql_part = `,\n "` + column_name + `" ` + type 
                } else {
                    create_table_sql_part = `"` + column_name + `" ` + type 
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
    
                // If index is true then make column into an indexed column (separate query done later)
                if(index === true) {
                    index_create_sql += `CREATE ${unique === true ? 'UNIQUE' : ''} INDEX "${table + '_' + column_name}" ON "${database}"."${table}" ("${column_name}");\n`
                }

                // If auto_increment is true then make column into an auto_incremental column (separate query done later)
                if(auto_increment === true) {
                    auto_increments.push(column_name)
                    if(def !== undefined) {
                        reject({
                            err: 'default value was specified for an auto_increment column',
                            step: 'create table (pgsql variant)',
                            description: `${column_name} has been specified as an auto_incremental column with a default value`,
                            resolution: `please check the ${column_name} column, as it has been specified to be ann auto_incremental column with a default value which are contradictory. 
                            If this query was generated by the autosql module, 
                            please raise a bug report on https://github.com/walterchoi/autosql/issues`
                        })
                    } else {
                        auto_increments_sql += `CREATE SEQUENCE ${table + '_' + column_name};\n`
                        def = "NEXTVAL('" + table + '_' + column_name + "')"
                    }
                }
    
                // If comment is provided then add comment to the table schema
                if(comment !== undefined) {
                    index_create_sql += `COMMENT ON COLUMN ${table}.${column_name} is '${comment}';`
                }
    
                // If default value is provided then add a default value to the column
                if(def !== undefined) {
                    create_table_sql_part += " SET DEFAULT " + def + ""
                }
    
                // If column is part of the primary key, then add column to the primary constraint index
                if(primary === true) {
                    if(primary_sql_part == null) {
                        primary_sql_part = `, PRIMARY KEY ("` + column_name + `"`
                    } else {
                        primary_sql_part += `, "` + column_name + `"`
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
                create_table_sql += ' COLLATE ' + config.collation + ';\n'
            } else {
                create_table_sql = create_table_sql + ";\n"
            }
            if(auto_increments_sql.length > 1) {
                if(create_table_sql.isArray) {
                    create_table_sql = create_table_sql.push(auto_increments_sql)
                } else {
                    create_table_sql = [auto_increments_sql, create_table_sql]
                }
            }
            if(index_create_sql.length > 1) {
                if(create_table_sql.isArray) {
                    create_table_sql = create_table_sql.push(index_create_sql)
                } else {
                    create_table_sql = [create_table_sql, index_create_sql]
                }
            }
            resolve (create_table_sql) 
        })
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

        var sql_query = `SELECT
        DISTINCT ON (c.COLUMN_NAME) COLUMN_NAME, c.DATA_TYPE,
        CASE
            WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NOT NULL THEN CONCAT(c.NUMERIC_PRECISION,',',c.NUMERIC_SCALE)
            WHEN c.NUMERIC_PRECISION IS NOT NULL AND c.NUMERIC_SCALE IS NULL THEN c.NUMERIC_PRECISION::varchar
            WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN c.CHARACTER_MAXIMUM_LENGTH::varchar
            ELSE NULL
            END AS LENGTH,
        c.IS_NULLABLE, CASE WHEN i.indexdef LIKE '%PRIMARY%' THEN 'PRIMARY'
        WHEN i.indexdef LIKE '%UNIQUE%' THEN 'UNIQUE'
        WHEN i.indexdef LIKE '%INDEX%' THEN 'INDEX'
        ELSE NULL
        END AS COLUMN_KEY
        FROM INFORMATION_SCHEMA.COLUMNS AS C
        INNER JOIN pg_indexes AS i ON i.TABLENAME = c.TABLE_NAME
        WHERE c.TABLE_SCHEMA = '${database}' AND c.TABLE_NAME = '${table}';`
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
            };
            var table = config.table;
            var sql_query = `ALTER TABLE "` + database + `"."` + table + `" \n`
            var sql_query_part = null
            var query_array = []
            var index_create_sql = ''
            var auto_increments = []
            var auto_increments_sql = ''

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
                        sql_query_part = `,\n ADD COLUMN IF NOT EXISTS "` + column_name + `" ` + type 
                    } else {
                        sql_query_part = `ADD COLUMN IF NOT EXISTS "` + column_name + `" ` + type 
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
                        index_create_sql += `CREATE UNIQUE INDEX IF NOT EXISTS "${table + '_' + column_name}" ON "${database}"."${table}" ("${column_name}");\n`
                    } else
                    // If index is true then make column into an indexed column (separate query done later)
                    if(index === true) {
                        index_create_sql += `CREATE INDEX IF NOT EXISTS "${table + '_' + column_name}" ON "${database}"."${table}" ("${column_name}");\n`
                    }
    
                    // If auto_increment is true then make column into an auto_incremental column (separate query done later)
                    if(auto_increment === true) {
                        auto_increments.push(column_name)
                        if(def !== undefined) {
                            reject({
                                err: 'default value was specified for an auto_increment column',
                                step: 'create table (pgsql variant)',
                                description: `${column_name} has been specified as an auto_incremental column with a default value`,
                                resolution: `please check the ${column_name} column, as it has been specified to be ann auto_incremental column with a default value which are contradictory. 
                                If this query was generated by the autosql module, 
                                please raise a bug report on https://github.com/walterchoi/autosql/issues`
                            })
                        } else {
                            auto_increments_sql += `CREATE SEQUENCE "${table + '_' + column_name}";\n`
                            def = "NEXTVAL('" + table + '_' + column_name + "')"
                        }
                    }
    
                    // If comment is provided then add comment to the table schema
                    if(comment !== undefined) {
                        sql_query_part += " COMMENT '" + comment + "'"
                    }
    
                    // If default value is provided then add a default value to the column
                    if(def !== undefined) {
                        sql_query_part += " DEFAULT " + def + ""
                    }

                    if(auto_increments_sql.length > 1) {
                        query_array.push(auto_increments_sql)
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
                        sql_query_part = `,\n ALTER COLUMN "` + column_name + `" TYPE ` + type 
                    } else {
                        sql_query_part = ` ALTER COLUMN "` + column_name + `" TYPE ` + type 
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
                        if(allowNull == true) {
                        sql_query_part += `,\n ALTER COLUMN "` + column_name + `" DROP NOT NULL`
                        } else {
                            sql_query_part += ` NOT NULL`
                        }
                    }
                    sql_query += sql_query_part
                }
            }
            sql_query += ";"
            if(query_array.length >= 1) {
                query_array.push(sql_query)
                if(index_create_sql.length > 1) {
                    query_array.push(index_create_sql)
                }
                resolve(query_array)
            } else {
                resolve(sql_query)
            }
        })  
    },
    create_insert_string : function (config, data) {
        return new Promise((resolve, reject) => {
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
            
            var sql_query = `INSERT INTO "` + database + `"."` + table + `" ` 
            var column_sql = `("` + headers.join(`", "`) + `") `
            var replace_sql = ''
            
            if(insert_type == 'REPLACE') {
                var keys = config.keys
                if(!keys) {
                    reject({
                        err: 'No unique or primary key columns was provided for REPLACE type insert',
                        step: 'create_insert_string (pgsql variant)',
                        description: 'at least one primary key or unique constraint index is required for a REPLACE type insert for pgsql',
                        resolution: `please set one primary or unique constraint to table: ${table} and provide this within the config in either 'key' values`
                    })
                }
                if(Object.getOwnPropertyNames(keys)[0] === undefined) {
                    replace_sql = ''
                } else {
                if(Array.isArray(keys[Object.getOwnPropertyNames(keys)[0]])) {
                    var key_columns = keys[Object.getOwnPropertyNames(keys)[0]]
                } else {
                    var key_columns = [keys[Object.getOwnPropertyNames(keys)[0]]]
                }
                replace_sql = 'ON CONFLICT ON CONSTRAINT ' + `"` + Object.getOwnPropertyNames(keys)[0] + `" \n` + 'DO UPDATE SET '
                var non_key_headers = Array.from(headers)
                for (var h = 0; h < non_key_headers.length; h++) {
                    if(key_columns.includes(non_key_headers[h])) {
                        non_key_headers.splice(h, 1)
                    }
                }
                if(non_key_headers.length > 0) {
                    replace_sql += `("` + non_key_headers.join('", "') + `") = (excluded."` + non_key_headers.join('", excluded."') + `")`
                } else {
                    replace_sql = 'ON CONFLICT ON CONSTRAINT ' + `"` + Object.getOwnPropertyNames(keys)[0] + `" \n` + 'DO NOTHING'
                }}
            }

            if(insert_type == 'IGNORE') {
                var keys = config.keys
                if(keys) {
                    replace_sql = 'ON CONFLICT ON CONSTRAINT ' + `"` + Object.getOwnPropertyNames(keys)[0] + `" \n` + 'DO NOTHING'
                } else {
                    replace_sql = ''
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
                    values_sql += ") "
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
    find_constraint : function (config) {
        var sql_query = `SELECT COLUMN_NAME, CONSTRAINT_NAME FROM 
        information_schema.constraint_column_usage
        WHERE table_schema = '${config.schema}' AND TABLE_NAME = '${config.table}'`
        return sql_query
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