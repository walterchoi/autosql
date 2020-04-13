var exports = {
    check_database_exists = function (database) {
        var sql_query_part = ""
        // Handle multiple databases being provided as an array
        if(database.isArray) {
            for (var d = 0; d < database.length; d++) {
                sql_query_part = sql_query_part +
                "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + database[d] + "') THEN 1 ELSE 0 END) AS '" + database[d] + "'"
                if(d != database.length - 1) {sql_query_part = sql_query_part + ', '}
            }
        } else {
            // Handle multiple databases being provided
            sql_query_part = 
            "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + database + "') THEN 1 ELSE 0 END) AS '" + database + "' "
        }
        
        var sql_query = "SELECT " + sql_query_part + ";"
        return (sql_query)
    },
    check_tables_exists = function (database, table) {
        var sql_query_part = ""
        // Handle multiple tables being provided as an array
        if(table.isArray) {
            for (var t = 0; t < table.length; t++) {
                sql_query_part = sql_query_part +
                "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table[t] + "') THEN 1 ELSE 0 END) AS '" + table[t] + "'"
                if(t != tables.length - 1) {sql_query_part = sql_query_part + ', '}
            }
        } else {
            // Handle multiple tables being provided
            sql_query_part = 
            "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table[t] + "') THEN 1 ELSE 0 END) AS '" + table[t] + "'"
        }
        
        var sql_query = "SELECT " + sql_query_part + ";"
        return (sql_query)
    }
}

            module.exports = {
                exports
            }