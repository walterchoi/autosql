var exports = {
    check_tables_exists = function (database, table, language) {
        var mysql_query_part = ""
        if(table.isArray) {
            for (var t = 0; t < table.length; t++) {
                mysql_query_part = mysql_query_part +
                "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table[t] + "') THEN 1 ELSE 0 END) AS '" + table[t] + "'"
                if(t != tables.length - 1) {mysql_query_part = mysql_query_part + ', '}
            }
        } else {
            mysql_query_part = 
            "(CASE WHEN EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA = '" + database + "' AND TABLE_NAME = '" + table + "') THEN 1 ELSE 0 END) AS '" + table + "' "
        }
        
        var mysql_query = "SELECT " + mysql_query_part + "FROM DUAL"
    }
}

            module.exports = {
                exports
            }