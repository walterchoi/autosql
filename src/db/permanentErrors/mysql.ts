export const mysqlPermanentErrors = [
  // Syntax errors
  "ER_SYNTAX_ERROR", // Syntax error or access rule violation

  // Table or field issues
  "ER_NO_SUCH_TABLE", // Table doesn't exist
  "ER_BAD_FIELD_ERROR", // Unknown column
  "ER_TOO_BIG_PRECISION", // Column precision was too big
  "ER_TOO_MANY_KEYS", // Too many keys added to table

  // Constraint violations
  "ER_DUP_ENTRY", // Duplicate entry for key
  "ER_ROW_IS_REFERENCED_2", // Cannot delete or update a parent row: a foreign key constraint fails
  "ER_NO_REFERENCED_ROW_2", // Cannot add or update a child row: a foreign key constraint fails

  // Access and permission errors
  "ER_DBACCESS_DENIED_ERROR", // Access denied for user to database
  "ER_ACCESS_DENIED_ERROR", // Access denied for user
  "ER_SPECIFIC_ACCESS_DENIED_ERROR", // Access denied; you need (at least one of) the specified privilege(s)

  // Resource limits
  "ER_OUT_OF_RESOURCES", // Out of resources
  "ER_TOO_MANY_CONNECTIONS", // Too many connections

  // Data type issues
  "ER_TRUNCATED_WRONG_VALUE_FOR_FIELD", // Incorrect value for column
  "ER_DATA_TOO_LONG", // Data too long for column
  "ER_DATA_OUT_OF_RANGE", // Out of range value for column

  // Authentication errors
  "ER_PASSWORD_NO_MATCH", // Password does not match

  // View-related errors
  "ER_VIEW_NO_EXPLAIN", // View's SELECT contains a subquery in the FROM clause
  "ER_VIEW_WRONG_LIST", // View's SELECT and view's field list have different column counts

  // Stored procedure and function errors
  "ER_SP_DOES_NOT_EXIST", // Stored procedure does not exist
  "ER_SP_LILABEL_MISMATCH", // Label mismatch in stored procedure
  "ER_SP_LABEL_REDEFINE", // Duplicate label in stored procedure

  // Trigger errors
  "ER_TRG_DOES_NOT_EXIST", // Trigger does not exist
  "ER_TRG_ALREADY_EXISTS", // Trigger already exists

  // Partitioning errors
  "ER_PARTITION_NO_SUCH_PARTITION", // Unknown partition
  "ER_PARTITION_FUNCTION_FAILURE", // Partition function not supported

  // Foreign key constraint errors
  "ER_CANNOT_ADD_FOREIGN", // Cannot add foreign key constraint
  "ER_CANNOT_DROP_FOREIGN", // Cannot drop foreign key constraint

  // Index errors
  "ER_DUP_KEYNAME", // Duplicate key name
  "ER_TOO_MANY_KEYS", // Too many keys specified
  "ER_TOO_MANY_KEY_PARTS", // Too many key parts specified
  "ER_KEY_COLUMN_DOES_NOT_EXIST", // Key column doesn't exist in table

  // Schema errors
  "ER_BAD_DB_ERROR", // Unknown database
  "ER_DB_DROP_EXISTS", // Can't drop database; database doesn't exist
  "ER_DB_CREATE_EXISTS", // Can't create database; database exists

  // Table errors
  "ER_TABLE_EXISTS_ERROR", // Table already exists
  "ER_BAD_TABLE_ERROR", // Unknown table
  "ER_TABLEACCESS_DENIED_ERROR", // Access denied for user to table

  // Column errors
  "ER_FIELD_SPECIFIED_TWICE", // Column specified twice
  "ER_NO_DEFAULT_FOR_FIELD", // Field doesn't have a default value

  // Function errors
  "ER_NO_SUCH_FUNCTION", // Function does not exist
  "ER_WRONG_PARAMCOUNT_TO_PROCEDURE", // Incorrect parameter count to procedure
  "ER_WRONG_PARAMETERS_TO_PROCEDURE", // Incorrect parameters to procedure

  // Locking errors
  "ER_LOCK_OR_ACTIVE_TRANSACTION", // Can't execute the given command because you have active locked tables or an active transaction
  "ER_TABLE_LOCK_WAIT_TIMEOUT", // Timeout waiting for table lock

  // Replication errors
  "ER_MASTER_NET_READ", // Error reading master configuration
  "ER_MASTER_NET_WRITE", // Error writing to master
  "ER_BINLOG_PURGE_PROHIBITED", // Binlog purge prohibited

  // Configuration errors
  "ER_OPTION_PREVENTS_STATEMENT", // The MySQL server is running with the --read-only option so it cannot execute this statement
  "ER_UNKNOWN_SYSTEM_VARIABLE", // Unknown system variable

  // Connection errors
  "ER_CON_COUNT_ERROR", // Too many connections
  "ER_NET_READ_ERROR", // Network read error
  "ER_NET_WRITE_ERROR", // Network write error

  // SSL errors
  "ER_SSL_ERROR", // SSL connection error
  "ER_SSL_CIPHER_ERROR", // SSL cipher error

  // Plugin errors
  "ER_UNKNOWN_PLUGIN", // Unknown plugin
  "ER_PLUGIN_IS_NOT_LOADED", // Plugin is not loaded

  // Geometry errors
  "ER_GEOMETRY_IN_UNKNOWN_LENGTH_UNIT", // Geometry in unknown length unit

  // Backup errors
  "ER_BACKUP_FAILED", // Backup failed
  "ER_RESTORE_FAILED", // Restore failed

  // Full-text search errors
  "ER_FTS_QUERY_ERROR", // Full-text query error
  "ER_FTS_TOO_MANY_WORDS", // Full-text query contains too many words

  // XA transaction errors
  "ER_XAER_NOTA", // XAER_NOTA: Unknown XID
  "ER_XAER_INVAL", // XAER_INVAL: Invalid arguments
  "ER_XAER_RMFAIL", // XAER_RMFAIL: Resource manager failed

  // Event scheduler errors
  "ER_EVENT_DROP_FAILED", // Failed to drop event
  "ER_EVENT_COMPILE_ERROR", // Error during event compilation

  // Foreign data wrapper errors
  "ER_FDW_ERROR", // Foreign data wrapper error
  "ER_FDW_TABLE_NOT_FOUND", // Foreign table not found

  // Partitioning errors
  "ER_PARTITION_EXCHANGE_DIFFERENT_OPTION", // Partition exchange with table having different options
  "ER_PARTITION_EXCHANGE_PART_TABLE", // Partition exchange with a partitioned table

  // Resource group errors
  "ER_INVALID_RESOURCE_GROUP", // Invalid resource group
  "ER_RESOURCE_GROUP_EXISTS", // Resource group already exists
  "ER_RESOURCE_GROUP_NOT_EXISTS", // Resource group does not exist
  "ER_RESOURCE_GROUP_BUSY", // Resource group is in use and cannot be dropped
  "ER_INVALID_THREAD_PRIORITY", // Invalid thread priority in resource group
  
  // Role-based access control errors
  "ER_ROLE_NOT_GRANTED", // Role was not granted
  "ER_CANNOT_USER", // Cannot create user with GRANT
  "ER_ROLE_DROP_FAILED", // Failed to drop role

  // GTID errors
  "ER_GTID_MODE_REQUIRES_BINLOG", // GTID mode requires binary logging
  "ER_GTID_PURGED_WAS_CHANGED", // GTID_PURGED was changed manually

  // Memory allocation errors
  "ER_OUT_OF_SORTMEMORY", // Out of sort memory
  "ER_OUTOFMEMORY", // Out of memory

  // Collation and charset issues
  "ER_UNKNOWN_COLLATION", // Unknown collation
  "ER_UNKNOWN_CHARACTER_SET", // Unknown character set
  "ER_WRONG_STRING_LENGTH", // Incorrect string length

  // Event scheduler errors
  "ER_EVENT_ALREADY_EXISTS", // Event already exists
  "ER_EVENT_DOES_NOT_EXIST", // Event does not exist
  "ER_EVENT_CANNOT_ALTER_IN_THE_PAST", // Event cannot be altered to occur in the past

  // JSON handling errors
  "ER_INVALID_JSON_TEXT", // Invalid JSON text
  "ER_INVALID_JSON_PATH", // Invalid JSON path expression
  "ER_INVALID_JSON_CHARSET", // Invalid JSON character set

  // General execution errors
  "ER_CANT_CREATE_FILE", // Can't create file
  "ER_CANT_LOCK", // Can't lock file
  "ER_DISK_FULL", // Disk is full
  "ER_CHECKREAD", // Error on reading file
  "ER_CHECKSUM_MISMATCH", // Table checksum mismatch

  // XA transaction issues
  "ER_XAER_RMERR", // XAER_RMERR: Resource manager error
  "ER_XAER_DUPID", // XAER_DUPID: The XID already exists
  "ER_XA_RBROLLBACK", // XA_RBROLLBACK: Transaction rolled back

  // Miscellaneous permanent failures
  "ER_UNKNOWN_STORAGE_ENGINE", // Unknown storage engine
  "ER_NOT_SUPPORTED_YET", // Feature not supported
  "ER_STACK_OVERRUN", // Thread stack overrun
  "ER_UNSUPPORTED_EXTENSION", // Unsupported extension
  "ER_UNKNOWN_TABLESPACE", // Unknown tablespace
  "ER_WRONG_VALUE_FOR_VAR", // Incorrect value for system variable
];
