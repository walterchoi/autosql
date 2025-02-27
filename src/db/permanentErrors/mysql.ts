export const mysqlPermanentErrors = [
  // Syntax errors
  "ER_SYNTAX_ERROR",

  // Table or field issues
  "ER_NO_SUCH_TABLE",
  "ER_BAD_FIELD_ERROR",

  // Constraint violations
  "ER_DUP_ENTRY",
  "ER_ROW_IS_REFERENCED_2",
  "ER_NO_REFERENCED_ROW_2",

  // Access and permission errors
  "ER_DBACCESS_DENIED_ERROR",
  "ER_ACCESS_DENIED_ERROR",
  "ER_SPECIFIC_ACCESS_DENIED_ERROR",

  // Resource limits
  "ER_OUT_OF_RESOURCES",
  "ER_TOO_MANY_CONNECTIONS",
  "ER_CON_COUNT_ERROR"
];
