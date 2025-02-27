export const pgsqlPermanentErrors = [
  // Syntax errors
  "42601", // Syntax error
  "42000", // Syntax error or access rule violation

  // Table or field issues
  "42P01", // Undefined table
  "42703", // Undefined column
  "42P02", // Undefined parameter
  "42704", // Undefined object
  "42883", // Undefined function
  "42P10", // Invalid column reference
  "42P11", // Invalid cursor definition
  "42P12", // Invalid database definition
  "42P13", // Invalid function definition
  "42P16", // Invalid table definition
  "42P17", // Invalid object definition
  
  // Constraint violations
  "23502", // Not null violation
  "23503", // Foreign key violation
  "23505", // Unique violation
  "23514", // Check constraint violation
  "23P01", // Exclusion violation

  // Authentication and access issues
  "28000", // Invalid authorization specification
  "28P01", // Invalid password
  "42501", // Insufficient privilege
  "0A000", // Feature not supported
  
  // Transaction and integrity issues
  "40001", // Serialization failure
  "40P01", // Deadlock detected
  "25P02", // In failed SQL transaction
  "25P03", // Idle in transaction session timeout
  "25P04", // Transaction timeout

  // Resource and system issues
  "53100", // Disk full
  "53200", // Out of memory
  "53300", // Too many connections
  "53400", // Configuration limit exceeded
  "58000", // System error
  "58030", // I/O error

  // Internal errors
  "XX000", // Internal error
  "XX001", // Data corrupted
  "XX002", // Index corrupted
];