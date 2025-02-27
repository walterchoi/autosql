export const pgsqlPermanentErrors = [
  // Syntax errors
  "42601", // Syntax error

  // Table or field issues
  "42P01", // Undefined table
  "42703", // Undefined column

  // Constraint violations
  "23505", // Unique violation
  "23503", // Foreign key violation
  "23502", // Not null violation

  // Access and permission errors
  "42501", // Insufficient privilege
  "28P01", // Invalid password

  // Resource limits
  "53400", // Configuration limit exceeded
  "53300"  // Too many connections
];