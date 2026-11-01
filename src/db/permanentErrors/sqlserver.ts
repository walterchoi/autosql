// SQL Server surfaces errors by numeric code (err.number), not SQLSTATE. These are permanent
// (retrying won't help) — deadlock victim (1205) and lock-timeout (1222) are intentionally omitted
// so they reach runTransaction's whole-transaction retry.
export const SQL_SERVER_PERMANENT_ERRORS = [
  "102",   // Incorrect syntax
  "156",   // Incorrect syntax near a keyword
  "207",   // Invalid column name
  "208",   // Invalid object name (undefined table)
  "245",   // Conversion failed (type mismatch)
  "515",   // Cannot insert NULL into a NOT NULL column
  "547",   // Foreign key / check constraint violation
  "2627",  // Unique constraint / primary key violation
  "2601",  // Duplicate key row in a unique index
  "4060",  // Cannot open database
  "18456", // Login failed
  "229",   // Permission denied
  "8114",  // Error converting data type
  "8152",  // String or binary data would be truncated
  "ELOGIN", // Connect-time auth/database-access failure (tedious's own code, not a T-SQL error
            // number — a bad password or an inaccessible database never gets a numeric error at
            // connect time, unlike a query-time failure; retrying with the same credentials can't
            // succeed, same reasoning as 18456 for a query-time login error).
];
