import { DialectConfig } from "../../config/types";

// SQL Server / Azure SQL (T-SQL) dialect config. Class A row-store, like MySQL/Postgres.
// Notes:
// - Text is stored as NVARCHAR (UTF-16, always Unicode) so emoji/multilingual round-trip without a
//   UTF-8 collation. Unbounded text (text/mediumtext/longtext/json) -> NVARCHAR(MAX), handled in the
//   table builder (the "(max)" token can't go through assertSafeTypeToken).
// - SQL Server integer types take NO length/display width, so they are noLength here.
// - Identifiers are bracket-quoted ([name]); parameters are named (@p0, @p1 ...) — see escape.ts and
//   the sqlserver adapter's executeQuery.
export const sqlServerConfig: DialectConfig = {
  dialect: "sqlserver",
  encoding: "",
  collate: "",
  engine: "",
  charset: "",
  requireLength: ["nvarchar", "varchar", "nchar", "char", "decimal", "varbinary", "binary"],
  optionalLength: [],
  noLength: [
    "int", "bigint", "smallint", "tinyint", "bit",
    "date", "time", "datetime2", "datetime", "datetimeoffset",
    "float", "real", "text", "ntext",
  ],
  decimals: ["exponent", "double", "decimal", "float"],
  translate: {
    // Server (INFORMATION_SCHEMA/sys DATA_TYPE, lower-cased) -> local inference type.
    serverToLocal: {
      "tinyint": "tinyint",
      "smallint": "smallint",
      "int": "int",
      "bigint": "bigint",
      "decimal": "decimal",
      "numeric": "decimal",
      "money": "decimal",
      "float": "double",
      "real": "double",
      "bit": "boolean",
      "date": "date",
      "time": "time",
      "datetime2": "datetime",
      "datetime": "datetime",
      "smalldatetime": "datetime",
      "datetimeoffset": "datetimetz",
      "varbinary": "binary",
      "binary": "binary",
      "nvarchar": "varchar",
      "varchar": "varchar",
      "nchar": "varchar",
      "char": "varchar",
      "text": "text",
      "ntext": "text",
      "uniqueidentifier": "varchar",
    },
    // Local inference type -> server type token used in DDL.
    localToServer: {
      "tinyint": "tinyint",
      "smallint": "smallint",
      "int": "int",
      "bigint": "bigint",
      "double": "float",
      "exponent": "float",
      "boolean": "bit",
      "datetime": "datetime2",
      "datetimetz": "datetimeoffset",
      "binary": "varbinary",
      "varchar": "nvarchar",
      "text": "nvarchar",
      "mediumtext": "nvarchar",
      "longtext": "nvarchar",
      "json": "nvarchar",
    },
  },
  defaultTranslation: {
    // SQL Server supports CURRENT_TIMESTAMP natively (alias of GETDATE()); no rewrite needed for it.
    "UUID()": "NEWID()",
    "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP": "CURRENT_TIMESTAMP",
  },
  sqlize: [
    // datetime2/datetimeoffset accept ISO 'YYYY-MM-DD HH:MM:SS[.fff]'. Normalise the ISO 'T' and a
    // trailing 'Z' the same way the other dialects do; values are parameter-bound, not escaped here.
    { regex: "T", replace: " ", type: ["date", "datetime", "datetimetz"] },
    { regex: "\\.\\d{4,}Z$", replace: "", type: ["date", "datetime"] },
    { regex: "Z$", replace: "", type: ["date", "datetime", "datetimetz"] },
  ],
  maxIndexCount: 250,
  maxDecimalScale: 38, // SQL Server DECIMAL: max precision/scale 38
};
