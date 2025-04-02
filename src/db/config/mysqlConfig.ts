import { TranslateMap, DialectConfig, SqlizeRule } from "../../config/types";

export const mysqlConfig: DialectConfig = {
  dialect: "mysql",
  engine: "InnoDB",
  charset: "utf8mb4",
  collate: "utf8mb4_unicode_ci",
  encoding: "",
  requireLength: ["varchar", "binary", "decimal"],
  optionalLength: ["int", "bigint", "smallint", "tinyint"],
  noLength: ["date", "time", "datetime", "datetimetz", "timestamp", "timestamptz", "json", "text", "mediumtext", "longtext", "double", "boolean"],
  decimals: ["exponent", "double", "decimal"],
  translate: {
    serverToLocal: {
      timestamp: "datetimetz",
      tinyint: "boolean",
      numeric: "decimal",
      "double precision": "double"
    },
    localToServer: {
      boolean: "tinyint",
      exponent: "double",
      double: "double precision",
      datetimetz: "timestamp"
    }
  },
  defaultTranslation: {
    "UUID()": "(UUID())",
    "TRUE": "1",
    "FALSE": "0"
  },
  sqlize: [
    { regex: "'", replace: "''", type: true },
    { regex: "\\\\", replace: "\\\\\\\\", type: true },
    { regex: "^true$", replace: "1", type: ["boolean", "tinyint"] },
    { regex: "^false$", replace: "0", type: ["boolean", "tinyint"] },
    { regex: "T", replace: " ", type: ["date", "datetime", "datetimetz"] },
    { regex: "\\.\\d{3,}Z$", replace: "", type: ["date", "datetime", "datetimetz"] },
    { regex: "Z$", replace: "", type: ["date", "datetime", "datetimetz"] }
  ],
  maxIndexCount: 64
};