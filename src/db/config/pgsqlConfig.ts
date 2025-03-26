import { TranslateMap, DialectConfig } from "../../config/types";

export const pgsqlConfig: DialectConfig = {
  dialect: "pgsql",
  encoding: "UTF8",
  collate: "en_US.UTF-8",
  engine: "",
  charset: "",
  requireLength: ["varchar", "binary", "decimal"],
  optionalLength: ["int", "bigint", "smallint", "tinyint"],
  noLength: ["date", "time", "datetime", "datetimetz", "timestamp", "timestamptz", "json", "text", "mediumtext", "longtext", "double", "boolean"],
  decimals: ["exponent", "double", "decimal"],
  translate: {
    serverToLocal: {
      "timestamp without time zone": "datetime",
      "timestamp with time zone": "datetimetz",
      "integer": "int",
      "character varying": "varchar",
      "numeric": "decimal",
      "double precision": "double",
      "bytea": "binary"
    },
    localToServer: {
      "tinyint": "smallint",
      "exponent": "numeric",
      "double": "double precision",
      "datetime": "timestamp without time zone",
      "datetimetz": "timestamp with time zone",
      "binary": "bytea",
      "mediumtext": "text",
      "longtext": "text"
    }
  },
  defaultTranslation: {
    "UUID()": "gen_random_uuid()",
    "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP": "CURRENT_TIMESTAMP"
  },
  sqlize:  [
    { regex: "'", replace: "''", type: true },
    { regex: "\\\\", replace: "\\\\\\\\", type: true },
    { regex: "T", replace: " ", type: ["date", "datetime", "datetimetz"] },
    { regex: "Z$", replace: "", type: ["date", "datetime", "datetimetz"] }
  ]
};
