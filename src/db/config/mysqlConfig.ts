import { TranslateMap, DialectConfig } from "../../config/types";

export const mysqlConfig: DialectConfig = {
  engine: "InnoDB",
  charset: "utf8mb4",
  collate: "utf8mb4_unicode_ci",
  encoding: "",
  requireLength: ["varchar", "binary", "decimal"],
  optionalLength: ["boolean", "tinyint", "smallint", "int", "bigint"],
  noLength: ["date", "time", "datetime", "datetimetz", "json", "text", "mediumtext", "longtext", "exponent", "double"],
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
    {
      find: "'",
      replace: "''",
      regex: "'",
      type: true
    },
    {
      find: "\\",
      replace: "\\\\",
      regex: "\\\\",
      type: true
    },
    {
      find: "true",
      replace: "1",
      regex: "^true$",
      type: ["boolean", "tinyint"]
    },
    {
      find: "false",
      replace: "0",
      regex: "^false$",
      type: ["boolean", "tinyint"]
    },
    {
      find: "T",
      replace: " ",
      regex: "T",
      type: ["date", "datetime", "datetimetz"]
    },
    {
      find: "Z",
      replace: "",
      regex: "Z$",
      type: ["date", "datetime", "datetimetz"]
    }
  ]
};
