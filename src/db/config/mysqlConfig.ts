import { TranslateMap, DialectConfig } from "../../config/types";

export const mysqlConfig: DialectConfig = {
  dialect: "mysql",
  engine: "InnoDB",
  charset: "utf8mb4",
  collate: "utf8mb4_unicode_ci",
  encoding: "",
  requireLength: ["varchar", "binary", "decimal"],
  optionalLength: ["int", "bigint", "smallint", "tinyint"],
  noLength: ["date", "time", "datetime", "datetimetz", "json", "text", "mediumtext", "longtext", "double", "boolean"],
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
