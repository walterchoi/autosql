import { TranslateMap, DialectConfig } from "../../config/types";

export const pgsqlConfig: DialectConfig = {
  encoding: "UTF8",
  collate: "en_US.UTF-8",
  engine: "",
  charset: "",
  requireLength: ["varchar"],
  optionalLength: ["int", "bigint"],
  noLength: ["date", "time", "datetime", "datetimetz", "json", "text", "mediumtext", "longtext", "exponent", "double", "binary", "smallint", "boolean"],
  decimals: ["exponent", "double", "decimal"],
  translate: {
    serverToLocal: {
      "timestamp without time zone": "datetime",
      "timestamp with time zone": "datetimetz",
      integer: "int",
      "character varying": "varchar",
      numeric: "decimal",
      "double precision": "double",
      bytea: "binary"
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
      find: "1",
      replace: "true",
      regex: "^1$",
      type: ["boolean"]
    },
    {
      find: "0",
      replace: "false",
      regex: "^0$",
      type: ["boolean"]
    },
    {
      find: ",",
      replace: "",
      regex: ",",
      type: ["binary", "tinyint", "smallint", "int", "bigint", "decimal"]
    },
    {
      find: ".",
      replace: "0",
      regex: "^\\.{1}$",
      type: ["binary", "tinyint", "smallint", "int", "bigint", "decimal"]
    }
  ]
};
