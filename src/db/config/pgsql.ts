import { TranslateMap, DialectConfig } from "./interfaces"

export const pgsqlConfig: DialectConfig = {
  require_length: ["varchar"],
  optional_length: ["int", "bigint"],
  no_length: ["date", "time", "datetime", "datetimetz", "json", "text", "mediumtext", "longtext", "exponent", "double", "binary", "smallint", "boolean"],
  decimals: ["exponent", "double", "decimal"],
  translate: {
    server_to_local: {
      tinyint: "smallint",
      exponent: "numeric",
      double: "double precision",
      datetime: "timestamp without time zone",
      datetimetz: "timestamp with time zone",
      binary: "bytea",
      mediumtext: "text",
      longtext: "text"
    },
    local_to_server: {
      "timestamp without time zone": "datetime",
      "timestamp with time zone": "datetimetz",
      integer: "int",
      "character varying": "varchar",
      numeric: "decimal",
      "double precision": "double",
      bytea: "binary"
    }
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
    }
  ]
};