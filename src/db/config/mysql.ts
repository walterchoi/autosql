import { TranslateMap, DialectConfig } from "./interfaces"

export const mysqlConfig: DialectConfig = {
    require_length: ["varchar", "binary", "decimal"],
    optional_length: ["boolean", "tinyint", "smallint", "int", "bigint"],
    no_length: ["date", "time", "datetime", "datetimetz", "json", "text", "mediumtext", "longtext", "exponent", "double"],
    decimals: ["exponent", "double", "decimal"],
    translate: {
      server_to_local: {
        timestamp: "datetimetz",
        tinyint: "boolean",
        numeric: "decimal",
        "double precision": "double"
      },
      local_to_server: {
        boolean: "tinyint",
        exponent: "double",
        double: "double precision",
        datetimetz: "timestamp"
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
  