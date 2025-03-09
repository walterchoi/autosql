import fs from "fs";
import path from "path";
import { Database } from "../../src/db/database";
import { DatabaseConfig } from "../../src/config/types";
import { isValidSingleQuery } from "../../src/db/utils/validateQuery";

const CONFIG_PATH = path.resolve(__dirname, "./config.local.json");

export const DB_CONFIG: Record<string, DatabaseConfig> = fs.existsSync(CONFIG_PATH)
    ? (JSON.parse(fs.readFileSync(CONFIG_PATH, "utf-8")) as Record<string, DatabaseConfig>)
    : {
          mysql: {
              sql_dialect: "mysql",
              host: "localhost",
              user: "root",
              password: "root",
              database: "mysql",
              port: 3306,
              updatePrimaryKey: true
          },
          pgsql: {
              sql_dialect: "pgsql",
              host: "localhost",
              user: "test_user",
              password: "test_password",
              database: "postgres",
              port: 5432,
              updatePrimaryKey: true
          }
      };

export { Database, isValidSingleQuery };