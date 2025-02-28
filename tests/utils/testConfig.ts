import fs from "fs";
import path from "path";
import { DatabaseConfig, Database } from "../../src/db/database";
import { isValidSingleQuery } from "../../src/db/validateQuery";

const CONFIG_PATH = path.resolve(__dirname, "../../src/config/config.local.json");

export const DB_CONFIG: Record<string, DatabaseConfig> = fs.existsSync(CONFIG_PATH)
    ? (JSON.parse(fs.readFileSync(CONFIG_PATH, "utf-8")) as Record<string, DatabaseConfig>)
    : {
          mysql: {
              sql_dialect: "mysql",
              host: "localhost",
              user: "root",
              password: "root",
              database: "mysql",
              port: 3306
          },
          pgsql: {
              sql_dialect: "pgsql",
              host: "localhost",
              user: "test_user",
              password: "test_password",
              database: "postgres",
              port: 5432
          }
      };

export { Database, isValidSingleQuery };