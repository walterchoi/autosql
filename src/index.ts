
/*import { predictType, collateTypes } from "./helpers/columnTypes";
import { getMetaData, initializeMetaData } from "./helpers/metadata";
import { getHeaders } from "./helpers/headers"
import { predictIndexes } from "./helpers/keys";
import { autoAlterTable, autoCreateTable, autoSql, autoConfigureTable } from "./db/database";
import { insertData, validateDatabase, validateQuery, runSqlQuery } from "./db/database";
import { setSsh } from "./helpers/utilities";
import { sqlize, sqlizeValue } from "./helpers/utilities";
import { isObject } from "./helpers/utilities";
import { Database, exportSqlHelper } from "./db/database";

export {
    predictType,
    collateTypes,
    getHeaders,
    initializeMetaData,
    getMetaData,
    predictIndexes,
    autoAlterTable,
    autoCreateTable,
    autoSql,
    autoConfigureTable,
    insertData,
    validateDatabase,
    validateQuery,
    runSqlQuery,
    setSsh,
    exportSqlHelper,
    sqlize,
    sqlizeValue,
    isObject
};*/

const DB_CONFIG = {
    "mysql": {
        "sqlDialect": "mysql",
        "host": "localhost",
        "user": "root",
        "password": "root",
        "database": "mysql",
        "schema": "test_schema",
        "port": 3306,
        "updatePrimaryKey": true,
        "addTimestamps": true
    },
    "pgsql": {
        "sqlDialect": "pgsql",
        "host": "localhost",
        "user": "test_user",
        "password": "test_password",
        "database": "postgres",
        "schema": "test_schema",
        "port": 5432,
        "updatePrimaryKey": true,
        "addTimestamps": true
    }
}

import { DatabaseConfig } from "./config/types";
import WorkerHelper from "./workers/workerHelper";

async function runWorkerTests() {
    const dbConfig = Object.values(DB_CONFIG)[0] as DatabaseConfig; // Pick one DB config
    const taskParams = [
        ["Task 1", "Additional Data 1"],
        ["Task 2", "Additional Data 2"],
        ["Task 3", "Additional Data 3"],
        ["Task 4", "Additional Data 4"],
        ["Task 5", "Additional Data 5"],
        ["Task 6", "Additional Data 6"],
        ["Task 7", "Additional Data 7"],
        ["Task 8", "Additional Data 8"],
        ["Task 9", "Additional Data 9"],
        ["Task 10", "Additional Data 10"],
      ];

    const results = await WorkerHelper.run(dbConfig, "testFunction", taskParams);

    // Log the results
    console.log("\n✅ All Workers Completed ✅");
    console.log(results)
}

runWorkerTests();
