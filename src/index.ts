
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

import WorkerPool from "./workers/workerPool";

async function runWorkerTests() {
    const dbConfig = Object.values(DB_CONFIG)[0]; // Pick one DB config
    const pool = new WorkerPool(10, dbConfig); // Create a pool with 10 workers

    console.log("Starting worker tests...");

    const workerPromises: Promise<any>[] = [];

    for (let i = 1; i <= 10; i++) {
        const params = [`Worker-${i}`, `Task-${i}`];

        const workerPromise = pool.runTask("test", params).then((result) => {
            console.log(`Worker ${i} completed:`, result);
            return result;
        });

        workerPromises.push(workerPromise);
    }

    // Wait for all workers to finish
    const results = await Promise.all(workerPromises);

    // Log the results
    console.log("\n✅ All Workers Completed ✅");
    results.forEach((result, index) => {
        console.log(`Worker ${index + 1} Result:`, result);
    });

    // Close the worker pool
    pool.close();
}

runWorkerTests();
