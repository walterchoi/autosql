
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
        ["Hello", "World", "!"], // String inputs
        [{ key1: "value1", key2: 42 }, "String Test", { nested: { key3: "value3" } }], // Mixed JSON & string
        ["Only String 1", { objField: "value" }, "Only String 2"], // Partial JSON
        [{ user: "Alice", action: "Login" }, { page: "Dashboard" }, { device: "Mobile" }], // Fully JSON
        ["Testing", { data: "Payload" }, 123], // String, Object, Number
        [{ config: true, mode: "dark" }, ["array", "of", "strings"], "End"], // Object, Array, String
        ["Array Example", { arrayData: [1, 2, 3, 4] }, { status: "active" }], // String, Object with Array
        [{ nested: { key: "value" } }, { moreData: ["a", "b", "c"] }, "Final"], // Nested JSON, Object, String
        ["Multi-Level", { user: { id: 1, name: "John" } }, { metadata: { verified: true } }], // Deeply Nested JSON
        [{ emptyObj: {} }, "Simple String", { deepNested: { level1: { level2: "data" } } }], // Empty Object & Deep Nested
        ["Numbers", 42, { isEven: true }], // String, Number, Boolean
        [{ location: "USA" }, { user: { id: 1001, details: { active: true } } }, "End Case"], // Complex JSON Object
        ["Testing Special Characters", { special: "!@#$%^&*()" }, { unicode: "✔️🚀" }], // Special characters & emoji
        [{ date: new Date().toISOString() }, "Timestamp", { timezone: "UTC" }], // Date, String, Object
        ["Final Test", { nullField: null }, { undefinedField: undefined }], // Handling null & undefined
        { type: "userAction", user: "Alice", action: "clicked", element: "button" }, // Object instead of array
        { config: { darkMode: true }, status: "active", priority: 1 }, // Object with nested properties
        { event: "purchase", details: { item: "Laptop", price: 1299.99 }, currency: "USD" }, // Nested Object
        { system: "Linux", version: "5.15.0", architecture: "x86_64" }, // JSON-like config object
        { userData: { id: 123, permissions: ["read", "write"] }, session: "abc123" }, // JSON with array
        { isValid: true, metadata: { created: new Date().toISOString() } }, // Boolean & Date handling
        ["Event", { name: "Login", timestamp: new Date().toISOString() }, { success: true }], // Mixed Object & Strings
        { test: "Special Chars", data: "!@#$%^&*", emoji: "🚀🔥" }, // Special character handling
        { analytics: { views: 12000, uniqueUsers: 5600 }, engagement: { clicks: 240 } }, // Analytics-style object
        ["String", { arrayInsideObject: { data: [1, 2, 3, 4] } }, "End"], // String & Nested Array Object
    ];
    

    const results = await WorkerHelper.run(dbConfig, "testFunction", taskParams);

    // Log the results
    console.log(results)
}

runWorkerTests();
