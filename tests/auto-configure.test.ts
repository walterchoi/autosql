import { DB_CONFIG, Database } from "./utils/testConfig";
import { MetadataHeader, AlterTableChanges } from "../src/config/types";

const TEST_TABLE_NAME = "test_auto_configure_table";

const INITIAL_METADATA: MetadataHeader = {
    id: { type: "int", primary: true, autoIncrement: true, allowNull: false },
    name: { type: "varchar", length: 255, allowNull: false, unique: true }
};

const UPDATED_METADATA: MetadataHeader = {
    ...INITIAL_METADATA,
    email: { type: "varchar", length: 255, allowNull: true } // New column added
};

const ALTER_TABLE_CHANGES: AlterTableChanges = {
    addColumns: {
        email: { type: "varchar", length: 255, allowNull: true }
    },
    modifyColumns: {},
    dropColumns: [],
    renameColumns: [],
    nullableColumns: [],
    noLongerUnique: [],
    primaryKeyChanges: []
};

Object.values(DB_CONFIG).forEach((config) => {
    describe(`autoConfigureTable Tests for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();
        });

        afterAll(async () => {
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME)
            const dropQueryResults = await db.runQuery(dropQuery)
            await db.closeConnection();
        });

        test("Creates table when it does not exist and no metadata provided", async () => {
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME)
            const dropQueryResults = await db.runQuery(dropQuery)
        
            // ✅ Provide sample rows for structure inference
            const sampleData = [
                { id: 1, name: "Alice" },
                { id: 2, name: "Bob" }
            ];
        
            const result = await db.autoSQL.autoConfigureTable(TEST_TABLE_NAME, sampleData, null, INITIAL_METADATA);
            if (Array.isArray(result)) {
                throw new Error("Expected QueryResult but received QueryInput[] (runQuery: false?)");
            }
            expect(result.success).toBe(true);
            console.log(`Test Result [${config.sqlDialect}]: Creates table when it does not exist:`, result);
        
            // ✅ Verify that the table was actually created
            const checkTableExistsQuery = db.getTableExistsQuery(
                db.getConfig().schema || db.getConfig().database || "",
                TEST_TABLE_NAME
            );
            const tableExistsResult = await db.runQuery(checkTableExistsQuery);
            expect(tableExistsResult!.results!.length).toBe(1);
            expect(Boolean(Number(tableExistsResult!.results![0].count))).toBe(true);
        });

        test("Creates table when it does not exist and metadata is provided", async () => {
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME)
            const dropQueryResults = await db.runQuery(dropQuery)
        
            // ✅ Provide sample data to ensure valid execution
            const sampleData = [{ id: 1, name: "Alice" }];
        
            const result = await db.autoSQL.autoConfigureTable(TEST_TABLE_NAME, sampleData, null, INITIAL_METADATA);
            if (Array.isArray(result)) {
                throw new Error("Expected QueryResult but received QueryInput[] (runQuery: false?)");
            }
            expect(result.success).toBe(true);
            console.log(`Test Result [${config.sqlDialect}]: Creates table with provided metadata:`, result);
        
            // ✅ Check if table was actually created
            const checkTableExistsQuery = db.getTableExistsQuery(
                db.getConfig().schema || db.getConfig().database || "",
                TEST_TABLE_NAME
            );
            const tableExistsResult = await db.runQuery(checkTableExistsQuery);
            expect(tableExistsResult!.results!.length).toBe(1);
            expect(Boolean(Number(tableExistsResult!.results![0].count))).toBe(true);
        });
        
        test("Returns success when table exists and no changes are needed", async () => {
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME)
            const dropQueryResults = await db.runQuery(dropQuery)
            // ✅ Ensure the table exists first
            await db.runQuery(`CREATE TABLE IF NOT EXISTS ${TEST_TABLE_NAME} (id INT PRIMARY KEY, name VARCHAR(255) UNIQUE NOT NULL);`);
        
            const result = await db.autoSQL.autoConfigureTable(TEST_TABLE_NAME, [], INITIAL_METADATA, INITIAL_METADATA);
            if (Array.isArray(result)) {
                throw new Error("Expected QueryResult but received QueryInput[] (runQuery: false?)");
            }
            expect(result.success).toBe(true);
            expect(result.results).toEqual([]); // ✅ No changes should be applied
            console.log(`Test Result [${config.sqlDialect}]: Table exists, no changes needed:`, result);
        });
        
        test("Alters table when metadata changes are detected", async () => {
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME)
            const dropQueryResults = await db.runQuery(dropQuery)
            // ✅ Ensure table exists with initial metadata
            await db.runQuery(`CREATE TABLE IF NOT EXISTS ${db.getConfig().schema || db.getConfig().database}.${TEST_TABLE_NAME} (id INT PRIMARY KEY, name VARCHAR(255) UNIQUE NOT NULL);`);
        
            // ✅ Provide sample data (though not required for alteration)
            const sampleData = [{ id: 1, name: "Alice", email: "alice@example.com" }];
        
            const result = await db.autoSQL.autoConfigureTable(TEST_TABLE_NAME, sampleData, INITIAL_METADATA);
            if (Array.isArray(result)) {
                throw new Error("Expected QueryResult but received QueryInput[] (runQuery: false?)");
            }
            expect(result.success).toBe(true);
            console.log(`Test Result [${config.sqlDialect}]: Table altered when metadata changes:`, result);
        });
        
        test("Alters table when precomputed table changes are provided", async () => {
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME)
            const dropQueryResults = await db.runQuery(dropQuery)
            // ✅ Ensure table exists
            await db.runQuery(`CREATE TABLE IF NOT EXISTS ${db.getConfig().schema || db.getConfig().database}.${TEST_TABLE_NAME} (id INT PRIMARY KEY, name VARCHAR(255) UNIQUE NOT NULL);`);
        
            // ✅ Provide sample data (though not required for precomputed changes)
            const sampleData = [{ id: 1, name: "Alice", email: "alice@example.com" }];
        
            const result = await db.autoSQL.autoConfigureTable(TEST_TABLE_NAME, sampleData, ALTER_TABLE_CHANGES, UPDATED_METADATA);
            if (Array.isArray(result)) {
                throw new Error("Expected QueryResult but received QueryInput[] (runQuery: false?)");
            }
            expect(result.success).toBe(true);
            console.log(`Test Result [${config.sqlDialect}]: Table altered using precomputed changes:`, result);
        }); 
    });
});
