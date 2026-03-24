import { DB_CONFIG, Database } from "./utils/testConfig";

const TEST_TABLE_NAME = "test_auto_sql_affected_rows";

Object.values(DB_CONFIG).forEach((config) => {
    describe(`autoSQL Tests for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            // Disable workers (worker.js only exists post-build) and staging inserts
            // (staging path has a separate test surface; here we just verify affectedRows)
            db = Database.create({ ...config, useWorkers: false, useStagingInsert: false });
            await db.establishConnection();
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME);
            await db.runQuery(dropQuery);
        });

        afterAll(async () => {
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME);
            await db.runQuery(dropQuery);
            await db.closeConnection();
        });

        test("returns affectedRows > 0 after inserting data", async () => {
            // Use IDs and scores clearly outside the boolean range (0/1) to avoid
            // the MySQL tinyint(1) type-inference trap on the second autoSQL call.
            const data = [
                { id: 100, name: "Alice", score: 950 },
                { id: 200, name: "Bob",   score: 820 },
                { id: 300, name: "Carol", score: 780 }
            ];

            const result = await db.autoSQL(TEST_TABLE_NAME, data);
            if (!result.success) {
                throw new Error(`autoSQL failed: ${result.error}`);
            }
            expect(result.affectedRows).toBeGreaterThan(0);
            console.log(`Test Result [${config.sqlDialect}]: affectedRows =`, result.affectedRows);
        });

        test("returns affectedRows > 0 on a second insert (upsert)", async () => {
            const data = [
                { id: 100, name: "Alice", score: 999 },
                { id: 400, name: "Dave",  score: 880 }
            ];

            const result = await db.autoSQL(TEST_TABLE_NAME, data);
            if (!result.success) {
                throw new Error(`autoSQL failed: ${result.error}`);
            }
            expect(result.affectedRows).toBeGreaterThan(0);
            console.log(`Test Result [${config.sqlDialect}]: upsert affectedRows =`, result.affectedRows);
        });
    });
});
