import { DB_CONFIG, Database } from "./utils/testConfig";

const TEST_TABLE_NAME = "test_table_existence";

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Check Table Existence Tests for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME)
            const dropQueryResults = await db.runQuery(dropQuery)
        });

        afterAll(async () => {
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME)
            const dropQueryResults = await db.runQuery(dropQuery)
            const closeResult = await db.closeConnection();
            expect(closeResult.success).toBe(true);
        });

        test("Check table existence when table exists", async () => {
            // ✅ Ensure the test table exists first
            const createTableQuery = `CREATE TABLE ${db.getConfig().schema || db.getConfig().database || ""}.${TEST_TABLE_NAME} (id INT PRIMARY KEY, name VARCHAR(50));`;
            await db.runQuery(createTableQuery);

            // ✅ Check if the table exists
            const checkTableExistsQuery = db.getTableExistsQuery(
                db.getConfig().schema || db.getConfig().database || "", 
                TEST_TABLE_NAME
            );
            const result = await db.runQuery(checkTableExistsQuery);

            // ✅ Expect result to show that table exists
            expect(result.length).toBe(1);
            expect(Boolean(Number(result[0].count))).toBe(true);
        });

        test("Check table existence when table does NOT exist", async () => {
            const NON_EXISTENT_TABLE = "non_existent_table_test";

            // ✅ Check for a table that does not exist
            const checkTableExistsQuery = db.getTableExistsQuery(
                db.getConfig().schema || db.getConfig().database || "", 
                NON_EXISTENT_TABLE
            );
            const result = await db.runQuery(checkTableExistsQuery);

            // ✅ Expect result to be empty (table does not exist)
            expect(result.length).toBe(1);
            expect(Boolean(Number(result[0].count))).toBe(false);
        });
    });
});
