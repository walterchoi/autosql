import { DB_CONFIG, Database } from "./utils/testConfig";

const TEST_TABLE_NAME = "test_index_table";

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Validate Index Queries for ${config.sql_dialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();

            // Create a test table to use in index queries
            const createTableQuery = db.createTableQuery(TEST_TABLE_NAME, [
                { id: { type: "int", length: 11, primary: true, allowNull: false } },
                { user_uuid: { type: "varchar", length: 36, allowNull: false, unique: true } },
                { email: { type: "varchar", length: 255, allowNull: false, unique: true } },
            ]);            
            
            for (const query of createTableQuery) {
                await db.runQuery(query);
            }
        });

        afterAll(async () => {
            try {
                const dropQuery = db.dropTableQuery(TEST_TABLE_NAME);
                await db.runQuery(dropQuery);
            } catch (error) {
                console.error("Error during afterAll cleanup:", error);
            }
        
            const closeResult = await db.closeConnection();
            expect(closeResult.success).toBe(true);
        });

        test("Generate valid PRIMARY KEY query", async () => {
            const query = db.getPrimaryKeysQuery(TEST_TABLE_NAME);
            await expect(db.testQuery(query)).resolves.not.toThrow();
        });

        test("Generate valid FOREIGN KEY constraints query", async () => {
            const query = db.getForeignKeyConstraintsQuery(TEST_TABLE_NAME);
            await expect(db.testQuery(query)).resolves.not.toThrow();
        });

        test("Generate valid View Dependencies query", async () => {
            const query = db.getViewDependenciesQuery(TEST_TABLE_NAME);
            await expect(db.testQuery(query)).resolves.not.toThrow();
        });

        test("Generate valid DROP PRIMARY KEY query", async () => {
            const query = db.getDropPrimaryKeyQuery(TEST_TABLE_NAME);
            await expect(db.runQuery(query)).resolves.not.toThrow();
        
            const checkPrimaryKey = db.getPrimaryKeysQuery(TEST_TABLE_NAME);
            const result = await db.runQuery(checkPrimaryKey);
            expect(result.length).toBe(0); // Table should no longer have a primary key
        });
        
        test("Generate valid ADD PRIMARY KEY query", async () => {
            const query = db.getAddPrimaryKeyQuery(TEST_TABLE_NAME, ["user_uuid"]);
            await expect(db.runQuery(query)).resolves.not.toThrow();
        
            const checkPrimaryKey = db.getPrimaryKeysQuery(TEST_TABLE_NAME);
            const result: { column_name?: string, COLUMN_NAME?: string }[] = await db.runQuery(checkPrimaryKey);
        
            console.log(result); // Debugging to verify column structure
        
            expect(
                result.some((pk) => {
                    const columnName = pk.column_name || pk.COLUMN_NAME; // Handle MySQL and PostgreSQL
                    return columnName?.toLowerCase() === "user_uuid";
                })
            ).toBe(true);
        });
        
    });
});
