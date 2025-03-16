import { DB_CONFIG, Database } from "./utils/testConfig";
import { MetadataHeader, QueryResult } from "../src/config/types";

const TEST_TABLE_NAME = "test_index_table";

const COLUMNS : MetadataHeader = {
    id: { type: "int", length: 11, primary: true, allowNull: false },
    user_uuid: { type: "varchar", length: 36, allowNull: false, unique: true },
    email: { type: "varchar", length: 255, allowNull: false, unique: true }
};

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Validate Index Queries for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();

            // Create a test table to use in index queries
            const createTableQuery = db.createTableQuery(TEST_TABLE_NAME, COLUMNS);            
            
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
            expect(result.success).toBe(true);
            expect(result.results?.length).toBe(0); // Table should no longer have a primary key
        });
        
        test("Generate valid ADD PRIMARY KEY query", async () => {
            const query = db.getAddPrimaryKeyQuery(TEST_TABLE_NAME, ["user_uuid"]);
            await expect(db.runQuery(query)).resolves.not.toThrow();
        
            const checkPrimaryKey = db.getPrimaryKeysQuery(TEST_TABLE_NAME);
            const result: QueryResult = await db.runQuery(checkPrimaryKey);
        
            expect(
                result!.results!.some((pk) => {
                    const columnName = pk.column_name || pk.COLUMN_NAME; // Handle MySQL and PostgreSQL
                    return columnName?.toLowerCase() === "user_uuid";
                })
            ).toBe(true);
        });
        
    });
});
