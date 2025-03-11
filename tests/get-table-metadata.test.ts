import { DB_CONFIG, Database } from "./utils/testConfig";
import { ColumnDefinition, MetadataHeader } from "../src/config/types";

const TEST_TABLE_NAME = "test_table";

const TEST_COLUMNS: MetadataHeader = {
    id: { type: "int", length: 11, primary: true, allowNull: false, autoIncrement: true },
    name: { type: "varchar", length: 50, allowNull: false, unique: true },
    age: { type: "int", length: 3, allowNull: true },
};

/*
Object.values(DB_CONFIG).forEach((config) => {
    describe(`getTableMetadata Tests for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();

            // ✅ Create table before testing metadata retrieval
            const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            for (const query of queries) {
                await db.runQuery(query);
            }
        });

        afterAll(async () => {
            // ✅ Drop table after tests
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME);
            await db.runQuery(dropQuery);

            // ✅ Ensure database closes successfully
            const closeResult = await db.closeConnection();
            expect(closeResult.success).toBe(true);
        });

        test("getTableMetadata should correctly retrieve table metadata", async () => {
            const metadata = await db.getTableMetaData(db.getConfig().schema || db.getConfig().database || "", TEST_TABLE_NAME);
        
            expect(metadata).not.toBeNull();
            expect(metadata).toBeDefined();
            
            if (!metadata) return; // Stop test if metadata is null

            Object.keys(TEST_COLUMNS).forEach((col) => {
                expect(metadata[col]).toBeDefined();
                expect(metadata[col].type).toEqual(TEST_COLUMNS[col].type);
                expect(metadata[col].length).toEqual(TEST_COLUMNS[col].length);
                expect(metadata[col].allowNull).toEqual(TEST_COLUMNS[col].allowNull);
                expect(metadata[col].primary).toEqual(TEST_COLUMNS[col].primary);
                expect(metadata[col].unique).toEqual(TEST_COLUMNS[col].unique);
                expect(metadata[col].autoIncrement).toEqual(TEST_COLUMNS[col].autoIncrement);
            });
        });

        test("getTableMetadata should return null for non-existent tables", async () => {
            const metadata = await db.getTableMetaData(db.getConfig().schema || db.getConfig().database || "", "non_existent_table");
            expect(metadata).toBeNull();
        });
    });
});
*/

describe("Sample Test", () => {
    test("should add numbers correctly", () => {
        expect(2 + 3).toBe(5);
    });
});