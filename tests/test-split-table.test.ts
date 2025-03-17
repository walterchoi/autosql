import { DB_CONFIG, Database } from "./utils/testConfig";
import { parseDatabaseMetaData } from "../src/helpers/utilities";
import { MetadataHeader } from "../src/config/types";

const TEST_TABLE_NAME = "test_split_table";

Object.values(DB_CONFIG).forEach((config) => {
    describe(`getSplitTablesQuery Tests for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();
        });

        afterAll(async () => {
            await db.closeConnection();
        });

        test("Detects split tables correctly", async () => {
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME);
            await db.runQuery(dropQuery);
            const dropQuery1 = db.dropTableQuery(`${TEST_TABLE_NAME}__part_001`);
            await db.runQuery(dropQuery1);
            const dropQuery2 = db.dropTableQuery(`${TEST_TABLE_NAME}__part_002`);
            await db.runQuery(dropQuery2);

            // ✅ Create the main table
            await db.runQuery(`
                CREATE TABLE ${config.schema}.${TEST_TABLE_NAME} (
                    id INT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL
                );
            `);

            // ✅ Create split tables
            await db.runQuery(`
                CREATE TABLE ${config.schema}.${TEST_TABLE_NAME}__part_001 (
                    id INT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255)
                );
            `);
            await db.runQuery(`
                CREATE TABLE ${config.schema}.${TEST_TABLE_NAME}__part_002 (
                    id INT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    phone VARCHAR(20)
                );
            `);

            // ✅ Run the query
            const splitQuery = db.getSplitTablesQuery(TEST_TABLE_NAME);
            const currentSplitResults = await db.runQuery(splitQuery);

            // ✅ Validate output
            expect(currentSplitResults.success).toBe(true); // Ensure query was successful
            expect(currentSplitResults.results).toBeDefined(); // Ensure results exist
            expect(currentSplitResults.results!.length).toBe(6); // Use ! to assert results exist

            const normalizeKeys = (obj: Record<string, any>) =>
                Object.keys(obj).reduce((acc, key) => {
                    acc[key.toLowerCase()] = obj[key]; // Convert all keys to lowercase
                    return acc;
                }, {} as Record<string, any>);
            
            expect(currentSplitResults!.results!.map(normalizeKeys)).toEqual(
                expect.arrayContaining([
                    expect.objectContaining({ table_name: `${TEST_TABLE_NAME}__part_001` }),
                    expect.objectContaining({ table_name: `${TEST_TABLE_NAME}__part_002` }),
                ])
            );

            if(currentSplitResults.results) {
                const normalizedResults = parseDatabaseMetaData(currentSplitResults!.results, db.getDialectConfig()) || {}
                const groupedByTable = Object.entries(normalizedResults).reduce((acc, [columnName, columnDef]) => {
                    if (!columnDef.tableName) return acc; // Skip if there's no table name
                
                    const tableName = columnDef.tableName;
                
                    if (!acc[tableName]) acc[tableName] = {}; // Initialize table entry
                
                    acc[tableName][columnName] = columnDef; // Add column metadata under the table name
                
                    return acc;
                }, {} as Record<string, MetadataHeader>);
                console.log(normalizedResults)
                console.log(groupedByTable)
            }

        });

        test("Returns no results when no split tables exist", async () => {
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME);
            await db.runQuery(dropQuery);
            const dropQuery1 = db.dropTableQuery(`${TEST_TABLE_NAME}__part_001`);
            await db.runQuery(dropQuery1);
            const dropQuery2 = db.dropTableQuery(`${TEST_TABLE_NAME}__part_002`);
            await db.runQuery(dropQuery2);

            // ✅ Create only the main table
            await db.runQuery(`
                CREATE TABLE ${config.schema}.${TEST_TABLE_NAME} (
                    id INT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL
                );
            `);

            // ✅ Run the query
            const splitQuery = db.getSplitTablesQuery(TEST_TABLE_NAME);
            const currentSplitResults = await db.runQuery(splitQuery);

            // ✅ Validate output
            expect(currentSplitResults.success).toBe(true); // Ensure query was successful
            expect(currentSplitResults.results).toBeDefined(); // Ensure results exist
            expect(currentSplitResults.results!.length).toBe(0); // Use ! to assert results exist
        });
    });
});
