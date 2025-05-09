import { DB_CONFIG, Database } from "./utils/testConfig";
import { parseDatabaseMetaData, organizeSplitTable, organizeSplitData } from "../src/helpers/utilities";
import { MetadataHeader } from "../src/config/types";

const TEST_TABLE_NAME = "test_split_table";

const newMetaData : MetadataHeader = {
    id: {
        type: "int",
        primary: true,
        allowNull: false
    },
    name: {
        type: "varchar",
        allowNull: false,
        length: 255
    },
    email: {
        type: "varchar",
        length: 255
    },
    phone: {
        type: "varchar",
        length: 20
    },
    address: {
        type: "varchar",
        length: 255
    },
    leads: {
        type: "smallint",
        allowNull: true
    },
    status: {
        type: "varchar",
        allowNull: true
    },
    opportunityId: {
        type: "varchar",
        allowNull: true
    }
}

const newData: Record<string, any>[] = [
    {
        id: 1,
        name: "Alice Johnson",
        email: "alice.johnson@example.com",
        phone: "123-456-7890",
        address: "123 Elm Street, Springfield",
        leads: 5,
        status: "Active",
        opportunityId: "OPP-001"
    },
    {
        id: 2,
        name: "Bob Smith",
        email: "bob.smith@example.com",
        phone: "987-654-3210",
        address: "456 Maple Avenue, Shelbyville",
        leads: null, // No leads assigned
        status: "Pending",
        opportunityId: "OPP-002"
    },
    {
        id: 3,
        name: "Charlie Davis",
        email: "charlie.davis@example.com",
        phone: "555-123-4567",
        address: "789 Oak Lane, Capital City",
        leads: 2,
        status: null, // Status unknown
        opportunityId: null // No opportunity assigned yet
    }
];

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

            const newGroupedByTable = organizeSplitTable(TEST_TABLE_NAME, newMetaData, currentSplitResults!.results || [], db.getDialectConfig())
            const newGroupedData = organizeSplitData(newData, newGroupedByTable)
            console.log(newGroupedByTable)
            console.log(newGroupedData)

            const groupedInfo = await db.splitTableData(TEST_TABLE_NAME, newData, newMetaData)
            console.log(groupedInfo)
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
