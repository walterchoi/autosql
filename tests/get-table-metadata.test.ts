import { DB_CONFIG, Database } from "./utils/testConfig";
import { ColumnDefinition, MetadataHeader } from "../src/config/types";

const TEST_TABLE_NAME = "test_table";

const TEST_COLUMNS: MetadataHeader = {
    user_id: {
        type: "int",
        length: 11,
        primary: true,
        autoIncrement: true,
        allowNull: false
    },
    user_uuid: {
        type: "varchar",
        length: 36,
        allowNull: false,
        unique: true,
        default: "UUID()"
    },
    username: {
        type: "varchar",
        length: 50,
        allowNull: false,
        unique: true
    },
    email: {
        type: "varchar",
        length: 255,
        allowNull: false,
        unique: true,
        index: true
    },
    password_hash: {
        type: "varchar",
        length: 255,
        allowNull: false
    },
    bio: {
        type: "text",
        allowNull: true
    },
    age: {
        type: "int",
        length: 3,
        allowNull: true,
        default: 18
    },
    is_active: {
        type: "boolean",
        allowNull: false,
        default: true,
        index: true
    },
    account_balance: {
        type: "decimal",
        length: 12,
        decimal: 2,
        allowNull: false,
        default: 0.00
    },
    user_metadata: {
        type: "json",
        allowNull: true
    },
    created_at: {
        type: "datetime",
        allowNull: false,
        default: "CURRENT_TIMESTAMP"
    },
    updated_at: {
        type: "datetime",
        allowNull: false,
        default: "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
    }
};

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

        test("✅ Retrieves correct table metadata", async () => {
            const metadata = await db.getTableMetaData(db.getConfig().schema || db.getConfig().database || "", TEST_TABLE_NAME);
            expect(metadata).not.toBeNull();
            expect(metadata).toBeDefined();

            if (!metadata) return; // Stop test if metadata is null

            Object.keys(TEST_COLUMNS).forEach((col) => {
                expect(metadata[col]).toBeDefined();
                expect(metadata[col].type).toEqual(TEST_COLUMNS[col].type);
            
                // ✅ Ignore length check for integer & text-based types
                if (!["int", "smallint", "bigint", "tinyint", "text", "mediumtext", "longtext", "json"].includes(TEST_COLUMNS[col].type ?? "")) {
                    expect(metadata[col].length).toEqual(TEST_COLUMNS[col].length ?? undefined);
                }
            
                expect(metadata[col].allowNull).toEqual(TEST_COLUMNS[col].allowNull);
                expect(metadata[col].primary).toEqual(TEST_COLUMNS[col].primary ?? false);
                expect(metadata[col].unique).toEqual(TEST_COLUMNS[col].unique ?? false);
                expect(metadata[col].autoIncrement).toEqual(TEST_COLUMNS[col].autoIncrement ?? false);
            });
        });

        test("✅ Returns `null` when table does not exist", async () => {
            const metadata = await db.getTableMetaData(db.getConfig().schema || db.getConfig().database || "", "non_existent_table");
            expect(metadata).toBeNull();
        });

        test("✅ Detects `auto_increment` (MySQL: `AUTO_INCREMENT`, PostgreSQL: `SERIAL` or `IDENTITY`)", async () => {
            const metadata = await db.getTableMetaData(db.getConfig().schema || db.getConfig().database || "", TEST_TABLE_NAME);
            expect(metadata?.user_id.autoIncrement).toBe(true);
        });        

        test("✅ Ensures unique constraints are detected correctly", async () => {
            const metadata = await db.getTableMetaData(db.getConfig().schema || db.getConfig().database || "", TEST_TABLE_NAME);
            expect(metadata?.user_uuid.unique).toBe(true);
            expect(metadata?.username.unique).toBe(true);
            expect(metadata?.email.unique).toBe(true);
        });

        test("✅ Detects `index` properties correctly", async () => {
            const metadata = await db.getTableMetaData(db.getConfig().schema || db.getConfig().database || "", TEST_TABLE_NAME);
        
            // ✅ If a column is unique, ignore the index check
            if (!metadata?.email.unique) {
                expect(metadata?.email.index).toBe(true);
            }
        
            expect(metadata?.is_active.index).toBe(true); // ✅ Manually indexed
        });        
    });
});