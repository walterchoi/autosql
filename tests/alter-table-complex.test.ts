import { DB_CONFIG, Database } from "./utils/testConfig";
import { ColumnDefinition, MetadataHeader } from "../src/config/types";

const TEST_TABLE_NAME = "alter_table_complex_test_table";

const BASE_COLUMNS: MetadataHeader = {
    id: { type: "int", length: 11, primary: true, allowNull: false },
    name: { type: "varchar", length: 50, allowNull: false, unique: true },
    created_at: { type: "datetime", allowNull: false }
};

const MODIFIED_COLUMNS_1: MetadataHeader = {
    id: { type: "int", length: 11, primary: true, allowNull: false },
    name: { type: "varchar", length: 100, allowNull: false, unique: true },
    created_at: { type: "datetime", allowNull: false },
    email: { type: "varchar", length: 255, allowNull: false, unique: true }
};

const MODIFIED_COLUMNS_2: MetadataHeader = {
    uuid: { type: "int", length: 11, primary: true, allowNull: false },
    name: { type: "varchar", length: 100, allowNull: false, unique: true },
    created_at: { type: "datetime", allowNull: false },
    email: { type: "varchar", length: 255, allowNull: false, unique: true }
};

const MODIFIED_COLUMNS_3: MetadataHeader = {
    id: { type: "int", length: 11, allowNull: false }, // Removed as primary key
    username: { type: "varchar", length: 100, allowNull: false, unique: true },
    email: { type: "varchar", length: 255, allowNull: false, unique: true, primary: true } // New primary key
};

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Alter Table Query Tests for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();
            
            // Create the initial table
            const queries = db.createTableQuery(TEST_TABLE_NAME, BASE_COLUMNS);
            for (const query of queries) {
                await db.runQuery(query);
            }
        });

        afterAll(async () => {
            const dropQuery = db.dropTableQuery(TEST_TABLE_NAME);
            await db.runQuery(dropQuery);
            const closeResult = await db.closeConnection();
            expect(closeResult.success).toBe(true);
        });

        test("Generate and execute ALTER TABLE query - Modify column & add new column", async () => {
            const queries = await db.alterTableQuery(TEST_TABLE_NAME, BASE_COLUMNS, MODIFIED_COLUMNS_1);
            
            expect(Array.isArray(queries)).toBe(true);
            expect(queries.length).toBeGreaterThan(0);

            for (const query of queries) {
                await expect(db.testQuery(query)).resolves.not.toThrow();
            }

            const firstQuery = queries[0];
            const alterTableQuery = typeof firstQuery === "string" ? firstQuery : firstQuery.query;

            if (config.sqlDialect === "mysql") {
                expect(alterTableQuery).toContain("MODIFY COLUMN `name` varchar(100) NOT NULL");
                expect(alterTableQuery).toContain("ADD COLUMN `email` varchar(255) NOT NULL");
            } else if (config.sqlDialect === "pgsql") {
                expect(alterTableQuery).toContain("ALTER COLUMN \"name\" SET DATA TYPE varchar(100)");
                expect(alterTableQuery).toContain("ADD COLUMN \"email\" varchar(255) NOT NULL");
            }
        });

        test("Generate and execute ALTER TABLE query - Modify primary key (if enabled)", async () => {
            if (!config.updatePrimaryKey) return; // Skip if auto primary key updates are disabled
        
            const queries = await db.alterTableQuery(TEST_TABLE_NAME, MODIFIED_COLUMNS_1, MODIFIED_COLUMNS_2);
            expect(Array.isArray(queries)).toBe(true);
            expect(queries.length).toBeGreaterThan(0);
        
            await expect(db.runTransaction(queries)).resolves.not.toThrow();
        
            const queryStrings = queries.map(q => (typeof q === "string" ? q : q.query));
        
            if (config.sqlDialect === "mysql") {
                expect(queryStrings).toContain('ALTER TABLE `test_schema`.`alter_table_complex_test_table` DROP PRIMARY KEY;');
                expect(queryStrings).toContain('ALTER TABLE `test_schema`.`alter_table_complex_test_table` CHANGE COLUMN `id` `uuid` int(11) NOT NULL;');
                expect(queryStrings).toContain('ALTER TABLE `test_schema`.`alter_table_complex_test_table` ADD PRIMARY KEY (`uuid`);');
            } else if (config.sqlDialect === "pgsql") {
                expect(queryStrings).toContain('ALTER TABLE "test_schema"."alter_table_complex_test_table" DROP CONSTRAINT "alter_table_complex_test_table_pkey";');
                expect(queryStrings).toContain('ALTER TABLE "test_schema"."alter_table_complex_test_table" RENAME COLUMN "id" TO "uuid";');
                expect(queryStrings).toContain('ALTER TABLE "test_schema"."alter_table_complex_test_table" ADD PRIMARY KEY ("uuid");');
            }
        });
    });        
});
