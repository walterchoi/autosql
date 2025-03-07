import { DB_CONFIG, Database } from "./utils/testConfig";
import { ColumnDefinition } from "../src/config/types";

const TEST_TABLE_NAME = "test_table";

const OLD_COLUMNS: { [column: string]: ColumnDefinition }[] = [
    { id: { type: "int", length: 11, primary: true, allowNull: false, autoIncrement: true } },
    { name: { type: "varchar", length: 50, allowNull: false, unique: true } }
];

const NEW_COLUMNS: { [column: string]: ColumnDefinition }[] = [
    { id: { type: "int", length: 11, primary: true, allowNull: false, autoIncrement: true } },
    { name: { type: "varchar", length: 100, allowNull: false, unique: true } },
    { email: { type: "varchar", length: 255, allowNull: false } }
];

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Alter Table Query Tests for ${config.sql_dialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(() => {
            db = Database.create(config);
        });

        test("Generate valid ALTER TABLE queries", async () => {
            const queries = await db.alterTableQuery(TEST_TABLE_NAME, OLD_COLUMNS, NEW_COLUMNS);

            expect(Array.isArray(queries)).toBe(true);
            expect(queries.length).toBeGreaterThan(0);

            const firstQuery = queries[0];

            // Ensure the first query is a string or an object with .query
            expect(typeof firstQuery === "string" || typeof firstQuery === "object").toBe(true);

            const queryStr = typeof firstQuery === "string" ? firstQuery : firstQuery.query;

            expect(queryStr.toLowerCase()).toContain(`alter table`);
            expect(queryStr.toLowerCase()).toContain(TEST_TABLE_NAME.toLowerCase());
        });

        test("Ensure table modification contains all changes", async () => {
            const queries = await db.alterTableQuery(TEST_TABLE_NAME, OLD_COLUMNS, NEW_COLUMNS);
            const firstQuery = queries[0];
            const alterTableQuery = typeof firstQuery === "string" ? firstQuery : firstQuery.query;

            if (config.sql_dialect === "mysql") {
                expect(alterTableQuery).toContain("ADD COLUMN `email` varchar(255) NOT NULL");
                expect(alterTableQuery).toContain("MODIFY COLUMN `name` varchar(100) NOT NULL");
            } else if (config.sql_dialect === "pgsql") {
                expect(alterTableQuery).toContain("ADD COLUMN \"email\" varchar(255) NOT NULL");
                expect(alterTableQuery).toContain("ALTER COLUMN \"name\" SET DATA TYPE varchar(100)");
            }
        });

        if (config.sql_dialect === "mysql") {
            test("Check MySQL-specific query format", async () => {
                const queries = await db.alterTableQuery(TEST_TABLE_NAME, OLD_COLUMNS, NEW_COLUMNS);
                const firstQuery = queries[0];
                const alterTableQuery = typeof firstQuery === "string" ? firstQuery : firstQuery.query;

                expect(alterTableQuery).toContain("MODIFY COLUMN `name` varchar(100) NOT NULL");
                expect(alterTableQuery).toContain("ADD COLUMN `email` varchar(255) NOT NULL");
            });
        } else if (config.sql_dialect === "pgsql") {
            test("Check PostgreSQL-specific query format", async () => {
                const queries = await db.alterTableQuery(TEST_TABLE_NAME, OLD_COLUMNS, NEW_COLUMNS);
                const firstQuery = queries[0];
                const alterTableQuery = typeof firstQuery === "string" ? firstQuery : firstQuery.query;

                expect(alterTableQuery).toContain("ALTER COLUMN \"name\" SET DATA TYPE varchar(100)");
                expect(alterTableQuery).toContain("ADD COLUMN \"email\" varchar(255) NOT NULL");
            });
        }
    });
});

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Validate Create Table Query Tests for ${config.sql_dialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();
            const queries = db.createTableQuery(TEST_TABLE_NAME, OLD_COLUMNS);
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

        test('Valid queries pass testQuery without errors', async () => {
            const queries = await db.alterTableQuery(TEST_TABLE_NAME, OLD_COLUMNS, NEW_COLUMNS);

            for (const query of queries) {
                await expect(db.testQuery(query)).resolves.not.toThrow();
            }
        });
    });
});