import { DB_CONFIG, Database } from "./utils/testConfig";
import { ColumnDefinition } from "../src/helpers/metadata";

const TEST_TABLE_NAME = "test_table";

const TEST_COLUMNS: { [column: string]: ColumnDefinition }[] = [
    {
        id: {
            type: "int",
            length: 11,
            primary: true,
            autoIncrement: true,
            allowNull: false
        }
    },
    {
        name: {
            type: "varchar",
            length: 255,
            allowNull: false,
            unique: true
        }
    },
    {
        is_active: {
            type: "boolean",
            allowNull: false,
            default: false
        }
    }
];

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Create Table Query Tests for ${config.sql_dialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(() => {
            db = Database.create(config);
        });

        test("Generate valid CREATE TABLE queries", async () => {
            const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            
            expect(Array.isArray(queries)).toBe(true); // Ensure it's an array
            expect(typeof queries[0]).toBe("string");
            expect(queries[0].toLowerCase()).toContain(`create table`);
            expect(queries[0].toLowerCase()).toContain(TEST_TABLE_NAME.toLowerCase());
        });

        test("Ensure table definition contains all columns", async () => {
            const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            const createTableQuery = queries[0]; // First query is always `CREATE TABLE`

            if (config.sql_dialect === "mysql") {
                expect(createTableQuery).toContain("`id` int AUTO_INCREMENT NOT NULL");
                expect(createTableQuery).toContain("`name` varchar(255) NOT NULL");
                expect(createTableQuery).toContain("`is_active` TINYINT(1) NOT NULL DEFAULT false");
            } else if (config.sql_dialect === "pgsql") {
                expect(createTableQuery).toContain("\"id\" SERIAL NOT NULL");
                expect(createTableQuery).toContain("\"name\" varchar(255) NOT NULL");
                expect(createTableQuery).toContain("\"is_active\" boolean NOT NULL DEFAULT false");
            }
        });

        test("Ensure index queries are separate", async () => {
            const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            
            if (queries.length > 1) {
                for (let i = 1; i < queries.length; i++) {
                    expect(queries[i].toLowerCase()).toContain("create index");
                }
            }
        });

        if (config.sql_dialect === "mysql") {
            test("Check MySQL-specific query format", async () => {
                const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
                const createTableQuery = queries[0];

                expect(createTableQuery).toContain("AUTO_INCREMENT");
                expect(createTableQuery).toContain("ENGINE=InnoDB");
                expect(createTableQuery).toContain("PRIMARY KEY (`id`)");
                expect(createTableQuery).toContain("UNIQUE(`name`)");

                if (queries.length > 1) {
                    expect(queries[1]).toContain("CREATE INDEX `name_idx` ON `test_table` (`name`);");
                }
            });
        } else if (config.sql_dialect === "pgsql") {
            test("Check PostgreSQL-specific query format", async () => {
                const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
                const createTableQuery = queries[0];

                expect(createTableQuery).toContain("SERIAL");
                expect(createTableQuery).toContain("PRIMARY KEY (\"id\")");
                expect(createTableQuery).toContain("UNIQUE(\"name\")");

                if (queries.length > 1) {
                    expect(queries[1]).toContain("CREATE INDEX \"name_idx\" ON \"test_table\" (\"name\");");
                }
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
        });

        afterAll(async () => {
            const closeResult = await db.closeConnection();
            expect(closeResult.success).toBe(true);
        });

        test('Valid queries pass testQuery without errors', async () => {
            const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);

            for (const query of queries) {
                console.log(query);
                await expect(db.testQuery(query)).resolves.not.toThrow();
            }
        });
    });
});
