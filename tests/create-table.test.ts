import { DB_CONFIG, Database } from "./utils/testConfig";
import { ColumnDefinition, MetadataHeader } from "../src/config/types";

const TEST_TABLE_NAME = "create_table_test_table";

const TEST_COLUMNS: MetadataHeader = 
    {
        id: {
            type: "int",
            length: 11,
            primary: true,
            autoIncrement: true,
            allowNull: false
        },
        name: {
                type: "varchar",
                length: 255,
                allowNull: false,
                unique: true
        },
        is_active: {
                type: "boolean",
                allowNull: false,
                default: false
        }
    };

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Create Table Query Tests for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(() => {
            db = Database.create(config);
        });

        test("Generate valid CREATE TABLE queries", async () => {
            const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            
            expect(Array.isArray(queries)).toBe(true);
            expect(queries.length).toBeGreaterThan(0);
            
            const firstQuery = queries[0];
            const queryStr = typeof firstQuery === "string" 
            ? firstQuery 
            : "query" in firstQuery 
                ? firstQuery.query 
            : (() => { throw new Error("Unexpected query format"); })();
            
            expect(typeof queryStr).toBe("string");
            expect(queryStr.toLowerCase()).toContain("create table");
        });

        test("Ensure table definition contains all columns", async () => {
            const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            const createTableQuery = queries[0]; // First query is always `CREATE TABLE`
            const queryStr = typeof createTableQuery === "string" 
                ? createTableQuery 
                : "query" in createTableQuery 
                    ? createTableQuery.query 
                : (() => { throw new Error("Unexpected query format"); })();
        
            if (config.sqlDialect === "mysql") {
                expect(queryStr).toContain("`id` int(11) AUTO_INCREMENT NOT NULL");
                expect(queryStr).toContain("`name` varchar(255) NOT NULL");
                expect(queryStr).toContain("`is_active` TINYINT(1) NOT NULL DEFAULT false");
            } else if (config.sqlDialect === "pgsql") {
                expect(queryStr).toContain("\"id\" SERIAL NOT NULL");
                expect(queryStr).toContain("\"name\" varchar(255) NOT NULL");
                expect(queryStr).toContain("\"is_active\" boolean NOT NULL DEFAULT false");
            }
        });        

        test("Ensure index queries are separate", async () => {
            const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            
            if (queries.length > 1) {
                for (let i = 1; i < queries.length; i++) {
                    const query = queries[i];
                    const queryStr = typeof query === "string" 
                    ? query 
                    : "query" in query 
                        ? query.query 
                        : (() => { throw new Error("Unexpected query format"); })();
                    expect(typeof queryStr).toBe("string");
                    expect(queryStr.toLowerCase()).toContain("create index");
                }
            }
        });

        test(`Check ${config.sqlDialect}-specific query format`, async () => {
            const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            const createTableQuery = queries[0];
            let queryStr1;
            const queryStr = typeof createTableQuery === "string" 
                ? createTableQuery 
                : "query" in createTableQuery 
                    ? createTableQuery.query 
                    : (() => { throw new Error("Unexpected query format"); })();
            if (queries.length > 1) {
                queryStr1 = typeof queries[1] === "string" 
                ? queries[1] 
                : "query" in queries[1] 
                    ? queries[1].query 
                    : (() => { throw new Error("Unexpected query format"); })();
            }
            if (config.sqlDialect === "mysql") {
                expect(queryStr).toContain("AUTO_INCREMENT");
                expect(queryStr).toContain("ENGINE=InnoDB");
                expect(queryStr).toContain("PRIMARY KEY (`id`)");
                expect(queryStr).toContain("UNIQUE(`name`)");

                if(queryStr1) {
                    expect(queryStr1).toContain("CREATE INDEX `name_idx` ON `test_table` (`name`);");
                }
            } else if (config.sqlDialect === "pgsql") {
                expect(queryStr).toContain("SERIAL");
                expect(queryStr).toContain("PRIMARY KEY (\"id\")");
                expect(queryStr).toContain("UNIQUE(\"name\")");

                if(queryStr1) {
                    expect(queryStr1).toContain("CREATE INDEX \"name_idx\" ON \"test_table\" (\"name\");");
                }
            }
        })
    });
});

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Validate Create Table Query Tests for ${config.sqlDialect.toUpperCase()}`, () => {
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
