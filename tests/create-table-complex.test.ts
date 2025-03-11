import { DB_CONFIG, Database } from "./utils/testConfig";
import { ColumnDefinition, QueryWithParams, MetadataHeader } from "../src/config/types";

const TEST_TABLE_NAME = "create_table_complex_test_table";

const TEST_COLUMNS: MetadataHeader = {
    user_id: {
        type: "int",
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
        allowNull: true,
        default: 18
    },
    is_active: {
        type: "boolean",
        allowNull: false,
        default: true
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
    describe(`Complex Create Table Query Tests for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(() => {
            db = Database.create(config);
        });

        test("Generate valid CREATE TABLE queries", async () => {
            const createTableQuery = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            const queryStr = typeof createTableQuery[0] === "string" 
                ? createTableQuery[0] 
                : "query" in createTableQuery[0]
                    ? createTableQuery[0].query 
                : (() => { throw new Error("Unexpected query format"); })();
            
            expect(typeof queryStr).toBe("string");
            expect(queryStr.toLowerCase()).toContain(`create table`);
            expect(queryStr.toLowerCase()).toContain(TEST_TABLE_NAME.toLowerCase());
        });

        test("Ensure table definition contains all expected columns", async () => {
            const createTableQuery = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            const queryStr = typeof createTableQuery[0] === "string" 
                ? createTableQuery[0] 
                : "query" in createTableQuery[0]
                    ? createTableQuery[0].query 
                : (() => { throw new Error("Unexpected query format"); })();

            if (config.sqlDialect === "mysql") {
                expect(queryStr).toContain("`user_id` int AUTO_INCREMENT NOT NULL");
                expect(queryStr).toContain("`user_uuid` varchar(36) NOT NULL DEFAULT (UUID())");
                expect(queryStr).toContain("`username` varchar(50) NOT NULL");
                expect(queryStr).toContain("`email` varchar(255) NOT NULL");
                expect(queryStr).toContain("`is_active` TINYINT(1) NOT NULL DEFAULT true");
                expect(queryStr).toContain("PRIMARY KEY (`user_id`)");
                expect(queryStr).toContain("UNIQUE(`user_uuid`)");
            } else if (config.sqlDialect === "pgsql") {
                expect(queryStr).toContain("\"user_id\" SERIAL NOT NULL");
                expect(queryStr).toContain("\"user_uuid\" varchar(36) NOT NULL DEFAULT gen_random_uuid()");
                expect(queryStr).toContain("\"username\" varchar(50) NOT NULL");
                expect(queryStr).toContain("\"email\" varchar(255) NOT NULL");
                expect(queryStr).toContain("\"is_active\" boolean NOT NULL DEFAULT true");
                expect(queryStr).toContain("PRIMARY KEY (\"user_id\")");
                expect(queryStr).toContain("CONSTRAINT \"create_table_complex_test_table_user_uuid_key\" UNIQUE(\"user_uuid\")");
            }
        });

        test("Ensure index queries are created separately", async () => {
            const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
        
            expect(Array.isArray(queries)).toBe(true);
            expect(queries.length).toBeGreaterThan(1);
        
            for (let i = 1; i < queries.length; i++) {
                const queryItem = queries[i];
        
                const queryStr = typeof queryItem === "string" 
                    ? queryItem 
                    : (queryItem as QueryWithParams).query;
        
                expect(queryStr.toLowerCase()).toContain("create index");
            }
        });

        test("Check SQL dialect-specific query format", async () => {
            const createTableQuery = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            expect(Array.isArray(createTableQuery)).toBe(true);
            expect(createTableQuery.length).toBeGreaterThan(0);

            const firstQuery = createTableQuery[0];
            const queryStr = typeof firstQuery === "string" 
                ? firstQuery 
                : "query" in firstQuery 
                    ? firstQuery.query 
                    : (() => { throw new Error("Unexpected query format"); })();

            if (config.sqlDialect === "mysql") {
                expect(queryStr).toContain("ENGINE=InnoDB");
                expect(queryStr).toContain("PRIMARY KEY (`user_id`)");
                expect(queryStr).toContain("UNIQUE(`user_uuid`)");
            } else if (config.sqlDialect === "pgsql") {
                expect(queryStr).toContain("PRIMARY KEY (\"user_id\")");
                expect(queryStr).toContain("CONSTRAINT \"create_table_complex_test_table_user_uuid_key\" UNIQUE(\"user_uuid\")");
            }
        });        
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
                const queryStr = typeof query === "string" 
                    ? query
                    : "query" in query
                        ? query.query 
                    : (() => { throw new Error("Unexpected query format"); })();

                if (config.sqlDialect === "pgsql" && queryStr.toLowerCase().startsWith("create index")) {
                    console.log("Skipping index validation:", queryStr);
                    continue;
                }
                await expect(db.testQuery(query)).resolves.not.toThrow();
            }
        });
    });
});
