import { DB_CONFIG, Database } from "./utils/testConfig";
import { ColumnDefinition } from "../src/helpers/metadata";

const TEST_TABLE_NAME = "users";

const TEST_COLUMNS: { [column: string]: ColumnDefinition }[] = [
    {
        user_id: {
            type: "int",
            length: 11,
            primary: true,
            autoIncrement: true,
            allowNull: false
        }
    },
    {
        user_uuid: {
            type: "varchar",
            length: 36,
            allowNull: false,
            unique: true,
            default: "UUID()"
        }
    },
    {
        username: {
            type: "varchar",
            length: 50,
            allowNull: false,
            unique: true
        }
    },
    {
        email: {
            type: "varchar",
            length: 255,
            allowNull: false,
            unique: true,
            index: true
        }
    },
    {
        password_hash: {
            type: "varchar",
            length: 255,
            allowNull: false
        }
    },
    {
        bio: {
            type: "text",
            allowNull: true
        }
    },
    {
        age: {
            type: "int",
            length: 3,
            allowNull: true,
            default: 18,
        }
    },
    {
        is_active: {
            type: "boolean",
            allowNull: false,
            default: true
        }
    },
    {
        account_balance: {
            type: "decimal",
            length: 12,
            decimal: 2,
            allowNull: false,
            default: 0.00
        }
    },
    {
        user_metadata: {
            type: "json",
            allowNull: true
        }
    },
    {
        created_at: {
            type: "datetime",
            allowNull: false,
            default: "CURRENT_TIMESTAMP"
        }
    },
    {
        updated_at: {
            type: "datetime",
            allowNull: false,
            default: "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        }
    }
];

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Complex Create Table Query Tests for ${config.sql_dialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(() => {
            db = Database.create(config);
        });

        test("Generate valid CREATE TABLE queries", async () => {
            const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            
            expect(Array.isArray(queries)).toBe(true);
            expect(typeof queries[0]).toBe("string");
            expect(queries[0].toLowerCase()).toContain(`create table`);
            expect(queries[0].toLowerCase()).toContain(TEST_TABLE_NAME.toLowerCase());
        });

        test("Ensure table definition contains all columns", async () => {
            const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            const createTableQuery = queries[0];

            if (config.sql_dialect === "mysql") {
                expect(createTableQuery).toContain("`user_id` int AUTO_INCREMENT NOT NULL");
                expect(createTableQuery).toContain("`user_uuid` varchar(36) NOT NULL DEFAULT (UUID())");
                expect(createTableQuery).toContain("`username` varchar(50) NOT NULL");
                expect(createTableQuery).toContain("`email` varchar(255) NOT NULL");
                expect(createTableQuery).toContain("`password_hash` varchar(255) NOT NULL");
                expect(createTableQuery).toContain("`bio` text");
                expect(createTableQuery).toContain("`age` int DEFAULT 18");
                expect(createTableQuery).toContain("`is_active` TINYINT(1) NOT NULL DEFAULT true");
                expect(createTableQuery).toContain("`account_balance` decimal(12,2) NOT NULL DEFAULT 0");
                expect(createTableQuery).toContain("`user_metadata` json");
                expect(createTableQuery).toContain("`created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP");
                expect(createTableQuery).toContain("`updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
            } else if (config.sql_dialect === "pgsql") {
                expect(createTableQuery).toContain("\"user_id\" SERIAL NOT NULL");
                expect(createTableQuery).toContain("\"user_uuid\" varchar(36) NOT NULL DEFAULT gen_random_uuid()");
                expect(createTableQuery).toContain("\"username\" varchar(50) NOT NULL");
                expect(createTableQuery).toContain("\"email\" varchar(255) NOT NULL");
                expect(createTableQuery).toContain("\"password_hash\" varchar(255) NOT NULL");
                expect(createTableQuery).toContain("\"bio\" text");
                expect(createTableQuery).toContain("\"age\" int DEFAULT 18");
                expect(createTableQuery).toContain("\"is_active\" boolean NOT NULL DEFAULT true");
                expect(createTableQuery).toContain("\"account_balance\" decimal NOT NULL DEFAULT 0");
                expect(createTableQuery).toContain("\"user_metadata\" json");
                expect(createTableQuery).toContain("\"created_at\" timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP");
                expect(createTableQuery).toContain("\"updated_at\" timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP");
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

        test("Check SQL dialect-specific query format", async () => {
            const queries = db.createTableQuery(TEST_TABLE_NAME, TEST_COLUMNS);
            const createTableQuery = queries[0];

            if (config.sql_dialect === "mysql") {
                expect(createTableQuery).toContain("ENGINE=InnoDB");
                expect(createTableQuery).toContain("PRIMARY KEY (`user_id`)");
                expect(createTableQuery).toContain("UNIQUE(`user_uuid`)");
                expect(createTableQuery).toContain("UNIQUE(`username`)");
                expect(createTableQuery).toContain("UNIQUE(`email`)");
                if (queries.length > 1) {
                    expect(queries).toContainEqual(expect.stringMatching(/CREATE INDEX `email_idx` ON `users` \(`email`\);/));
                }
            } else if (config.sql_dialect === "pgsql") {
                expect(createTableQuery).toContain("PRIMARY KEY (\"user_id\")");
                expect(createTableQuery).toContain("UNIQUE(\"user_uuid\")");
                expect(createTableQuery).toContain("UNIQUE(\"username\")");
                expect(createTableQuery).toContain("UNIQUE(\"email\")");
                if (queries.length > 1) {
                    expect(queries).toContainEqual(expect.stringMatching(/CREATE INDEX "email_idx" ON "users" \("email"\);/));
                }
            }
        });
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
                if (config.sql_dialect === "pgsql" && query.toLowerCase().startsWith("create index")) {
                    console.log("Skipping index validation:", query);
                    continue;
                }
                await expect(db.testQuery(query)).resolves.not.toThrow();
            }
        });
    });
});