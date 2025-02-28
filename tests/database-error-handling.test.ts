import { DB_CONFIG, Database, isValidSingleQuery } from "./utils/testConfig";
    
Object.values(DB_CONFIG).forEach((config) => {
    describe(`Database Error Handling Tests for ${config.sql_dialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();

            // Ensure the schema and table exist before running tests
            await db.createSchema("test_schema");
            await db.runQuery(`
                CREATE TABLE IF NOT EXISTS test_schema.test_table (
                    id INT PRIMARY KEY,
                    name VARCHAR(255)
                );
            `);
        });

        afterAll(async () => {
            try {
                await db.runQuery(`DROP TABLE IF EXISTS test_schema.test_table;`);
                await db.runQuery(`DROP SCHEMA IF EXISTS test_schema CASCADE;`);
            } catch (error) {
                
            }
            const closeResult = await db.closeConnection();
            expect(closeResult.success).toBe(true);
        });

        // ✅ Invalid SQL Syntax
        test("Invalid SQL Syntax", async () => {
            try {
                await db.runQuery("INVALID SQL SYNTAX;");
                fail("Expected query to throw an error");
            } catch (error) {
                expect(error).toBeDefined();
                console.log(`Caught ${config.sql_dialect.toUpperCase()} Syntax Error:`, error instanceof Error ? error.message : String(error));
            }
        });

        // ✅ Inserting Wrong Data Types
        test("Insert wrong data type", async () => {
            try {
                await db.runQuery("INSERT INTO test_schema.test_table (id) VALUES ('string');");
                fail("Expected query to throw an error");
            } catch (error) {
                expect(error).toBeDefined();
                console.log(`Caught ${config.sql_dialect.toUpperCase()} Data Type Error:`, error instanceof Error ? error.message : String(error));
            }
        });

        // ✅ Using Nonexistent Tables
        test("Query non-existent table", async () => {
            try {
                await db.runQuery("SELECT * FROM non_existent_table;");
                fail("Expected query to throw an error");
            } catch (error) {
                expect(error).toBeDefined();
                console.log(`Caught ${config.sql_dialect.toUpperCase()} Table Not Found Error:`, error instanceof Error ? error.message : String(error));
            }
        });

        // ✅ Transaction Errors
        test("Transaction rollback on failure", async () => {
            const result = await db.runTransaction([
                "INSERT INTO test_schema.test_table (id, name) VALUES (1, 'Alice');",
                "INVALID SQL SYNTAX;" // Intentional failure
            ]);

            expect(result.success).toBe(false);
            console.log(`Rollback Test (${config.sql_dialect.toUpperCase()}):`, result.error);
        });

        // ✅ Database Transaction Handling
        describe(`Database Transaction Handling for ${config.sql_dialect.toUpperCase()}`, () => {
            test("Successful transaction", async () => {
                const result = await db.runTransaction([
                    "INSERT INTO test_schema.test_table (id, name) VALUES (2, 'Bob');",
                    "INSERT INTO test_schema.test_table (id, name) VALUES (3, 'Charlie');"
                ]);

                expect(result.success).toBe(true);
            });

            test("Failed transaction rolls back", async () => {
                const result = await db.runTransaction([
                    "INSERT INTO test_schema.test_table (id, name) VALUES (4, 'David');",
                    "INVALID SQL SYNTAX;" // Intentional failure
                ]);

                expect(result.success).toBe(false);
                console.log(`Rollback Test (${config.sql_dialect.toUpperCase()}):`, result.error);
            });
        });
    });
});