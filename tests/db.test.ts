import fs from "fs";
import { DatabaseConfig, Database } from "../src/db/database";
import path from "path";

const CONFIG_PATH = path.resolve(__dirname, "../src/config/config.local.json");
const DB_CONFIG: Record<string, DatabaseConfig> = fs.existsSync(CONFIG_PATH)
    ? (JSON.parse(fs.readFileSync(CONFIG_PATH, "utf-8")) as Record<string, DatabaseConfig>)
    : {
          mysql: {
              sql_dialect: "mysql",
              host: "localhost",
              user: "root",
              password: "root",
              database: "mysql",
              port: 3306
          },
          pgsql: {
              sql_dialect: "pgsql",
              host: "localhost",
              user: "test_user",
              password: "test_password",
              database: "postgres",
              port: 5432
          }
      };
    
Object.values(DB_CONFIG).forEach((config) => {
        describe(`Database Tests for ${config.sql_dialect.toUpperCase()}`, () => {
            let db: Database;
    
            beforeAll(async () => {
                db = Database.create(config);
                await db.establishConnection();
            });
    
            afterAll(async () => {
                const closeResult = await db.closeConnection();
                expect(closeResult.success).toBe(true);
            });
    
            test("Connection should be successful", async () => {
                const success = await db.testConnection();
                expect(success).toBe(true);
            });
            
            test("Create schema", async () => {
                const result = await db.createSchema("test_schema");
                expect(result.success).toBe(true);
            });

            test("Create schema that already exists", async () => {
                await db.createSchema("test_schema"); // First creation
                const result = await db.createSchema("test_schema"); // Second attempt
                expect(result.success).toBe(true);
            });

            test("Check if schema exists", async () => {
                await db.createSchema("test_schema");
                const result = await db.checkSchemaExists("test_schema");
                expect(result["test_schema"]).toBe(true);
            });

            test("Run a basic query", async () => {
                const result = await db.runQuery("SELECT 1 AS test;");
                expect(result[0].test).toBe(1);
            });    

        });
    });

    
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