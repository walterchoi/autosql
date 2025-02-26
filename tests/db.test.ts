import fs from "fs";
import { Database } from "../src/db/database";
import { MySQLDatabase } from "../src/db/mysql";
import { PostgresDatabase } from "../src/db/postgresql";

const CONFIG_PATH = "./src/config/config.local.json";
const DB_CONFIG = fs.existsSync(CONFIG_PATH)
    ? JSON.parse(fs.readFileSync(CONFIG_PATH, "utf-8"))
    : {
          mysql: {
              host: "localhost",
              user: "root",
              password: "test_password",
              database: "mysql",
              port: 3306
          },
          pgsql: {
              host: "localhost",
              user: "root",
              password: "test_password",
              database: "postgres",
              port: 5432
          }
      };

describe("Database Connection Tests", () => {
    let mysqlDb: Database;
    let pgDb: Database;

    beforeAll(async () => {
        mysqlDb = new MySQLDatabase(DB_CONFIG.mysql);
        pgDb = new PostgresDatabase(DB_CONFIG.pgsql);

        await mysqlDb.establishConnection();
        await pgDb.establishConnection();
    });

    afterAll(async () => {
        // Drop schema and recreate it (Ensures a fresh test environment)
        await mysqlDb.runQuery("DROP SCHEMA IF EXISTS test_schema;");
        
        await pgDb.runQuery("DROP SCHEMA IF EXISTS test_schema CASCADE;");
    
        // Close database connections to prevent Jest from hanging
        await mysqlDb.closeConnection();
        await pgDb.closeConnection();
    });
    
    test("MySQL connection should be successful", async () => {
        const success = await mysqlDb.testConnection();
        expect(success).toBe(true);
    });

    test("PostgreSQL connection should be successful", async () => {
        const success = await pgDb.testConnection();
        expect(success).toBe(true);
    });

    describe("Database Schema Tests", () => {
        test("Create schema (MySQL)", async () => {
            const result = await mysqlDb.createSchema("test_schema");
            expect(result.success).toBe(true);
        });
    
        test("Create schema (PostgreSQL)", async () => {
            const result = await pgDb.createSchema("test_schema");
            expect(result.success).toBe(true);
        });
    
        test("Create schema that already exists (MySQL)", async () => {
            await mysqlDb.createSchema("test_schema"); // First creation
            const result = await mysqlDb.createSchema("test_schema"); // Second attempt
            expect(result.success).toBe(true);
        });
    
        test("Create schema that already exists (PostgreSQL)", async () => {
            await pgDb.createSchema("test_schema"); // First creation
            const result = await pgDb.createSchema("test_schema"); // Second attempt
            expect(result.success).toBe(true);
        });

        test("Check if schema exists (MySQL)", async () => {
            const result = await mysqlDb.checkSchemaExists("test_schema");
            expect(result["test_schema"]).toBe(true);
        });
    
        test("Check if schema exists (PostgreSQL)", async () => {
            const result = await pgDb.checkSchemaExists("test_schema");
            expect(result["test_schema"]).toBe(true);
        });
    });

    describe("Database Query Tests", () => {
        test("Run a basic query (MySQL)", async () => {
            const result = await mysqlDb.runQuery("SELECT 1 AS test;");
            expect(result[0].test).toBe(1);
        });
    
        test("Run a basic query (PostgreSQL)", async () => {
            const result = await pgDb.runQuery("SELECT 1 AS test;");
            expect(result[0].test).toBe(1);
        });
    })

    describe("Database Error Handling Tests", () => {
        /** ✅ Invalid SQL Syntax (MySQL & PostgreSQL) */
        test("Invalid SQL Syntax (MySQL)", async () => {
            try {
                await mysqlDb.runQuery("INVALID SQL SYNTAX;");
                fail("Expected query to throw an error");
            } catch (error) {
                expect(error).toBeDefined();
                console.log("Caught MySQL Syntax Error:", error instanceof Error ? error.message : String(error));
            }
        });
    
        test("Invalid SQL Syntax (PostgreSQL)", async () => {
            try {
                await pgDb.runQuery("INVALID SQL SYNTAX;");
                fail("Expected query to throw an error");
            } catch (error) {
                expect(error).toBeDefined();
                console.log("Caught PostgreSQL Syntax Error:", error instanceof Error ? error.message : String(error));
            }
        });
    
        /** ✅ Inserting Wrong Data Types */
        test("Insert wrong data type (MySQL)", async () => {
            try {
                await mysqlDb.runQuery("INSERT INTO test_schema.test_table (id) VALUES ('string');");
                fail("Expected query to throw an error");
            } catch (error) {
                expect(error).toBeDefined();
                console.log("Caught MySQL Data Type Error:", error instanceof Error ? error.message : String(error));
            }
        });
    
        test("Insert wrong data type (PostgreSQL)", async () => {
            try {
                await pgDb.runQuery("INSERT INTO test_schema.test_table (id) VALUES ('string');");
                fail("Expected query to throw an error");
            } catch (error) {
                expect(error).toBeDefined();
                console.log("Caught PostgreSQL Data Type Error:", error instanceof Error ? error.message : String(error));
            }
        });
    
        /** ✅ Using Nonexistent Tables */
        test("Query non-existent table (MySQL)", async () => {
            try {
                await mysqlDb.runQuery("SELECT * FROM non_existent_table;");
                fail("Expected query to throw an error");
            } catch (error) {
                expect(error).toBeDefined();
                console.log("Caught MySQL Table Not Found Error:", error instanceof Error ? error.message : String(error));
            }
        });
    
        test("Query non-existent table (PostgreSQL)", async () => {
            try {
                await pgDb.runQuery("SELECT * FROM non_existent_table;");
                fail("Expected query to throw an error");
            } catch (error) {
                expect(error).toBeDefined();
                console.log("Caught PostgreSQL Table Not Found Error:", error instanceof Error ? error.message : String(error));
            }
        });
    
        /** ✅ Transaction Errors */
        test("Transaction rollback on failure (MySQL)", async () => {
            try {
                await mysqlDb.startTransaction();
                await mysqlDb.runQuery("INSERT INTO test_schema.test_table (id) VALUES (1);");
                await mysqlDb.runQuery("INVALID SQL SYNTAX;"); // Intentional failure
                await mysqlDb.commit(); // Should not reach this point
                fail("Expected transaction to fail");
            } catch (error) {
                await mysqlDb.rollback(); // Ensure rollback is called
                expect(error).toBeDefined();
                console.log("Caught MySQL Transaction Error:", error instanceof Error ? error.message : String(error));
            }
        });
    
        test("Transaction rollback on failure (PostgreSQL)", async () => {
            try {
                await pgDb.startTransaction();
                await pgDb.runQuery("INSERT INTO test_schema.test_table (id) VALUES (1);");
                await pgDb.runQuery("INVALID SQL SYNTAX;"); // Intentional failure
                await pgDb.commit(); // Should not reach this point
                fail("Expected transaction to fail");
            } catch (error) {
                await pgDb.rollback(); // Ensure rollback is called
                expect(error).toBeDefined();
                console.log("Caught PostgreSQL Transaction Error:", error instanceof Error ? error.message : String(error));
            }
        });
    });    
    
});