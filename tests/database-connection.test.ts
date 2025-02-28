import { DB_CONFIG, Database, isValidSingleQuery } from "./utils/testConfig";
    
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
