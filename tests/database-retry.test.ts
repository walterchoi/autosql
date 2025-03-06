import { DB_CONFIG, Database, isValidSingleQuery } from "./utils/testConfig";
import { QueryInput, QueryWithParams } from "../src/config/types";
jest.setTimeout(10000);

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Database Retry Logic Handling Tests for ${config.sql_dialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();
        });

        afterAll(async () => {
            const closeResult = await db.closeConnection();
            expect(closeResult.success).toBe(true);
        });

        test('Retries temporary errors up to 3 times before succeeding', async () => {
            let attemptCount = 0;
            jest.spyOn(db as any, 'executeQuery').mockImplementation(async () => {
                attemptCount++;
                if (attemptCount < 3) {
                    const tempError = new Error("Temporary error");
                    if(config.sql_dialect == 'mysql') {
                        (tempError as any).code = "ER_LOCK_DEADLOCK"; // Example MySQL temporary error
                    }
                    else if (config.sql_dialect === 'pgsql') {
                        (tempError as any).code = "40P01"; // PostgreSQL deadlock error
                    }
                    throw tempError;
                }
                return [{ id: 1 }];
            });
    
            const result = await db.runQuery('SELECT * FROM users');
            expect(result).toEqual([{ id: 1 }]);
            expect(attemptCount).toBe(3);
        });
    
        test('Throws permanent errors immediately without retrying', async () => {
            let attemptCount = 0;
            jest.spyOn(db as any, 'executeQuery').mockImplementation(async () => {
                attemptCount++;
                const permanentError = new Error("Syntax error");
                if(config.sql_dialect == 'mysql') {
                    (permanentError as any).code = "ER_SYNTAX_ERROR"; // MySQL permanent error
                }
                else if (config.sql_dialect === 'pgsql') {
                    (permanentError as any).code = "42601"; // PostgreSQL permanent error
                }
                throw permanentError;
            });
    
            await expect(db.runQuery('INVALID SQL')).rejects.toThrow("Syntax error");
            expect(attemptCount).toBe(1); // No retries
        });
    
        test('Retries transactions with temporary errors and rolls back on failure', async () => {
            let attemptCount = 0;
            jest.spyOn(db as any, 'executeQuery').mockImplementation(async (queryOrParams: unknown) => {
                const queryStr = typeof queryOrParams === "string" 
                    ? queryOrParams 
                    : (queryOrParams as QueryWithParams).query;
            
                if (queryStr.startsWith("BEGIN") || queryStr.startsWith("COMMIT") || queryStr.startsWith("ROLLBACK")) {
                    return;
                }
                if (queryStr.startsWith("UPDATE")) {
                    attemptCount++;
                    if (attemptCount < 3) {
                        const tempError = new Error("Deadlock");
                        if (config.sql_dialect === 'mysql') {
                            (tempError as any).code = "ER_LOCK_DEADLOCK"; // MySQL temporary error
                        } else if (config.sql_dialect === 'pgsql') {
                            (tempError as any).code = "40P01"; // PostgreSQL deadlock error
                        }
                        throw tempError;
                    }
                }
                return { affectedRows: 1 };
            });
            
        
            const transactionResult = await db.runTransaction([
                { query: "UPDATE users SET name='Test' WHERE id=1" },
                { query: "UPDATE users SET name='Another' WHERE id=2" },
            ]);
        
            expect(transactionResult.success).toBe(true);
            expect(attemptCount).toBe(4);
        });
    
        test('Fails transaction immediately on permanent error', async () => {
            let attemptCount = 0;
            jest.spyOn(db as any, 'executeQuery').mockImplementation(async (queryOrParams: unknown) => {
                const queryStr = typeof queryOrParams === "string" 
                    ? queryOrParams 
                    : (queryOrParams as QueryWithParams).query;
            
                if (queryStr.startsWith("BEGIN") || queryStr.startsWith("COMMIT") || queryStr.startsWith("ROLLBACK")) {
                    return;
                }
                if (queryStr.startsWith("UPDATE")) {
                    attemptCount++;
                    const permanentError = new Error("Column not found");
            
                    if (config.sql_dialect === 'mysql') {
                        (permanentError as any).code = "ER_BAD_FIELD_ERROR"; // MySQL permanent error
                    } else if (config.sql_dialect === 'pgsql') {
                        (permanentError as any).code = "42P01"; // PostgreSQL permanent error
                    }
                    
                    throw permanentError;
                }
                return { affectedRows: 1 };
            });            
    
            const transactionResult = await db.runTransaction([
                "UPDATE users SET non_existent_column='Test' WHERE id=1",
            ]);
    
            expect(transactionResult.success).toBe(false);
            expect(transactionResult.error).toBe("Column not found");
            expect(attemptCount).toBe(1);
        });
    });
});