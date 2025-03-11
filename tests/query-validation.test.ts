import { DB_CONFIG, Database, isValidSingleQuery } from "./utils/testConfig";

Object.values(DB_CONFIG).forEach((config) => {
    describe(`Query Validation for ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;

        beforeAll(async () => {
            db = Database.create(config);
            await db.establishConnection();
        });

        afterAll(async () => {
            const closeResult = await db.closeConnection();
            expect(closeResult.success).toBe(true);
        });

        test('Valid query passes testQuery without errors', async () => {
            await expect(db.testQuery('SELECT 1 AS test;')).resolves.not.toThrow();
        });

        test('Invalid query throws error in testQuery', async () => {
            await expect(db.testQuery('INVALID SQL')).rejects.toThrow();
        });   
    });
});

describe(`Single Query Validation`, () => {
    test('Valid single query with trailing semicolon should pass', () => {
        expect(isValidSingleQuery("SELECT 1 AS TEST;")).toBe(true);
    });
    
    test('Valid single query without semicolon should pass', () => {
        expect(isValidSingleQuery("SELECT * FROM users")).toBe(true);
    });
    
    test('Valid single query with comment after semicolon should pass', () => {
        expect(isValidSingleQuery("SELECT * FROM users; -- This is a comment")).toBe(true);
    });
    
    test('Valid single query with multi-line comment should pass', () => {
        expect(isValidSingleQuery("SELECT * FROM users; /* This is a multi-line comment */")).toBe(true);
    });
    
    test('Multiple queries should fail', () => {
        expect(isValidSingleQuery("SELECT * FROM users; DELETE FROM users;")).toBe(false);
    });
    
    test('Multiple queries disguised with comments should fail', () => {
        expect(isValidSingleQuery("SELECT * FROM users; /* Comment */ DELETE FROM users;")).toBe(false);
    });
    
    test('Valid query with multi-line string containing semicolon should pass', () => {
        expect(isValidSingleQuery("SELECT 'Hello;\nWorld' AS greeting;")).toBe(true);
    });
    
    test('Valid query with semicolon inside single-quoted string should pass', () => {
        expect(isValidSingleQuery("SELECT 'this;is;a;test' AS testString;")).toBe(true);
    });
    
    test('Valid query with semicolon inside double-quoted string should pass', () => {
        expect(isValidSingleQuery('SELECT "semicolon;inside;double;quotes" AS test;')).toBe(true);
    });
    
    test('Valid query with JSON object containing semicolon should pass', () => {
        expect(isValidSingleQuery("SELECT '{\"key\": \"value;with;semicolons\"}' AS jsonData;")).toBe(true);
    });
    
    test('Multiple queries separated by semicolon should fail', () => {
        expect(isValidSingleQuery("SELECT * FROM users; DELETE FROM users;")).toBe(false);
    });
    
    test('Multiple queries with semicolon inside a valid string should fail', () => {
        expect(isValidSingleQuery("SELECT 'valid;string'; DELETE FROM users;")).toBe(false);
    });
    
    test('Multi-line SQL with single valid statement should pass', () => {
        expect(isValidSingleQuery(`
            SELECT name
            FROM users
            WHERE description = 'Line 1;
            Line 2; still inside string';
        `)).toBe(true);
    });
    
    test('Valid query with trailing semicolon should pass', () => {
        expect(isValidSingleQuery("SELECT 1 AS TEST;")).toBe(true);
    });
    
    test('Valid query with comment after semicolon should pass', () => {
        expect(isValidSingleQuery("SELECT * FROM users; -- This is a comment")).toBe(true);
    });
    
    test('Multiple queries disguised with comments should fail', () => {
        expect(isValidSingleQuery("SELECT * FROM users; /* Comment */ DELETE FROM users;")).toBe(false);
    });         
})