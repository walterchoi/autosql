import { isValidSingleQuery } from "../src/db/utils/validateQuery";

// A23: the single-statement guard is now a left-to-right tokenizer rather than layered regexes that
// stripped comments before strings (a comment straddling two string literals could hide a `; DROP …`)
// and mishandled doubled quotes and dollar-quoting.
describe("isValidSingleQuery tokenizer (A23)", () => {
    test("accepts a single statement (with or without trailing semicolon)", () => {
        expect(isValidSingleQuery("SELECT * FROM t")).toBe(true);
        expect(isValidSingleQuery("SELECT * FROM t;")).toBe(true);
        expect(isValidSingleQuery("UPDATE t SET a = 1 WHERE b = 2;")).toBe(true);
    });
    test("rejects genuinely multiple statements", () => {
        expect(isValidSingleQuery("SELECT 1; SELECT 2")).toBe(false);
        expect(isValidSingleQuery("SELECT 1; DROP TABLE users;")).toBe(false);
    });
    test("a semicolon inside a string literal does not count", () => {
        expect(isValidSingleQuery("SELECT 'a; b' AS x")).toBe(true);
    });
    test("doubled quotes inside a literal are handled (escapeLiteral output)", () => {
        expect(isValidSingleQuery("SELECT 'it''s; here' AS x")).toBe(true);
    });
    test("the comment-straddling-strings bypass is closed", () => {
        expect(isValidSingleQuery("SELECT '/*' ; DROP TABLE users ; SELECT '*/'")).toBe(false);
    });
    test("dollar-quoted bodies (Postgres) are skipped", () => {
        expect(isValidSingleQuery("SELECT $$ a; b; c $$ AS x")).toBe(true);
        expect(isValidSingleQuery("SELECT $tag$ a; b $tag$ AS x")).toBe(true);
    });
    test("semicolons inside comments don't count", () => {
        expect(isValidSingleQuery("SELECT 1 -- ; not a statement\nFROM t")).toBe(true);
        expect(isValidSingleQuery("SELECT 1 /* ; nope */ FROM t")).toBe(true);
    });
});
