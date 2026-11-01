import { getUsingClause } from "../src/db/queryBuilders/mysql/alterTableTypeConversion";

// getUsingClause generates the data-migration UPDATE statement ALTER COLUMN needs to run BEFORE
// the type change, for every old-type/new-type pair that needs real value transformation (not a
// plain cast). Zero coverage anywhere — a regression in any one of these 9 branches would
// silently produce the wrong migration SQL (not an error), corrupting data on the next ALTER.

describe("getUsingClause (MySQL)", () => {
    test("same type: returns just the quoted column name, not an UPDATE", () => {
        expect(getUsingClause("col", "int", "int", "t")).toBe("`col`");
    });

    test("boolean -> numeric: CASE WHEN ... THEN 1 ELSE 0", () => {
        const sql = getUsingClause("flag", "boolean", "int", "t")!;
        expect(sql).toContain("UPDATE `t` SET `flag` = CASE WHEN `flag` THEN 1 ELSE 0 END WHERE `flag` IS NOT NULL;");
    });

    test("numeric -> boolean: CASE WHEN ... = 1 THEN TRUE ELSE FALSE", () => {
        const sql = getUsingClause("flag", "int", "boolean", "t")!;
        expect(sql).toContain("CASE WHEN `flag` = 1 THEN TRUE ELSE FALSE END");
    });

    test("text -> boolean: recognizes the accepted literal sets for true/false, else NULL", () => {
        const sql = getUsingClause("flag", "varchar", "boolean", "t")!;
        expect(sql).toContain("IN ('true', 't', 'yes', 'y', '1', 'on') THEN TRUE");
        expect(sql).toContain("IN ('false', 'f', 'no', 'n', '0', 'off') THEN FALSE");
        expect(sql).toContain("ELSE NULL");
    });

    test("integer -> floating: multiplies by 1.0 (forces a real decimal, not a rounded cast)", () => {
        const sql = getUsingClause("amount", "int", "decimal", "t")!;
        expect(sql).toContain("SET `amount` = `amount` * 1.0 WHERE `amount` IS NOT NULL;");
    });

    test("floating -> integer: ROUND() rather than a truncating cast", () => {
        const sql = getUsingClause("amount", "decimal", "int", "t")!;
        expect(sql).toContain("SET `amount` = ROUND(`amount`) WHERE `amount` IS NOT NULL;");
    });

    test("text -> numeric: empty string becomes NULL via NULLIF, not a cast error", () => {
        const sql = getUsingClause("amount", "varchar", "int", "t")!;
        expect(sql).toContain("SET `amount` = NULLIF(`amount`, '') WHERE `amount` IS NOT NULL;");
    });

    test("text -> datetime: STR_TO_DATE with the full datetime format", () => {
        const sql = getUsingClause("ts", "varchar", "datetime", "t")!;
        expect(sql).toContain("STR_TO_DATE(`ts`, '%Y-%m-%d %H:%i:%s')");
    });

    test("text -> date: STR_TO_DATE with the date-only format", () => {
        const sql = getUsingClause("d", "varchar", "date", "t")!;
        expect(sql).toContain("STR_TO_DATE(`d`, '%Y-%m-%d')");
    });

    test("text -> time: STR_TO_DATE with the time-only format", () => {
        const sql = getUsingClause("tm", "varchar", "time", "t")!;
        expect(sql).toContain("STR_TO_DATE(`tm`, '%H:%i:%s')");
    });

    test("an unhandled old/new type pair falls through to null (plain cast, no data migration)", () => {
        expect(getUsingClause("x", "varchar", "json", "t")).toBeNull();
    });

    test("a schema is prefixed onto the table reference when provided", () => {
        const sql = getUsingClause("flag", "boolean", "int", "t", "my_schema")!;
        expect(sql).toContain("UPDATE `my_schema`.`t` SET");
    });

    test("outlier: an unsafe newType (not a plain identifier) throws instead of being interpolated", () => {
        expect(() => getUsingClause("flag", "boolean", "int; DROP TABLE t", "t")).toThrow(/Invalid SQL column type/);
    });
});
