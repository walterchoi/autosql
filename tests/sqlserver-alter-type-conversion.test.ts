import { getUsingExpression } from "../src/db/queryBuilders/sqlserver/alterTableTypeConversion";

// getUsingExpression returns the bare CAST/CASE T-SQL expression ALTER COLUMN splices into a
// two-step convert. 16 branches (more than mysql's getUsingClause sibling — SQL Server has no
// native boolean/JSON type, and distinguishes datetimetz and the CONVERT-style-code text
// conversions), zero coverage anywhere. A regression in any one silently produces the wrong
// expression (not an error) — generated SQL that quietly corrupts data on the next ALTER.

describe("getUsingExpression (SQL Server)", () => {
    test("same type: returns just the quoted column name", () => {
        expect(getUsingExpression("col", "int", "int")).toBe("[col]");
    });

    test("boolean -> numeric: CASE ... 1/0, NULL-preserving", () => {
        const sql = getUsingExpression("flag", "boolean", "int");
        expect(sql).toBe("CASE WHEN [flag] IS NULL THEN NULL WHEN [flag] = 1 THEN 1 ELSE 0 END");
    });

    test("numeric -> boolean: same CASE shape, triggered from the other direction", () => {
        const sql = getUsingExpression("flag", "int", "boolean");
        expect(sql).toBe("CASE WHEN [flag] IS NULL THEN NULL WHEN [flag] = 1 THEN 1 ELSE 0 END");
    });

    test("boolean -> text: 'true'/'false' literals, not 1/0", () => {
        const sql = getUsingExpression("flag", "boolean", "varchar");
        expect(sql).toContain("THEN 'true' ELSE 'false' END");
    });

    test("text -> boolean: accepted literal sets map to bit 1/0 (no boolean literal in T-SQL)", () => {
        const sql = getUsingExpression("flag", "varchar", "boolean");
        expect(sql).toContain("IN ('true', 't', 'yes', 'y', '1', 'on') THEN 1");
        expect(sql).toContain("IN ('false', 'f', 'no', 'n', '0', 'off') THEN 0");
        expect(sql).toContain("ELSE NULL");
    });

    test("integer -> floating: CAST AS DECIMAL", () => {
        expect(getUsingExpression("amount", "int", "decimal")).toBe("CAST([amount] AS DECIMAL)");
    });

    test("floating -> integer: ROUND then CAST AS INT, not a truncating cast", () => {
        expect(getUsingExpression("amount", "decimal", "int")).toBe("CAST(ROUND([amount], 0) AS INT)");
    });

    test("text -> numeric: empty string becomes NULL via NULLIF before casting", () => {
        expect(getUsingExpression("amount", "varchar", "int")).toBe("CAST(NULLIF([amount], '') AS DECIMAL)");
    });

    test("json -> text: NVARCHAR(MAX) (no native JSON type)", () => {
        expect(getUsingExpression("data", "json", "varchar")).toBe("CAST([data] AS NVARCHAR(MAX))");
    });

    test("text -> json: also NVARCHAR(MAX), same storage either direction", () => {
        expect(getUsingExpression("data", "varchar", "json")).toBe("CAST([data] AS NVARCHAR(MAX))");
    });

    test("text -> datetime: NULLIF then CAST AS DATETIME2", () => {
        expect(getUsingExpression("ts", "varchar", "datetime")).toBe("CAST(NULLIF([ts], '') AS DATETIME2)");
    });

    test("text -> datetimetz: CAST AS DATETIMEOFFSET, distinct from plain datetime", () => {
        expect(getUsingExpression("ts", "varchar", "datetimetz")).toBe("CAST(NULLIF([ts], '') AS DATETIMEOFFSET)");
    });

    test("text -> date: CAST AS DATE", () => {
        expect(getUsingExpression("d", "varchar", "date")).toBe("CAST(NULLIF([d], '') AS DATE)");
    });

    test("text -> time: CAST AS TIME", () => {
        expect(getUsingExpression("tm", "varchar", "time")).toBe("CAST(NULLIF([tm], '') AS TIME)");
    });

    test("date -> text: CONVERT with style code 120 (ODBC canonical)", () => {
        expect(getUsingExpression("d", "date", "varchar")).toBe("CONVERT(NVARCHAR(MAX), [d], 120)");
    });

    test("time -> text: CONVERT with style code 108 (hh:mm:ss), distinct from date's 120", () => {
        expect(getUsingExpression("tm", "time", "varchar")).toBe("CONVERT(NVARCHAR(MAX), [tm], 108)");
    });

    test("text -> an unmatched target type: falls to the NULLIF-guarded generic CAST", () => {
        // varchar -> text: both isText, no specific branch matches (not boolean/numeric/json/date/time).
        expect(getUsingExpression("x", "varchar", "text")).toBe("CAST(NULLIF([x], '') AS text)");
    });

    test("a non-text, unmatched pair: falls to the plain CAST with no NULLIF guard", () => {
        // int -> binary: neither numeric<->boolean, integer<->floating, text, date, nor time.
        expect(getUsingExpression("x", "int", "binary")).toBe("CAST([x] AS binary)");
    });

    test("outlier: an unsafe newType throws instead of being interpolated", () => {
        expect(() => getUsingExpression("x", "int", "binary; DROP TABLE t")).toThrow(/Invalid SQL column type/);
    });
});
