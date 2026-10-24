import { buildMergeFromStreamQuery } from "../src/helpers/streamHelpers";
import { escapeIdentifier } from "../src/db/utils/escape";
import type { MetadataHeader, DatabaseConfig, supportedDialects } from "../src/config/types";

// Characterization test, written BEFORE collapsing mysqlCast/pgCast/sqlserverCast (identical
// control flow over different type lists and cast syntax) into one table-driven function
// (lint-and-dedup audit, Tier 2). Locks down the exact per-column CAST clause each dialect emits
// today for one representative column of each type family. insertType "INSERT" with no primary
// key keeps every dialect's query shape to a plain `INSERT ... SELECT <casts> FROM <staging>`, so
// the SELECT list can be asserted directly without the merge/upsert clauses around it.
describe("buildMergeFromStreamQuery — per-dialect CAST clauses (pre-dedup characterization)", () => {
    const q = (col: string, dialect: supportedDialects) => escapeIdentifier(col, dialect);

    function selectListOf(dialect: supportedDialects, metaData: MetadataHeader): string {
        const config: DatabaseConfig = { sqlDialect: dialect } as DatabaseConfig;
        const result = buildMergeFromStreamQuery("t", "staging_t", metaData, "INSERT", config);
        const query = typeof result === "string" ? result : result.query;
        const match = query.match(/SELECT (.+) FROM/);
        if (!match) throw new Error(`could not find SELECT list in: ${query}`);
        return match[1];
    }

    test("mysql: int/float/decimal/datetime/date/time/boolean/text", () => {
        const metaData: MetadataHeader = {
            a_int: { type: "int" },
            a_float: { type: "float" },
            a_decimal: { type: "decimal", length: 12, decimal: 4 },
            a_datetime: { type: "datetime" },
            a_date: { type: "date" },
            a_time: { type: "time" },
            a_bool: { type: "boolean" },
            a_text: { type: "varchar" },
        } as any;
        const d: supportedDialects = "mysql";
        expect(selectListOf(d, metaData)).toBe([
            `CAST(${q("a_int", d)} AS SIGNED)`,
            `CAST(${q("a_float", d)} AS DECIMAL)`,
            `CAST(${q("a_decimal", d)} AS DECIMAL(12,4))`,
            `CAST(${q("a_datetime", d)} AS DATETIME)`,
            `CAST(${q("a_date", d)} AS DATE)`,
            `CAST(${q("a_time", d)} AS TIME)`,
            `CAST(${q("a_bool", d)} AS SIGNED)`,
            q("a_text", d),
        ].join(", "));
    });

    test("pgsql: int/bigint/smallint/float/decimal/timestamp/date/time/boolean/text", () => {
        const metaData: MetadataHeader = {
            a_int: { type: "int" },
            a_bigint: { type: "bigint" },
            a_small: { type: "tinyint" },
            a_float: { type: "float" },
            a_decimal: { type: "decimal", length: 12, decimal: 4 },
            a_ts: { type: "datetime" },
            a_date: { type: "date" },
            a_time: { type: "time" },
            a_bool: { type: "boolean" },
            a_text: { type: "varchar" },
        } as any;
        const d: supportedDialects = "pgsql";
        expect(selectListOf(d, metaData)).toBe([
            `${q("a_int", d)}::INTEGER`,
            `${q("a_bigint", d)}::BIGINT`,
            `${q("a_small", d)}::SMALLINT`,
            `${q("a_float", d)}::FLOAT`,
            `${q("a_decimal", d)}::DECIMAL(12,4)`,
            `${q("a_ts", d)}::TIMESTAMP`,
            `${q("a_date", d)}::DATE`,
            `${q("a_time", d)}::TIME`,
            `${q("a_bool", d)}::BOOLEAN`,
            q("a_text", d),
        ].join(", "));
    });

    test("sqlserver: int/bigint/smallint(tinyint)/float/decimal/datetime2/date/time/boolean/text", () => {
        const metaData: MetadataHeader = {
            a_int: { type: "int" },
            a_bigint: { type: "bigint" },
            a_small: { type: "tinyint" },
            a_float: { type: "float" },
            a_decimal: { type: "decimal", length: 12, decimal: 4 },
            a_dt2: { type: "datetime" },
            a_date: { type: "date" },
            a_time: { type: "time" },
            a_bool: { type: "boolean" },
            a_text: { type: "varchar" },
        } as any;
        const d: supportedDialects = "sqlserver";
        const cast = (col: string, typ: string) => `CAST(NULLIF(${q(col, d)}, '') AS ${typ})`;
        expect(selectListOf(d, metaData)).toBe([
            cast("a_int", "INT"),
            cast("a_bigint", "BIGINT"),
            cast("a_small", "SMALLINT"),
            cast("a_float", "FLOAT"),
            cast("a_decimal", "DECIMAL(12,4)"),
            cast("a_dt2", "DATETIME2"),
            cast("a_date", "DATE"),
            cast("a_time", "TIME"),
            cast("a_bool", "BIT"),
            q("a_text", d),
        ].join(", "));
    });
});
