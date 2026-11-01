import { getUsingClause } from "../src/db/queryBuilders/pgsql/alterTableTypeConversion";

// A12: the ALTER COLUMN ... USING cast interpolated the LOCAL inference type (double/exponent/tinyint/
// datetime/binary), which are not real Postgres types — the ALTER then failed at execution. The cast
// target must be translated to the Postgres server type (mirroring SET DATA TYPE).
describe("Postgres USING cast uses server type tokens, not local ones (A12)", () => {
    test("numeric conversions cast to real Postgres types", () => {
        // floating -> floating hits the plain numeric cast: `double` must become `double precision`.
        expect(getUsingClause("c", "exponent", "double")).toBe(`"c"::double precision`);
        expect(getUsingClause("c", "smallint", "bigint")).toBe(`"c"::bigint`); // unchanged token, valid
    });
    test("float -> integer rounds and casts to the server integer type", () => {
        expect(getUsingClause("c", "double", "tinyint")).toBe(`ROUND("c")::smallint`);
    });
    test("text -> numeric and the default cast translate the target type", () => {
        expect(getUsingClause("c", "varchar", "double")).toBe(`NULLIF("c", '')::double precision`);
        // binary source isn't text/numeric/date, so datetime falls to the default cast -> server type.
        expect(getUsingClause("c", "binary", "datetime")).toBe(`NULLIF("c", '')::timestamp without time zone`);
    });
});

describe("Postgres USING cast — boolean conversions (previously untested)", () => {
    test("boolean -> numeric: NULL-preserving CASE to 1/0", () => {
        expect(getUsingClause("c", "boolean", "int")).toBe(`CASE WHEN "c" IS NULL THEN NULL WHEN "c" THEN 1 ELSE 0 END`);
    });

    test("numeric -> boolean: CASE on = 1", () => {
        expect(getUsingClause("c", "int", "boolean")).toBe(`CASE WHEN "c" IS NULL THEN NULL WHEN "c" = 1 THEN TRUE ELSE FALSE END`);
    });

    test("boolean -> text: 'true'/'false' literals", () => {
        expect(getUsingClause("c", "boolean", "varchar")).toBe(`CASE WHEN "c" IS NULL THEN NULL WHEN "c" THEN 'true' ELSE 'false' END`);
    });

    test("text -> boolean: accepted literal sets, case/whitespace-insensitive", () => {
        const sql = getUsingClause("c", "varchar", "boolean");
        expect(sql).toContain(`IN ('true', 't', 'yes', 'y', '1', 'on') THEN TRUE`);
        expect(sql).toContain(`IN ('false', 'f', 'no', 'n', '0', 'off') THEN FALSE`);
        expect(sql).toContain("ELSE NULL");
    });
});

describe("Postgres USING cast — date/time conversions (previously untested)", () => {
    test("text -> datetime: TO_TIMESTAMP with the full format", () => {
        expect(getUsingClause("c", "varchar", "datetime")).toBe(`TO_TIMESTAMP(NULLIF("c", ''), 'YYYY-MM-DD HH24:MI:SS')`);
    });

    test("text -> datetimetz: TO_TIMESTAMP with a timezone format, distinct from plain datetime", () => {
        expect(getUsingClause("c", "varchar", "datetimetz")).toBe(`TO_TIMESTAMP(NULLIF("c", ''), 'YYYY-MM-DD HH24:MI:SS TZ')`);
    });

    test("text -> date: TO_DATE", () => {
        expect(getUsingClause("c", "varchar", "date")).toBe(`TO_DATE(NULLIF("c", ''), 'YYYY-MM-DD')`);
    });

    test("text -> time: TO_TIME", () => {
        expect(getUsingClause("c", "varchar", "time")).toBe(`TO_TIME(NULLIF("c", ''), 'HH24:MI:SS')`);
    });

    test("date -> text: TO_CHAR with the full datetime format", () => {
        expect(getUsingClause("c", "date", "varchar")).toBe(`TO_CHAR("c", 'YYYY-MM-DD HH24:MI:SS')`);
    });

    test("time -> text: TO_CHAR with the time-only format, distinct from date's", () => {
        expect(getUsingClause("c", "time", "varchar")).toBe(`TO_CHAR("c", 'HH24:MI:SS')`);
    });
});

describe("Postgres USING cast — JSON conversions (previously untested)", () => {
    test("json -> text: plain ::TEXT cast", () => {
        expect(getUsingClause("c", "json", "varchar")).toBe(`"c"::TEXT`);
    });

    test("text -> json: ::JSONB cast", () => {
        expect(getUsingClause("c", "varchar", "json")).toBe(`"c"::JSONB`);
    });
});
