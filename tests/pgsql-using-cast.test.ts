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
