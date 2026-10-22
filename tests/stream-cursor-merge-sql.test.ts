import { buildMergeFromStreamQuery } from "../src/helpers/streamHelpers";
import { DatabaseConfig, MetadataHeader } from "../src/config/types";

// The dedup wrapper is what makes re-staging a page safe, and it only works if it actually reaches
// the SQL. Assert on the generated statement so a regression shows up here rather than as duplicate
// rows in a live target.

const meta: MetadataHeader = {
    id: { type: "int", primary: true, allowNull: false },
    name: { type: "varchar", length: 50, primary: false, allowNull: true },
};

const sql = (q: any): string => (typeof q === "string" ? q : q.query);

const cfg = (d: "mysql" | "pgsql" | "sqlserver"): DatabaseConfig =>
    ({ sqlDialect: d, schema: "s" } as DatabaseConfig);

describe("stream merge dedup SQL", () => {
    (["mysql", "pgsql", "sqlserver"] as const).forEach((dialect) => {
        test(`${dialect}: dedup requested emits ROW_NUMBER partitioned by the primary key`, () => {
            const q = sql(buildMergeFromStreamQuery("t", "stg", meta, "UPDATE", cfg(dialect), true));
            expect(q).toMatch(/ROW_NUMBER\(\) OVER \(PARTITION BY/i);
            expect(q).toMatch(/_autosql_seq/);
            expect(q).toMatch(/_autosql_rn/);
        });

        test(`${dialect}: dedup not requested leaves the staging reference bare`, () => {
            const q = sql(buildMergeFromStreamQuery("t", "stg", meta, "UPDATE", cfg(dialect), false));
            expect(q).not.toMatch(/ROW_NUMBER/i);
        });
    });

    test("no primary key means no dedup wrapper, since there is nothing to partition by", () => {
        const keyless: MetadataHeader = { a: { type: "varchar", length: 10, primary: false, allowNull: true } };
        const q = sql(buildMergeFromStreamQuery("t", "stg", keyless, "UPDATE", cfg("pgsql"), true));
        expect(q).not.toMatch(/ROW_NUMBER/i);
    });
});
