import { buildHistoryQuery as buildHistoryQueryRaw } from "../src/db/queryBuilders/shared/insertBuilderShared";
import type { ResolvedHistory, HistoryHooks } from "../src/db/queryBuilders/shared/insertBuilderShared";
import type { MetadataHeader, QueryWithParams } from "../src/config/types";

// buildHistoryQuery's return type is QueryInput (string | QueryWithParams); it always actually
// returns the object form — narrow it once here instead of casting at every call site.
const buildHistoryQuery: (...args: Parameters<typeof buildHistoryQueryRaw>) => QueryWithParams =
    buildHistoryQueryRaw as any;

// buildHistoryQuery's own structural logic — composite-PK join, calculated columns excluded from
// the diff (but not from what's carried into the history row), and pkFilter's param/placeholder
// wiring — only ever exercised via history-incremental-live.test.ts's single-column, single-PK,
// never-NULL table. Hooks are stand-ins here (not a real dialect) so the assertions target
// buildHistoryQuery's own logic, not any one dialect's SQL flavor.

const hooks: HistoryHooks = {
    quote: (n) => `[${n}]`,
    colDiff: (a, b) => `DIFF(${a},${b})`,
    now: "NOW()",
    placeholder: (n) => `$${n}`,
};

function resolved(header: MetadataHeader): ResolvedHistory {
    return { table: "t", header };
}

describe("buildHistoryQuery", () => {
    test("a composite primary key joins on EVERY key column, ANDed", () => {
        const header: MetadataHeader = {
            a: { type: "int", primary: true },
            b: { type: "int", primary: true },
            val: { type: "varchar" },
        };
        const { query } = buildHistoryQuery(resolved(header), "", hooks);
        expect(query).toContain("ON t1.[a] = t2.[a] AND t1.[b] = t2.[b]");
    });

    test("a calculated column is excluded from the diff condition but still carried into the history row", () => {
        const header: MetadataHeader = {
            id: { type: "int", primary: true },
            val: { type: "varchar" },
            dwh_loaded_at: { type: "datetime", calculated: true },
        };
        const { query } = buildHistoryQuery(resolved(header), "", hooks);
        // Carried into both the INSERT column list and the SELECT.
        expect(query).toContain("[val], [dwh_loaded_at], [dwh_as_at]");
        expect(query).toContain("t1.[val], t1.[dwh_loaded_at], NOW()");
        // But NOT part of the diff — only `val` is compared.
        expect(query).toContain("WHERE DIFF(t1.[val],t2.[val]);");
        expect(query).not.toContain("dwh_loaded_at,t2");
    });

    test("no pkFilter: the WHERE clause is just the diff condition, no params", () => {
        const header: MetadataHeader = { id: { type: "int", primary: true }, val: { type: "varchar" } };
        const { query, params } = buildHistoryQuery(resolved(header), "", hooks);
        expect(query.trim().endsWith("WHERE DIFF(t1.[val],t2.[val]);")).toBe(true);
        expect(params).toEqual([]);
    });

    test("pkFilter (single key): adds an ANDed filter clause with a bound param", () => {
        const header: MetadataHeader = { id: { type: "int", primary: true }, val: { type: "varchar" } };
        const { query, params } = buildHistoryQuery(resolved(header), "", hooks, { id: 42 });
        expect(query).toContain("WHERE (DIFF(t1.[val],t2.[val])) AND t1.[id] = $1;");
        expect(params).toEqual([42]);
    });

    test("pkFilter (composite key): one placeholder per key column, in declared order", () => {
        const header: MetadataHeader = {
            a: { type: "int", primary: true },
            b: { type: "int", primary: true },
            val: { type: "varchar" },
        };
        const { query, params } = buildHistoryQuery(resolved(header), "", hooks, { a: 1, b: 2 });
        expect(query).toContain("t1.[a] = $1 AND t1.[b] = $2");
        expect(params).toEqual([1, 2]);
    });

    test("a pre-existing dwh_as_at in the header is excluded (it's the history table's own key, not the source's)", () => {
        const header: MetadataHeader = {
            id: { type: "int", primary: true },
            dwh_as_at: { type: "datetime" },
        };
        const { query } = buildHistoryQuery(resolved(header), "", hooks);
        // Only one dwh_as_at — the one buildHistoryQuery itself appends, not a doubled column list.
        expect(query.match(/dwh_as_at/g)?.length).toBe(1);
    });

    test("a schema prefix is applied to both the history table and the source/temp table references", () => {
        const header: MetadataHeader = { id: { type: "int", primary: true }, val: { type: "varchar" } };
        const { query } = buildHistoryQuery(resolved(header), "myschema.", hooks);
        expect(query).toContain("INSERT INTO myschema.[t__history]");
        expect(query).toContain("FROM myschema.[t] t1");
        expect(query).toContain("INNER JOIN myschema.[temp_staging__t] t2");
    });
});
