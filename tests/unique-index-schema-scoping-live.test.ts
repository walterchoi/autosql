import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// 2.8.0 fix: PgDatabase.getUniqueIndexesQuery's `schema` param was accepted but silently dropped —
// `pg_class.relname` isn't schema-qualified, so without a pg_namespace join a lookup could match a
// same-named table's unique index in a DIFFERENT schema. MySQL's sibling already scopes by
// `table_schema`, so this regression is Postgres-only. Proven here with two schemas, each holding
// a same-named table + same-named unique constraint but on DIFFERENT columns, so cross-schema
// contamination is unmistakable (the wrong column name would show up).

const config = DB_CONFIG.pgsql;

(config ? describe : describe.skip)("getUniqueIndexesQuery is schema-scoped (live, Postgres)", () => {
    const qi = (n: string) => escapeIdentifier(n, "pgsql");
    const SCHEMA_A = "test_schema";
    const SCHEMA_B = "unique_index_scoping_other_schema";
    const TABLE = "unique_index_scoping_test";

    let dbA: Database; // schema: SCHEMA_A
    let dbB: Database; // schema: SCHEMA_B

    beforeAll(async () => {
        dbA = Database.create({ ...config, schema: SCHEMA_A, useWorkers: false });
        await dbA.establishConnection();
        await dbA.runQuery({ query: `CREATE SCHEMA IF NOT EXISTS ${qi(SCHEMA_B)}`, params: [] });

        dbB = Database.create({ ...config, schema: SCHEMA_B, useWorkers: false });
        await dbB.establishConnection();

        for (const [db, schema, col] of [[dbA, SCHEMA_A, "code_a"], [dbB, SCHEMA_B, "code_b"]] as const) {
            await db.runQuery({ query: `DROP TABLE IF EXISTS ${qi(schema)}.${qi(TABLE)}`, params: [] });
            await db.runQuery({
                query: `CREATE TABLE ${qi(schema)}.${qi(TABLE)} (${qi("id")} INT PRIMARY KEY, ${qi(col)} VARCHAR(20), `
                    + `CONSTRAINT ${qi("uq_shared_name")} UNIQUE (${qi(col)}))`,
                params: [],
            });
        }
    });

    afterAll(async () => {
        await dbA.runQuery({ query: `DROP TABLE IF EXISTS ${qi(SCHEMA_A)}.${qi(TABLE)}`, params: [] }).catch(() => {});
        await dbB.runQuery({ query: `DROP TABLE IF EXISTS ${qi(SCHEMA_B)}.${qi(TABLE)}`, params: [] }).catch(() => {});
        await dbA.runQuery({ query: `DROP SCHEMA IF EXISTS ${qi(SCHEMA_B)} CASCADE`, params: [] }).catch(() => {});
        await dbA.closeConnection();
        await dbB.closeConnection();
    });

    // Results also include each table's own PRIMARY KEY index (also "unique" in pg_index) — find
    // the shared-named constraint by name rather than assuming it's the only row.
    const find = (results: any[], name: string) => results.find((r) => String(r.index_name) === name);

    test("a schema-A lookup sees ONLY schema A's index, naming schema A's own column", async () => {
        const r = await dbA.runQuery(dbA.getUniqueIndexesQuery(TABLE));
        expect(r.success).toBe(true);
        expect(String(find(r.results!, "uq_shared_name")?.columns)).toBe("code_a");
    });

    test("a schema-B lookup sees ONLY schema B's index, naming schema B's own column", async () => {
        const r = await dbB.runQuery(dbB.getUniqueIndexesQuery(TABLE));
        expect(r.success).toBe(true);
        expect(String(find(r.results!, "uq_shared_name")?.columns)).toBe("code_b");
    });

    test("outlier: a column-name filter still stays scoped to its own schema", async () => {
        const r = await dbA.runQuery(dbA.getUniqueIndexesQuery(TABLE, "code_a"));
        expect(r.success).toBe(true);
        expect(r.results).toHaveLength(1);
        expect(String(r.results![0].columns)).toBe("code_a");

        // Asking schema A about schema B's own column name finds nothing — proves the join is
        // doing the scoping, not an accidental column-name coincidence.
        const none = await dbA.runQuery(dbA.getUniqueIndexesQuery(TABLE, "code_b"));
        expect(none.success).toBe(true);
        expect(none.results).toHaveLength(0);
    });
});
