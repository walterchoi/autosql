import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// A2 live proof: row-level history must capture a before-image ONLY for rows the batch actually
// changes — not for every row already in the table. The bug: the before-image query LEFT JOINed the
// full target table against the staged batch with only a column-diff filter, so on an incremental
// load every table row NOT in the batch (t2 all NULL → `col <=> NULL` TRUE) was recorded as a
// "change" — unbounded history growth proportional to table size, not batch size. Fix: INNER JOIN.
//
// This test was impossible to fail before only because every prior history test staged the WHOLE
// table; here we seed 3 rows and stage 1, so the buggy code would write 3 history rows and the fixed
// code writes exactly 1.

// SQL Server history is deferred (D-F); exercise the dialects that support it.
Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect !== "sqlserver")
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

        describe(`incremental row-level history (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "history_incremental_test";
            const HISTORY = TABLE + "__history";
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            const histRef = `${qi("test_schema")}.${qi(HISTORY)}`;
            const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;

            const baseConfig = {
                ...config,
                schema: "test_schema",
                useWorkers: false,
                addTimestamps: false,
                addHistory: true,
                historyTables: [TABLE],
            };

            let admin: Database;

            beforeAll(async () => {
                admin = Database.create(baseConfig);
                await admin.establishConnection();
            });
            afterAll(async () => {
                await dropAll();
                await admin.closeConnection();
            });

            async function dropAll() {
                for (const r of [tempRef, histRef, ref]) {
                    await admin.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {});
                }
            }
            const count = async (tableRef: string) => {
                const r = await admin.runQuery({ query: `SELECT COUNT(*) AS c FROM ${tableRef}`, params: [] });
                return Number(Object.values(r.results![0])[0]);
            };
            const valOf = async (id: number) => {
                const r = await admin.runQuery({ query: `SELECT ${qi("val")} AS v FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
                return r.results!.length ? Number(Object.values(r.results![0])[0]) : null;
            };

            beforeEach(async () => {
                await dropAll();
            });

            test("updating 1 of 3 rows writes exactly ONE before-image (not one per table row)", async () => {
                const db = Database.create(baseConfig);
                await db.establishConnection();
                try {
                    // Seed: create the table + 3 rows. First load has no pre-existing rows, so no history.
                    const seed = await db.autoSQL(TABLE, [
                        { id: 1, val: 10 },
                        { id: 2, val: 20 },
                        { id: 3, val: 30 },
                    ]);
                    expect(seed.success).toBe(true);

                    // Clear history to get a deterministic baseline regardless of seed behaviour.
                    await admin.runQuery({ query: `DELETE FROM ${histRef}`, params: [] });
                    expect(await count(histRef)).toBe(0);

                    // Incremental batch: change ONLY id=2 (20 → 25). ids 1 and 3 are untouched and are
                    // NOT in this batch — they must not be historised.
                    const inc = await db.autoSQL(TABLE, [{ id: 2, val: 25 }]);
                    expect(inc.success).toBe(true);

                    expect(await valOf(2)).toBe(25); // the update applied

                    // The teeth: exactly one before-image (id=2, old val 20). The LEFT JOIN bug wrote 3.
                    expect(await count(histRef)).toBe(1);
                    const hist = await admin.runQuery({
                        query: `SELECT ${qi("id")} AS id, ${qi("val")} AS val FROM ${histRef}`,
                        params: [],
                    });
                    expect(Number((hist.results![0] as any).id)).toBe(2);
                    expect(Number((hist.results![0] as any).val)).toBe(20); // the BEFORE value
                } finally {
                    await db.closeConnection();
                }
            });

            test("re-loading an unchanged batch writes NO before-image", async () => {
                const db = Database.create(baseConfig);
                await db.establishConnection();
                try {
                    const seed = await db.autoSQL(TABLE, [{ id: 1, val: 10 }, { id: 2, val: 20 }]);
                    expect(seed.success).toBe(true);
                    await admin.runQuery({ query: `DELETE FROM ${histRef}`, params: [] });

                    // Same values → nothing changes → no before-image (INNER JOIN matches, diff is empty).
                    const again = await db.autoSQL(TABLE, [{ id: 1, val: 10 }, { id: 2, val: 20 }]);
                    expect(again.success).toBe(true);
                    expect(await count(histRef)).toBe(0);
                } finally {
                    await db.closeConnection();
                }
            });
        });
    });
