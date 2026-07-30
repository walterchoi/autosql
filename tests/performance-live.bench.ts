import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";
import { makeRows } from "./utils/fakeData";

// Opt-in live throughput benchmark (`npm run bench`). Absolute rows/s varies by machine and database,
// so the assertions are deliberately GENEROUS ceilings — they catch a catastrophic regression (e.g.
// losing multi-row batching and falling back to row-by-row inserts), not micro-changes. The logged
// rows/s figures are the useful signal for tracking trends across runs.

const N = 3_000;
const rows = makeRows(N);

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);
    const TABLE = "bench_load_test";
    const ref = `${qi(config.schema as string)}.${qi(TABLE)}`;

    describe(`bench: live load throughput — ${config.sqlDialect.toUpperCase()}`, () => {
        let db: Database;
        beforeAll(async () => {
            db = Database.create({ ...config, useWorkers: false });
            await db.establishConnection();
            await db.runQuery(db.dropTableQuery(TABLE)).catch(() => {});
        });
        afterAll(async () => {
            await db.runQuery(db.dropTableQuery(TABLE)).catch(() => {});
            await db.closeConnection();
        });

        const time = async (label: string, fn: () => Promise<void>) => {
            const t0 = process.hrtime.bigint();
            await fn();
            const ms = Number(process.hrtime.bigint() - t0) / 1e6;
            // eslint-disable-next-line no-console
            console.log(`  ⏱  ${config.sqlDialect} ${label}: ${ms.toFixed(0)}ms  (${Math.round((N / ms) * 1000).toLocaleString()} rows/s)`);
            return ms;
        };

        test(`create + insert ${N} rows`, async () => {
            const ms = await time("create+insert", async () => {
                const r = await db.autoSQL(TABLE, rows);
                expect(r.success).toBe(true);
            });
            const c = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
            expect(Number(Object.values(c.results![0])[0])).toBe(N);
            expect(ms).toBeLessThan(60_000); // generous catastrophic-regression ceiling
        });

        test(`re-ingest (upsert) ${N} rows`, async () => {
            const ms = await time("upsert", async () => {
                const r = await db.autoSQL(TABLE, rows);
                expect(r.success).toBe(true);
            });
            const c = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
            expect(Number(Object.values(c.results![0])[0])).toBe(N); // merged, not duplicated
            expect(ms).toBeLessThan(60_000);
        });
    });
});
