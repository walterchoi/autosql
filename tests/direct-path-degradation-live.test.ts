import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Live end-to-end coverage for the non-streaming DIRECT-insert path's graceful degradation (the
// D-L follow-up / known gap). PR #39's unit tests STUB the DB; this exercises the real flow against
// live MySQL + Postgres:
//
//   useStagingInsert:false + a row the database rejects  →  the batch INSERT fails atomically and
//   rolls back  →  applyPerRowFallback re-inserts the batch one row at a time  →  the good rows land,
//   the rejected row still fails and is diverted to rejectedRowsTable (or, when rejectedRowsTable is
//   unset, the load fails loud and nothing is written).
//
// The trigger is a CHECK constraint (amount >= 0) on a pre-created table. It is the right kind of
// failure for this test because it is:
//   • upsert-proof — the direct path inserts with ON CONFLICT/ON DUPLICATE KEY UPDATE, and neither
//     bypasses a CHECK, so the bad row genuinely errors (a duplicate PK would be silently upserted;
//     a secondary-unique collision is upserted by MySQL's ON DUPLICATE KEY UPDATE);
//   • inference-proof — a negative amount is a valid INT, so schema inference/widening cannot "fix"
//     it away before the insert (a NOT NULL / length violation would be widened out by compareMetaData).
// streamMaxRetries:1 keeps it to a single per-row round (no schema-widening pass), so the bad row
// diverts deterministically.

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`direct-path per-row degradation (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "direct_degradation_test";
        const REJECTED = "direct_degradation_rejected";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const rejRef = `${qi("test_schema")}.${qi(REJECTED)}`;
        const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;

        // addTimestamps:false keeps the rows (and the diverted raw_data) to just id/amount/name.
        const baseConfig = {
            ...config,
            schema: "test_schema",
            useWorkers: false,
            addTimestamps: false,
            useStagingInsert: false,
            streamMaxRetries: 1,
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

        // Recreate the table (with the CHECK) and reseed before every sub-test, and drop the
        // accumulating rejected_rows table, so each test's exact-count assertions start clean.
        beforeEach(async () => {
            await dropAll();
            await admin.runQuery({
                query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("amount")} INT, ${qi("name")} VARCHAR(50), CONSTRAINT ${qi("amount_nonneg")} CHECK (${qi("amount")} >= 0))`,
                params: [],
            });
            await admin.runQuery({
                query: `INSERT INTO ${ref} (${qi("id")}, ${qi("amount")}, ${qi("name")}) VALUES (1, 10, 'seed')`,
                params: [],
            });
        });

        async function dropAll() {
            await admin.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await admin.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
            await admin.runQuery({ query: `DROP TABLE IF EXISTS ${rejRef}`, params: [] }).catch(() => {});
        }

        const rowCount = async (tableRef: string) => {
            const r = await admin.runQuery({ query: `SELECT COUNT(*) AS c FROM ${tableRef}`, params: [] });
            return Number(Object.values(r.results![0])[0]);
        };
        const nameOf = async (id: number) => {
            const r = await admin.runQuery({ query: `SELECT ${qi("name")} AS n FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] });
            return r.results!.length ? String(Object.values(r.results![0])[0]) : null;
        };

        test("a CHECK-violating row diverts to rejectedRowsTable while the good rows land", async () => {
            const db = Database.create({ ...baseConfig, rejectedRowsTable: REJECTED });
            await db.establishConnection();
            try {
                // Batch of two new rows: id=2 violates the amount CHECK; id=3 is valid. The batch
                // INSERT fails on id=2 and rolls back, then the per-row fallback re-inserts: id=3
                // lands, id=2 fails again and is diverted.
                const r = await db.autoSQL(TABLE, [{ id: 2, amount: -5, name: "bad" }, { id: 3, amount: 20, name: "good" }]);
                expect(r.success).toBe(true); // degraded gracefully — not a hard failure

                // The seed (id=1) and the one good row (id=3) are present; the bad id=2 never landed.
                expect(await rowCount(ref)).toBe(2);
                expect(await nameOf(1)).toBe("seed");
                expect(await nameOf(3)).toBe("good");
                expect(await nameOf(2)).toBeNull();

                // Exactly one row diverted, and it is the CHECK-violating id=2 row (raw_data preserved).
                expect(await rowCount(rejRef)).toBe(1);
                const rej = await admin.runQuery({ query: `SELECT ${qi("raw_data")} AS raw_data FROM ${rejRef}`, params: [] });
                const rawVal = Object.values(rej.results![0])[0];
                const parsed = typeof rawVal === "string" ? JSON.parse(rawVal) : rawVal;
                expect(Number(parsed.id)).toBe(2);
                expect(Number(parsed.amount)).toBe(-5);
            } finally {
                await db.closeConnection();
            }
        });

        test("without rejectedRowsTable the rejected batch fails loud and rolls back", async () => {
            const db = Database.create(baseConfig); // no rejectedRowsTable → fail-loud default
            await db.establishConnection();
            try {
                // The batch fails on id=2's CHECK violation; with no rejectedRowsTable the per-row
                // fallback never runs and autoSQL surfaces the failure.
                const r = await db.autoSQL(TABLE, [{ id: 2, amount: -5, name: "bad" }, { id: 3, amount: 20, name: "good" }]);
                expect(r.success).toBe(false);

                // The failed batch rolled back atomically and per-row never silently ran, so the good
                // id=3 row was NOT inserted — only the seed row remains.
                expect(await rowCount(ref)).toBe(1);
                expect(await nameOf(1)).toBe("seed");
                expect(await nameOf(3)).toBeNull();
            } finally {
                await db.closeConnection();
            }
        });
    });
});
