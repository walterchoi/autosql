import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// A6/A19 live proof: with schemaHistory + strictDriftDetection, re-ingesting the SAME data into an
// UNCHANGED table must NOT report drift. The bug: the stored baseline checksum was taken over the
// INFERRED metadata while the drift check compares the INTROSPECTED metadata — different shapes for
// the same physical table (esp. boolean↔tinyint, decimals, datetimes, managed dwh_* columns), so
// strict mode threw and BLOCKED every load after the first. Fix (A6): store the baseline from a
// post-migration RE-INTROSPECTION, so both sides are introspection-derived and match.
//
// The columns below are deliberately the divergence-prone ones. Two re-ingest cycles prove stability.
// A discriminating test then mutates the table out-of-band to prove real drift is still caught.

// schemaHistory is guarded off for SQL Server (D-F); exercise the dialects that support it.
Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect !== "sqlserver")
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

        describe(`schema-drift stability (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "drift_stability_test";
            const HISTORY = TABLE + "__history";
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            const histRef = `${qi("test_schema")}.${qi(HISTORY)}`;
            const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;

            const baseConfig = {
                ...config,
                schema: "test_schema",
                useWorkers: false,
                addTimestamps: true,            // stress the managed dwh_* columns too
                schemaHistory: true,
                strictDriftDetection: true,     // a false positive throws → load fails → test fails
            };

            const rows = [
                { id: 1, name: "alpha", amount: "1.50", ts: "2024-01-15 10:00:00", active: true },
                { id: 2, name: "beta", amount: "9.99", ts: "2024-07-20 18:30:00", active: false },
            ];

            let admin: Database;
            beforeAll(async () => { admin = Database.create(baseConfig); await admin.establishConnection(); });
            afterAll(async () => { await dropAll(); await admin.closeConnection(); });
            async function dropAll() {
                for (const r of [tempRef, histRef, ref]) {
                    await admin.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {});
                }
            }
            beforeEach(async () => { await dropAll(); });

            test("re-ingesting unchanged data twice does NOT false-positive drift", async () => {
                const db = Database.create(baseConfig);
                await db.establishConnection();
                try {
                    const first = await db.autoSQL(TABLE, rows);
                    expect(first.success).toBe(true); // creates table + records baseline checksum

                    // Two more identical loads — strict drift would throw on a false positive.
                    const second = await db.autoSQL(TABLE, rows);
                    expect(second.success).toBe(true);
                    const third = await db.autoSQL(TABLE, rows);
                    expect(third.success).toBe(true);
                } finally {
                    await db.closeConnection();
                }
            });

            test("discriminating: an out-of-band ALTER IS detected as drift (detection isn't vacuous)", async () => {
                const db = Database.create(baseConfig);
                await db.establishConnection();
                try {
                    const first = await db.autoSQL(TABLE, rows);
                    expect(first.success).toBe(true);

                    // Modify the table outside autosql, then re-ingest: strict drift must reject it.
                    await admin.runQuery({ query: `ALTER TABLE ${ref} ADD ${qi("sneaky")} INT`, params: [] });

                    const after = await db.autoSQL(TABLE, rows);
                    expect(after.success).toBe(false);
                    expect(String(after.error)).toMatch(/drift/i);
                } finally {
                    await db.closeConnection();
                }
            });
        });
    });
