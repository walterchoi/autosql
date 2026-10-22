import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";
import { buildResumableStagingTableName } from "../src/helpers/streamHelpers";

// Staging tables that survive BETWEEN runs (openStream `sessionKey`).
//
// Before this, staging identity was per-invocation by construction: openStream generated a run id,
// named the table from it, and *deleted* prior staging tables for that target first. So a retry
// destroyed the pages the failed attempt had already landed — the opposite of what resuming needs.
//
// The discriminating case is a SECOND Database/handler instance adopting the first one's rows: the
// in-memory `activeStreamStagingTables` set cannot help there, so anything that passes has to work
// off the table actually present in the database.

Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect !== "sqlserver")
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

        describe(`resumable stream staging (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "stream_resumable_test";
            const SESSION = "connection-42";          // stable across runs — NOT a run id
            const PREFIX = "autosql_stream__";
            const stagingName = buildResumableStagingTableName(TABLE, PREFIX, SESSION);

            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            const stagingRef = `${qi("test_schema")}.${qi(stagingName)}`;
            const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;

            const makeDb = () => Database.create({ ...config, schema: "test_schema", useWorkers: false });
            const cleanup = async (db: Database) => {
                for (const r of [stagingRef, tempRef, ref]) {
                    await db.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {});
                }
            };

            let db: Database;
            beforeAll(async () => { db = makeDb(); await db.establishConnection(); await cleanup(db); });
            afterEach(async () => { await cleanup(db); });
            afterAll(async () => { await db.closeConnection(); });

            test("a separate handler adopts a suspended session's rows and merges all of them", async () => {
                // --- Run 1: write two chunks, then stop WITHOUT promoting or discarding. ---
                const first: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                expect(first.resumable).toBe(true);
                expect(first.adopted).toBe(false); // nothing staged yet — caller must send from the start
                await first.write([{ id: 1, name: "a" }]);
                await first.write([{ id: 2, name: "b" }]);
                await first.suspend();

                // The staging table outlives the run that created it — that is the whole point.
                const survived = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${stagingRef}`, params: [] });
                expect(survived.success).toBe(true);
                expect(Number(survived.results![0].c ?? survived.results![0].C)).toBe(2);

                // --- Run 2: a genuinely NEW connection + handler, as a later Lambda invocation would be. ---
                const db2 = makeDb();
                await db2.establishConnection();
                try {
                    const second: any = await db2.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                    expect(second.adopted).toBe(true); // the resume point the caller stored is trustworthy
                    await second.write([{ id: 3, name: "c" }]);
                    const result = await second.end();
                    expect(result.success).toBe(true);
                } finally {
                    await db2.closeConnection();
                }

                // All three chunks landed — including the two written by the run that failed.
                const rows = await db.runQuery({ query: `SELECT id FROM ${ref} ORDER BY id`, params: [] });
                expect(rows.results!.map((r: any) => Number(r.id ?? r.ID))).toEqual([1, 2, 3]);

                // end() promoted, so staging is gone.
                const after = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${stagingRef}`, params: [] });
                expect(after.success).toBe(false);
            });

            test("opening the same target WITHOUT a session key does not destroy a resumable table", async () => {
                const keyed: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                await keyed.write([{ id: 10, name: "keep" }]);
                await keyed.suspend();

                // A plain (per-run) stream on the same target runs orphan cleanup on open. Cleanup is
                // keyed on the target table, so without the exclusion it would drop the table a resume
                // is about to adopt — and other session keys' tables into the same target too.
                const plain: any = await db.openStream(TABLE);
                await plain.abort();

                const still = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${stagingRef}`, params: [] });
                expect(still.success).toBe(true);
                expect(Number(still.results![0].c ?? still.results![0].C)).toBe(1);
            });

            test("a failed end() keeps a resumable table instead of destroying cross-run work", async () => {
                const handle: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                await handle.write([{ id: 1, name: "a" }]);

                // Force the flush to fail after staging is populated. Previously `finally` dropped the
                // staging table unconditionally, so a transient merge failure (lock timeout, connection
                // blip) threw away everything accumulated over earlier runs.
                const boom = new Error("simulated merge failure");
                jest.spyOn(db as any, "runTransaction").mockRejectedValueOnce(boom);
                const result = await handle.end();
                expect(result.success).toBe(false);
                jest.restoreAllMocks();

                const kept = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${stagingRef}`, params: [] });
                expect(kept.success).toBe(true);
                expect(Number(kept.results![0].c ?? kept.results![0].C)).toBe(1);

                // And the retry still sees the row, so no data was lost by the failure.
                const retry: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                expect(retry.adopted).toBe(true);
                await retry.abort();
            });

            test("adopting a table whose shape has drifted refuses loudly rather than writing mismatched columns", async () => {
                const first: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                await first.write([{ id: 1, name: "a" }]);
                await first.suspend();

                // The caller's data has gained a column since the staging table was created. Adopting
                // and writing anyway would silently drop `extra`; the stable-shape check must fire.
                const second: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                expect(second.adopted).toBe(true);
                await expect(second.write([{ id: 2, name: "b", extra: "new" }])).rejects.toThrow(/stable column set/i);
                await second.abort();
            });

            test("inference at end() sees every staged row, not just the resumed run's chunk", async () => {
                // `reference_autosql_shared_table_traps`: autosql infers from "the first batch", and the
                // first batch of a RESUMED run is not the first batch of the dataset. For the stream path
                // end() re-reads the whole staging table, so a value that only widens the type in an
                // earlier run's chunk must still be honoured.
                const first: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                await first.write([{ id: 1, name: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }]);
                await first.suspend();

                const second: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                await second.write([{ id: 2, name: "b" }]);
                expect((await second.end()).success).toBe(true);

                // The long value from run 1 survived — the column was sized from all staged rows.
                const rows = await db.runQuery({ query: `SELECT name FROM ${ref} WHERE id = 1`, params: [] });
                expect(String(rows.results![0].name ?? rows.results![0].NAME)).toHaveLength(60);
            });
        });
    });
