import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";
import { buildResumableStagingTableName } from "../src/helpers/streamHelpers";

// Cursor support on a resumable stream: the resume point lives IN the staging table, so it shares
// one lifetime with the rows it describes. The discriminating case is re-staging a page (which a
// resumed run does deliberately, for safety) and still merging each row exactly once.

Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect !== "sqlserver")
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

        describe(`stream cursor (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "stream_cursor_test";
            const SESSION = "cursor-conn-1";
            const stagingName = buildResumableStagingTableName(TABLE, "autosql_stream__", SESSION);
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            const stagingRef = `${qi("test_schema")}.${qi(stagingName)}`;

            const makeDb = () => Database.create({ ...config, schema: "test_schema", useWorkers: false });
            let db: Database;
            const cleanup = async () => {
                for (const r of [stagingRef, `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`, ref]) {
                    await db.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {});
                }
            };
            const cell = (r: any, k: string) => (k in r ? r[k] : r[k.toUpperCase()]);

            beforeAll(async () => { db = makeDb(); await db.establishConnection(); await cleanup(); });
            afterEach(cleanup);
            afterAll(async () => { await db.closeConnection(); });

            test("re-staging the page a run died on merges each row exactly once", async () => {
                // Run 1 lands pages 1 and 2, then dies part way through page 3.
                const first: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                await first.write([{ id: 1, name: "a" }], { cursor: "page-1" });
                await first.write([{ id: 2, name: "b" }], { cursor: "page-2" });
                await first.write([{ id: 3, name: "c-partial" }], { cursor: "page-3" });
                await first.suspend();

                // Run 2 reads the resume point off the staging table and re-sends page 3 wholesale.
                const second: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                expect(second.adopted).toBe(true);
                const state = await second.stagingState();
                expect(state.cursors).toEqual(["page-1", "page-2", "page-3"]);
                expect(state.lastCursor).toBe("page-3");
                expect(state.stagedRows).toBe(3);

                await second.write([{ id: 3, name: "c" }], { cursor: "page-3" }); // the re-run
                await second.write([{ id: 4, name: "d" }], { cursor: "page-4" });
                expect((await second.end()).success).toBe(true);

                // Four rows, not five: the duplicated id 3 collapsed, and the RE-RUN won.
                const rows = await db.runQuery({ query: `SELECT id, name FROM ${ref} ORDER BY id`, params: [] });
                expect(rows.results!.map((r: any) => Number(cell(r, "id")))).toEqual([1, 2, 3, 4]);
                expect(String(cell(rows.results![2], "name"))).toBe("c");
            });

            test("the target never gains an _autosql_ column", async () => {
                const h: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                await h.write([{ id: 1, name: "a" }], { cursor: "p1" });
                expect((await h.end()).success).toBe(true);

                const cols = await db.runQuery({
                    query: `SELECT column_name FROM information_schema.columns WHERE table_schema = ${config.sqlDialect === "mysql" ? "?" : "$1"} AND table_name = ${config.sqlDialect === "mysql" ? "?" : "$2"}`,
                    params: ["test_schema", TABLE],
                });
                const names = cols.results!.map((r: any) => String(cell(r, "column_name")).toLowerCase());
                expect(names).toContain("id");
                expect(names.filter((n: string) => n.startsWith("_autosql_"))).toEqual([]);
            });

            test("stagingState reads the table, so a brand-new handler sees another process's cursors", async () => {
                const first: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                await first.write([{ id: 1, name: "a" }], { cursor: "p1" });
                await first.write([{ id: 2, name: "b" }], { cursor: "p2" });
                await first.suspend();

                const db2 = makeDb();
                await db2.establishConnection();
                try {
                    const second: any = await db2.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                    const state = await second.stagingState();
                    expect(state.lastCursor).toBe("p2");
                    expect(state.stagedRows).toBe(2);
                    expect(state.firstStagedAt).toBeInstanceOf(Date);
                    expect(state.lastStagedAt).toBeInstanceOf(Date);
                    await second.abort();
                } finally {
                    await db2.closeConnection();
                }
            });

            test("a column using the reserved _autosql_ prefix is refused", async () => {
                const h: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
                await expect(h.write([{ id: 1, _autosql_cursor: "sneaky" }], { cursor: "p1" }))
                    .rejects.toThrow(/reserved/i);
                await h.abort();
            });

            test("a cursor without a primaryKey is refused, since duplicates could not be collapsed", async () => {
                const h: any = await db.openStream(TABLE, undefined, undefined, { sessionKey: SESSION });
                await expect(h.write([{ id: 1 }], { cursor: "p1" })).rejects.toThrow(/requires a primaryKey/i);
                await h.abort();
            });

            test("a cursor on a per-run stream is refused, since nothing could read it back", async () => {
                const h: any = await db.openStream(TABLE, undefined, ["id"]);
                await expect(h.write([{ id: 1 }], { cursor: "p1" })).rejects.toThrow(/sessionKey/i);
                await h.abort();
            });
        });
    });
