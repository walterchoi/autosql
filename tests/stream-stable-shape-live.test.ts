import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// A18: a stream's staging columns are fixed at creation (from the first chunk). A key that first
// appears in a LATER row/chunk had no column to land in and was SILENTLY DROPPED (data loss). It must
// now fail loud, and the first chunk's columns come from the UNION of its rows (not just row[0]).

// Streaming is deferred on SQL Server (D-F).
Object.values(DB_CONFIG)
    .filter((config) => config.sqlDialect !== "sqlserver")
    .forEach((config) => {
        const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

        describe(`stream stable-shape enforcement (live) for ${config.sqlDialect.toUpperCase()}`, () => {
            const TABLE = "stream_stable_shape_test";
            const ref = `${qi("test_schema")}.${qi(TABLE)}`;
            const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
            let db: Database;

            beforeAll(async () => { db = Database.create({ ...config, schema: "test_schema", useWorkers: false }); await db.establishConnection(); });
            afterAll(async () => {
                for (const r of [tempRef, ref]) await db.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {});
                await db.closeConnection();
            });

            test("a later chunk introducing a new column fails loud (not a silent drop)", async () => {
                const handle: any = await db.openStream(TABLE);
                await handle.write([{ id: 1, a: "x" }]);
                await expect(handle.write([{ id: 2, a: "y", b: "z" }])).rejects.toThrow(/stable column set/i);
                await handle.abort();
            });

            test("the first chunk's columns are the UNION of its rows (a later row in chunk 1 isn't dropped)", async () => {
                const handle: any = await db.openStream(TABLE);
                // 'b' first appears in the 2nd row of the FIRST chunk — it must still become a column.
                await handle.write([{ id: 1, a: "x" }, { id: 2, a: "y", b: "z" }]);
                await handle.abort(); // don't need to merge; the write succeeding proves 'b' was accepted
            });
        });
    });
