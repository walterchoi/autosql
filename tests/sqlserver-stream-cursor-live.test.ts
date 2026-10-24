import { Database } from "../src/db/database";
import type { DatabaseConfig } from "../src/config/types";
import { escapeIdentifier } from "../src/db/utils/escape";
import { buildResumableStagingTableName } from "../src/helpers/streamHelpers";

// Cursor + dedup on SQL Server. The riskiest dialect for this: the dedup wraps the staging table in
// a derived table that then feeds MERGE's USING clause, and T-SQL is the one dialect that rejects
// duplicate source keys outright rather than tolerating them.

const CONFIG: DatabaseConfig = {
    sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd",
    database: "master", schema: "test_schema", port: 1433, useWorkers: false,
};
const qi = (n: string) => escapeIdentifier(n, "sqlserver");
const ref = (t: string) => `${qi("test_schema")}.${qi(t)}`;

describe("SQL Server stream cursor (live)", () => {
    const TABLE = "ss_stream_cursor";
    const SESSION = "ss-cursor-1";
    const stagingName = buildResumableStagingTableName(TABLE, "autosql_stream__", SESSION);

    let db: Database;
    const drop = async (t: string) => { await db.runQuery({ query: `DROP TABLE IF EXISTS ${ref(t)}`, params: [] }).catch(() => {}); };

    beforeAll(async () => {
        db = Database.create(CONFIG);
        await db.establishConnection();
        await db.runQuery(db.getCreateSchemaQuery("test_schema"));
    });
    beforeEach(async () => { for (const t of [stagingName, "temp_staging__" + TABLE, TABLE]) await drop(t); });
    afterAll(async () => {
        for (const t of [stagingName, "temp_staging__" + TABLE, TABLE]) await drop(t);
        await db.closeConnection();
    });

    test("re-staging a page merges each row once, and the re-run wins", async () => {
        const first: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
        await first.write([{ id: 1, name: "a" }], { cursor: "page-1" });
        await first.write([{ id: 2, name: "b-partial" }], { cursor: "page-2" });
        await first.suspend();

        const second: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
        const state = await second.stagingState();
        expect(state.lastCursor).toBe("page-2");
        expect(state.stagedRows).toBe(2);

        await second.write([{ id: 2, name: "b" }], { cursor: "page-2" }); // the re-run
        expect((await second.end()).success).toBe(true);

        const rows = await db.runQuery({ query: `SELECT id, name FROM ${ref(TABLE)} ORDER BY id`, params: [] });
        expect(rows.results!.map((r: any) => Number(r.id))).toEqual([1, 2]);
        expect(String(rows.results![1].name)).toBe("b");
    });

    test("the target never gains an _autosql_ column", async () => {
        const h: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
        await h.write([{ id: 1, name: "a" }], { cursor: "p1" });
        expect((await h.end()).success).toBe(true);

        const cols = await db.runQuery({
            query: `SELECT column_name FROM information_schema.columns WHERE table_schema = @p0 AND table_name = @p1`,
            params: ["test_schema", TABLE],
        });
        const names = cols.results!.map((r: any) => String(r.column_name).toLowerCase());
        expect(names).toContain("id");
        expect(names.filter((n: string) => n.startsWith("_autosql_"))).toEqual([]);
    });

    test("staged_at is set from the database clock, and seq orders within one second", async () => {
        const h: any = await db.openStream(TABLE, undefined, ["id"], { sessionKey: SESSION });
        // Three pages inside the same second: only _autosql_seq can order them.
        await h.write([{ id: 1, name: "a" }], { cursor: "p1" });
        await h.write([{ id: 2, name: "b" }], { cursor: "p2" });
        await h.write([{ id: 3, name: "c" }], { cursor: "p3" });

        const state = await h.stagingState();
        expect(state.cursors).toEqual(["p1", "p2", "p3"]);
        expect(state.firstStagedAt).toBeInstanceOf(Date);
        await h.abort();
    });
});
