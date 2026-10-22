import { AutoSQLHandler } from "../src/db/autosql";
import {
    buildResumableStagingTableName,
    isResumableStreamTable,
    isAutosqlStreamTable,
} from "../src/helpers/streamHelpers";

// Guards and naming for resumable (session-keyed) stream staging. No DB needed.

function makeMockDb(listedTables: string[], dropCalls: string[], stagingColumns: string[] = []) {
    return {
        runWithSchema: (_s: any, fn: any) => fn(),
        getConfig: () => ({
            sqlDialect: "mysql" as const,
            schema: "s",
            streamingStagingPrefix: "autosql_stream__",
            keepOrphanedStagingTables: false,
        }),
        updateSchema: jest.fn(),
        runQuery: jest.fn().mockImplementation((q: { query: string }) => {
            if (q.query === "SELECT 1") return Promise.resolve({ success: true, results: [{ 1: 1 }] });
            if (q.query.includes("information_schema.columns")) {
                return Promise.resolve({ success: true, results: stagingColumns.map((c) => ({ column_name: c })) });
            }
            if (q.query.includes("information_schema.tables")) {
                return Promise.resolve({ success: true, results: listedTables.map((t) => ({ table_name: t })) });
            }
            return Promise.resolve({ success: true, results: [] });
        }),
        runTransaction: jest.fn().mockImplementation((qs: { query: string }[]) => {
            dropCalls.push(qs.map((x) => x.query).join(" "));
            return Promise.resolve({ success: true });
        }),
        warn: jest.fn(),
        error: jest.fn(),
    } as any;
}

describe("resumable stream staging naming", () => {
    test("the staging name is a pure function of the session key, not of the call", () => {
        const a = buildResumableStagingTableName("users", "autosql_stream__", "conn-1");
        const b = buildResumableStagingTableName("users", "autosql_stream__", "conn-1");
        const c = buildResumableStagingTableName("users", "autosql_stream__", "conn-2");
        expect(a).toBe(b);       // same key on a later run resolves to the same table
        expect(a).not.toBe(c);   // different sessions never collide
    });

    test("the session key is hashed, so a caller-owned string can't shape the identifier", () => {
        const weird = buildResumableStagingTableName("users", "autosql_stream__", 'a"; DROP TABLE x; --');
        expect(weird).toMatch(/^autosql_stream__users__rs_[0-9a-f]{8}$/);
    });

    test("a resumable table is recognised as such, and NOT as a per-run orphan", () => {
        const name = buildResumableStagingTableName("users", "autosql_stream__", "conn-1");
        expect(isResumableStreamTable(name, "users", "autosql_stream__")).toBe(true);
        expect(isAutosqlStreamTable(name, "users", "autosql_stream__")).toBe(false);
        // and a per-run table is not mistaken for a resumable one
        expect(isResumableStreamTable("autosql_stream__users__deadbeef", "users", "autosql_stream__")).toBe(false);
    });
});

describe("resumable stream staging guards", () => {
    test("orphan cleanup skips resumable tables but still drops per-run orphans", async () => {
        const resumable = buildResumableStagingTableName("users", "autosql_stream__", "conn-1");
        const other = buildResumableStagingTableName("users", "autosql_stream__", "conn-2");
        const dropCalls: string[] = [];
        const db = makeMockDb([resumable, other, "autosql_stream__users__deadbeef"], dropCalls);
        const handler = new AutoSQLHandler(db);

        await handler._cleanupOrphanedStreamTables("users", "autosql_stream__");

        const dropped = dropCalls.join(" | ");
        expect(dropped).toContain("deadbeef");       // genuine crash leftover
        expect(dropped).not.toContain(resumable);    // the table a resume will adopt
        expect(dropped).not.toContain(other);        // ANOTHER session's in-progress load, same target
    });

    test("an empty sessionKey is rejected rather than shared between callers", async () => {
        const db = makeMockDb([], []);
        const handler = new AutoSQLHandler(db);
        await expect(handler.openStream("users", undefined, undefined, { sessionKey: "   " }))
            .rejects.toThrow(/non-empty/i);
    });

    test("adopted is false when no staging table exists yet, true when it does", async () => {
        const db1 = makeMockDb([], [], []); // information_schema.columns returns nothing
        const fresh: any = await new AutoSQLHandler(db1).openStream("users", undefined, undefined, { sessionKey: "k" });
        expect(fresh.adopted).toBe(false);
        expect(fresh.resumable).toBe(true);

        const db2 = makeMockDb([], [], ["id", "name"]);
        const adopted: any = await new AutoSQLHandler(db2).openStream("users", undefined, undefined, { sessionKey: "k" });
        expect(adopted.adopted).toBe(true);
        expect((adopted as any).columns).toEqual(["id", "name"]); // shape comes from the live table
    });

    test("a per-run stream is never resumable and never reports adopted", async () => {
        const db = makeMockDb([], []);
        const handle: any = await new AutoSQLHandler(db).openStream("users");
        expect(handle.resumable).toBe(false);
        expect(handle.adopted).toBe(false);
    });

    test("suspend() refuses on a per-run stream rather than leaking an unreachable table", async () => {
        const db = makeMockDb([], []);
        const handle: any = await new AutoSQLHandler(db).openStream("users");
        await expect(handle.suspend()).rejects.toThrow(/requires a stream opened with a sessionKey/i);
    });

    test("suspend() leaves the staging table in place and frees the active-set slot", async () => {
        const dropCalls: string[] = [];
        const db = makeMockDb([], dropCalls, ["id"]);
        const handler = new AutoSQLHandler(db);
        const handle: any = await handler.openStream("users", undefined, undefined, { sessionKey: "k" });

        await handle.suspend();

        expect(dropCalls.join(" ")).not.toMatch(/DROP TABLE/i); // nothing discarded
        // No longer held as a live stream, so a later cleanup pass isn't blocked by stale state.
        expect((handler as any).activeStreamStagingTables.size).toBe(0);
        // Spent: writing after suspend is a caller bug, not a silent no-op.
        await expect(handle.write([{ id: 1 }])).rejects.toThrow(/after end\(\)\/abort\(\)/i);
    });
});
