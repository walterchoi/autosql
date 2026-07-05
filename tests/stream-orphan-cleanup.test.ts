import { AutoSQLHandler } from "../src/db/autosql";

// A concurrent stream to the same table produces a staging table matching the same
// `${prefix}${table}__` pattern the orphan cleanup scans for. Cleanup must not drop the
// staging table of a stream that is currently open on this instance.

function makeMockDb(listedTables: string[], dropCalls: string[]) {
    return {
        getConfig: () => ({
            sqlDialect: "mysql" as const,
            schema: "s",
            streamingStagingPrefix: "autosql_stream__",
            keepOrphanedStagingTables: false,
        }),
        updateSchema: jest.fn(),
        runQuery: jest.fn().mockImplementation((q: { query: string }) => {
            if (q.query === "SELECT 1") return Promise.resolve({ success: true, results: [{ 1: 1 }] });
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

describe("stream orphan cleanup", () => {
    test("does not drop the staging table of a live stream on the same instance", async () => {
        const listedTables: string[] = [];
        const dropCalls: string[] = [];
        const db = makeMockDb(listedTables, dropCalls);
        const handler = new AutoSQLHandler(db);

        // Open a stream — registers its staging table as active.
        const handle = await handler.openStream("users");
        const liveName = (handle as any).stagingTable as string;

        // Simulate the DB now listing both the live stream's table and a genuine orphan
        // (a leftover from a previous crashed run).
        listedTables.push(liveName, "autosql_stream__users__deadbeef");
        dropCalls.length = 0;

        await handler._cleanupOrphanedStreamTables("users", "autosql_stream__");

        const dropped = dropCalls.join(" | ");
        expect(dropped).toContain("deadbeef"); // genuine orphan is dropped
        expect(dropped).not.toContain(liveName); // live stream's staging is preserved

        // After the stream ends, its table is no longer protected.
        await handle.abort();
        dropCalls.length = 0;
        listedTables.length = 0;
        listedTables.push(liveName);
        await handler._cleanupOrphanedStreamTables("users", "autosql_stream__");
        expect(dropCalls.join(" | ")).toContain(liveName);
    });
});
