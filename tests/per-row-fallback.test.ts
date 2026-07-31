import { Database } from "../src/db/database";

// Graceful degradation on the non-streaming direct-insert path (and the shared streaming flush):
// perRowInsertWithRetry re-inserts a failed batch one row at a time, then diverts rows that still
// fail to rejectedRowsTable (or throws when it isn't configured). White-box unit test — stub the
// DB I/O so no live database is needed. maxRetries=1 keeps the between-rounds widening pass
// (getMetaData/compareMetaData/configureTables) out of scope; that path is covered by the
// streaming live tests.

function makeHandler(extraConfig: Record<string, any> = {}) {
    const db: any = Database.create({
        sqlDialect: "pgsql", host: "localhost", user: "u", password: "p", database: "d",
        ...extraConfig,
    });
    // Encode the row into the QueryInput so the stubbed runQuery can decide pass/fail per row.
    db.getInsertStatementQuery = (_table: string, rows: any[]) => ({ query: JSON.stringify(rows[0]), params: [] });
    db.runQuery = jest.fn(async (q: any) => {
        const row = JSON.parse(q.query);
        return row.bad ? { success: false, error: "boom" } : { success: true, affectedRows: 1 };
    });
    db.runTransaction = jest.fn(async () => ({ success: true }));
    return { db, handler: db.autoSQLHandler as any };
}

describe("perRowInsertWithRetry — non-streaming graceful degradation", () => {
    test("inserts good rows and diverts an unrecoverable row to rejectedRowsTable", async () => {
        const { db, handler } = makeHandler({ rejectedRowsTable: "rej" });
        const rows = [{ id: 1 }, { id: 2, bad: true }, { id: 3 }];

        const inserted = await handler.perRowInsertWithRetry("t", rows, {} as any, "INSERT", 1);

        expect(inserted).toBe(2);                          // the two good rows landed
        expect(db.runQuery).toHaveBeenCalledTimes(3);      // one attempt per row
        // rejected diversion = bootstrap table + insert-rejected = two runTransaction calls
        expect(db.runTransaction).toHaveBeenCalledTimes(2);
    });

    test("throws (does not silently drop) when a row is unrecoverable and rejectedRowsTable is unset", async () => {
        const { db, handler } = makeHandler(); // no rejectedRowsTable
        const rows = [{ id: 1 }, { id: 2, bad: true }];

        await expect(handler.perRowInsertWithRetry("t", rows, {} as any, "INSERT", 1))
            .rejects.toThrow(/failed to insert/i);
        expect(db.runTransaction).not.toHaveBeenCalled(); // nothing diverted
    });

    test("no diversion when every row succeeds", async () => {
        const { db, handler } = makeHandler({ rejectedRowsTable: "rej" });
        const rows = [{ id: 1 }, { id: 2 }];

        const inserted = await handler.perRowInsertWithRetry("t", rows, {} as any, "INSERT", 1);

        expect(inserted).toBe(2);
        expect(db.runTransaction).not.toHaveBeenCalled();
    });
});
