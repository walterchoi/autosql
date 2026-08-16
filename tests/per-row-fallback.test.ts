import { Database } from "../src/db/database";
import { MetadataHeader } from "../src/config/types";

// Graceful degradation on the non-streaming direct-insert path (and the shared streaming flush):
// perRowInsertWithRetry re-inserts a failed batch one row at a time, then diverts rows that still
// fail to rejectedRowsTable (or throws when it isn't configured). White-box unit test — stub the
// DB I/O so no live database is needed. maxRetries=1 keeps the between-rounds widening pass
// (getMetaData/compareMetaData/configureTables) out of scope; that path is covered by the
// streaming live tests.
//
// Note: perRowInsertWithRetry now PRE-SQLIZES each row (getInsertValues sqlizeValues=true) before
// getInsertStatementQuery — the same normalization the bulk path does — so the stub is given a real
// MetadataHeader and keys pass/fail off the sqlized value array it receives.

const META: MetadataHeader = { id: { type: "int" } };
const BAD_ID = "999"; // sqlized id that the stub rejects

function makeHandler(extraConfig: Record<string, any> = {}) {
    const db: any = Database.create({
        sqlDialect: "pgsql", host: "localhost", user: "u", password: "p", database: "d",
        ...extraConfig,
    });
    // rows[0] is the pre-sqlized value array (e.g. ["1"]); encode it so the stub can decide per row.
    db.getInsertStatementQuery = (_table: string, rows: any[]) => ({ query: JSON.stringify(rows[0]), params: [] });
    db.runQuery = jest.fn(async (q: any) => {
        const vals = JSON.parse(q.query);
        return String(vals[0]) === BAD_ID ? { success: false, error: "boom" } : { success: true, affectedRows: 1 };
    });
    db.runTransaction = jest.fn(async () => ({ success: true }));
    return { db, handler: db.autoSQLHandler as any };
}

describe("perRowInsertWithRetry — non-streaming graceful degradation", () => {
    test("inserts good rows and diverts an unrecoverable row to rejectedRowsTable", async () => {
        const { db, handler } = makeHandler({ rejectedRowsTable: "rej" });
        const rows = [{ id: 1 }, { id: 999 }, { id: 3 }];

        const { inserted, rejected } = await handler.perRowInsertWithRetry("t", rows, META, "INSERT", 1);

        expect(inserted).toBe(2);                  // the two good rows landed
        expect(rejected).toEqual([{ id: 999 }]);   // the unrecoverable row (raw) is surfaced
        expect(db.runQuery).toHaveBeenCalledTimes(3); // one attempt per row
        // rejected diversion = bootstrap table + insert-rejected = two runTransaction calls
        expect(db.runTransaction).toHaveBeenCalledTimes(2);
    });

    test("throws (does not silently drop) when a row is unrecoverable and rejectedRowsTable is unset", async () => {
        const { db, handler } = makeHandler(); // no rejectedRowsTable
        const rows = [{ id: 1 }, { id: 999 }];

        await expect(handler.perRowInsertWithRetry("t", rows, META, "INSERT", 1))
            .rejects.toThrow(/failed to insert/i);
        expect(db.runTransaction).not.toHaveBeenCalled(); // nothing diverted
    });

    test("no diversion when every row succeeds", async () => {
        const { db, handler } = makeHandler({ rejectedRowsTable: "rej" });
        const rows = [{ id: 1 }, { id: 2 }];

        const { inserted, rejected } = await handler.perRowInsertWithRetry("t", rows, META, "INSERT", 1);

        expect(inserted).toBe(2);
        expect(rejected).toEqual([]);
        expect(db.runTransaction).not.toHaveBeenCalled();
    });
});
