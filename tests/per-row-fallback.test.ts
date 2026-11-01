import { Database } from "../src/db/database";
import type { MetadataHeader } from "../src/config/types";

// Graceful degradation on the non-streaming direct-insert path (and the shared streaming flush):
// perRowInsertWithRetry re-inserts a failed batch one row at a time, then diverts rows that still
// fail to rejectedRowsTable (or throws when it isn't configured). White-box unit test — stub the
// DB I/O so no live database is needed. Most tests here use maxRetries=1 to keep the
// between-rounds widening pass (getMetaData/compareMetaData/configureTables) out of scope; the
// "widens, then succeeds on round 2" describe block below exercises that pass directly instead —
// every maxRetries:1 test elsewhere in the suite (streaming live tests included) deliberately
// skips it too, so this is its only coverage.
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

describe("perRowInsertWithRetry — widens between rounds, then succeeds (maxRetries > 1)", () => {
    // A column too short for round 1's failing row. Real getMetaData/compareMetaData run
    // unmocked, so a genuinely wider column is derived from the failing row's own value —
    // only configureTables (the worker-dispatched DDL) is stubbed.
    const WIDE_META: MetadataHeader = { id: { type: "int" }, name: { type: "varchar", length: 5 } };

    function makeWideningHandler() {
        const db: any = Database.create({ sqlDialect: "pgsql", host: "localhost", user: "u", password: "p", database: "d" });
        db.getInsertStatementQuery = (_table: string, rows: any[]) => ({ query: JSON.stringify(rows[0]), params: [] });
        let longNameAttempts = 0;
        db.runQuery = jest.fn(async (q: any) => {
            const [, name] = JSON.parse(q.query);
            if (String(name).length > 5) {
                longNameAttempts++;
                return longNameAttempts === 1
                    ? { success: false, error: "value too long for column 'name'" }
                    : { success: true, affectedRows: 1 };
            }
            return { success: true, affectedRows: 1 };
        });
        db.runTransaction = jest.fn(async () => ({ success: true }));
        const handler = db.autoSQLHandler as any;
        handler.configureTables = jest.fn().mockResolvedValue([{ success: true }]);
        return { db, handler };
    }

    test("round 1 fails on a too-short column, configureTables widens it, round 2 succeeds", async () => {
        const { db, handler } = makeWideningHandler();
        const rows = [{ id: 1, name: "ok" }, { id: 2, name: "a_very_long_name" }];

        const { inserted, rejected } = await handler.perRowInsertWithRetry("t", rows, WIDE_META, "INSERT", 2);

        expect(inserted).toBe(2); // both landed — id:1 on round 1, id:2 on round 2
        expect(rejected).toEqual([]);
        expect(db.runTransaction).not.toHaveBeenCalled(); // nothing diverted to rejectedRowsTable

        // configureTables was called exactly once, only for the row that actually failed, with the
        // ORIGINAL metaData preserved as previousMetaData (not clobbered) and a widened length.
        expect(handler.configureTables).toHaveBeenCalledTimes(1);
        const widenInput = handler.configureTables.mock.calls[0][0][0];
        expect(widenInput.data).toEqual([{ id: 2, name: "a_very_long_name" }]);
        expect(widenInput.previousMetaData).toBe(WIDE_META);
        expect(widenInput.metaData.name.length).toBeGreaterThan(5);
    });

    test("a schema-widening failure (configureTables rejects) doesn't abort the retry — round 2 still runs, and still fails since nothing actually widened", async () => {
        const db: any = Database.create({ sqlDialect: "pgsql", host: "localhost", user: "u", password: "p", database: "d" });
        db.getInsertStatementQuery = (_table: string, rows: any[]) => ({ query: JSON.stringify(rows[0]), params: [] });
        db.runQuery = jest.fn(async () => ({ success: false, error: "value too long for column 'name'" })); // always fails
        db.runTransaction = jest.fn(async () => ({ success: true }));
        const handler = db.autoSQLHandler as any;
        handler.configureTables = jest.fn().mockRejectedValue(new Error("ALTER TABLE denied"));

        const rows = [{ id: 1, name: "a_very_long_name" }];

        await expect(handler.perRowInsertWithRetry("t", rows, WIDE_META, "INSERT", 2))
            .rejects.toThrow(/failed to insert after 2 retry round\(s\)/);

        expect(handler.configureTables).toHaveBeenCalledTimes(1); // attempted once (round 1→2), rejection didn't abort the loop
        expect(db.runQuery).toHaveBeenCalledTimes(2); // both rounds actually attempted the insert
    });
});
