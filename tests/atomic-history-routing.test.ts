import { Database } from "../src/db/database";
import { InsertInput, MetadataHeader } from "../src/config/types";

// R1 Slice 2, PR 2g (spec-1 §5.b): the plain addHistory case (row-level history WITHOUT the
// rejectedRowsTable degradation opt-in) used to run insertHistory and then the merge as TWO separate
// transactions — a crash between them recorded history for rows that never merged. It now takes the
// same zero-window atomic path as the degradation combo: before-image + merge in ONE transaction, so
// history and data commit — or roll back — together. SQL Server keeps the non-atomic path (its
// row-level history is unverified, D-F; configureHistoryTables guards the atomic path off).

const meta: MetadataHeader = { id: { type: "int", primary: true }, val: { type: "int" } };
const input = (): InsertInput[] => [
    { table: "t", data: [{ id: 1, val: 5 }], previousMetaData: meta, metaData: meta } as InsertInput,
];

// Stub the DB-touching staging/history steps so load() exercises only the routing decision.
function stubPipeline(db: any) {
    const h = db.autoSQLHandler;
    h.staging.prepareStagingTables = jest.fn().mockResolvedValue(undefined);
    h.staging.insertStagingTables = jest.fn().mockResolvedValue(undefined);
    h.staging.resolveConflicts = jest.fn().mockResolvedValue(undefined);
    h.staging.removeStagingTables = jest.fn().mockResolvedValue(undefined);
    h.staging.insertFromStagingTablesAtomic = jest.fn().mockResolvedValue([{ success: true }]);
    h.staging.insertFromStagingTables = jest.fn().mockResolvedValue([{ success: true }]);
    h.history.configureHistoryTables = jest.fn().mockResolvedValue([]);
    h.history.insertHistory = jest.fn().mockResolvedValue([]);
    return h;
}

describe("row-level history routing — plain addHistory now takes the atomic path (2g)", () => {
    test("pgsql plain addHistory (no rejectedRowsTable) → atomic path, all-or-nothing (perRowFallback:false)", async () => {
        const db: any = Database.create({ sqlDialect: "pgsql", host: "h", user: "u", password: "p", database: "d", useStagingInsert: true, addHistory: true, historyTables: ["t"] });
        const h = stubPipeline(db);
        const inp = input();
        await h.strategy.load({ insertInput: inp, table: "t", label: "test" });

        expect(h.history.configureHistoryTables).toHaveBeenCalledWith(inp);
        expect(h.staging.insertFromStagingTablesAtomic).toHaveBeenCalledWith(inp, [], { perRowFallback: false });
        // The old two-transaction plumbing must NOT run for this case.
        expect(h.history.insertHistory).not.toHaveBeenCalled();
        expect(h.staging.insertFromStagingTables).not.toHaveBeenCalled();
    });

    test("SQL Server addHistory → keeps the non-atomic insertHistory-then-merge path (atomic guarded off, D-F)", async () => {
        const db: any = Database.create({ sqlDialect: "sqlserver", host: "h", user: "u", password: "p", database: "d", schema: "s", useStagingInsert: true, addHistory: true, historyTables: ["t"] });
        const h = stubPipeline(db);
        const inp = input();
        await h.strategy.load({ insertInput: inp, table: "t", label: "test" });

        expect(h.history.insertHistory).toHaveBeenCalledWith(inp);
        expect(h.staging.insertFromStagingTables).toHaveBeenCalledWith(inp, { perRowFallback: false });
        // Never reaches the atomic path — configureHistoryTables would throw the SQL-Server guard.
        expect(h.history.configureHistoryTables).not.toHaveBeenCalled();
        expect(h.staging.insertFromStagingTablesAtomic).not.toHaveBeenCalled();
    });
});

describe("insertFromStagingTablesAtomic — one transaction for before-image + merge (2g)", () => {
    // Sentinel query objects: we only care that both land in the SAME transaction group.
    function setup(over: Record<string, any> = {}) {
        const db: any = Database.create({ sqlDialect: "pgsql", host: "h", user: "u", password: "p", database: "d", useStagingInsert: true, addHistory: true, historyTables: ["t"], ...over });
        db.getInsertChangedRowsToHistoryQuery = jest.fn().mockReturnValue({ query: "BEFORE_IMAGE", params: [] });
        db.getInsertFromStagingQuery = jest.fn().mockReturnValue({ query: "MERGE", params: [] });
        const staging = db.autoSQLHandler.staging;
        // "t__history" resolves (getTrueTableName) back to "t", so the before-image is added for "t".
        const historyInputs: InsertInput[] = [{ table: "t__history", data: [], metaData: meta, previousMetaData: meta } as InsertInput];
        return { db, staging, historyInputs };
    }

    test("packs [before-image, merge] into ONE transaction group (atomic by construction)", async () => {
        const { db, staging, historyInputs } = setup();
        let captured: any;
        db.runTransactionsWithConcurrency = jest.fn(async (groups: any[]) => { captured = groups; return groups.map(() => ({ success: true })); });
        await staging.insertFromStagingTablesAtomic(input(), historyInputs, { perRowFallback: false });
        expect(captured).toEqual([[{ query: "BEFORE_IMAGE", params: [] }, { query: "MERGE", params: [] }]]);
    });

    test("no rejectedRowsTable + merge fails → throws all-or-nothing, does NOT divert per-PK", async () => {
        const { db, staging, historyInputs } = setup();
        const divert = jest.spyOn(db.autoSQLHandler.degradation, "perPkAtomicStagingMerge");
        db.runTransactionsWithConcurrency = jest.fn(async (groups: any[]) => groups.map(() => ({ success: false, error: "merge boom" })));
        await expect(staging.insertFromStagingTablesAtomic(input(), historyInputs, { perRowFallback: false })).rejects.toThrow();
        expect(divert).not.toHaveBeenCalled();
    });

    test("rejectedRowsTable + perRowFallback + merge fails → diverts per-PK (does NOT throw)", async () => {
        const { db, staging, historyInputs } = setup({ rejectedRowsTable: "rej" });
        const divert = jest.spyOn(db.autoSQLHandler.degradation, "perPkAtomicStagingMerge").mockResolvedValue({ success: true } as any);
        db.runTransactionsWithConcurrency = jest.fn(async (groups: any[]) => groups.map(() => ({ success: false, error: "merge boom" })));
        const res = await staging.insertFromStagingTablesAtomic(input(), historyInputs, { perRowFallback: true });
        expect(divert).toHaveBeenCalledTimes(1);
        expect(res).toEqual([{ success: true }]);
    });
});
