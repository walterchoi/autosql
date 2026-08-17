import { Database } from "../src/db/database";
import { InsertInput, MetadataHeader, AlterTableChanges } from "../src/config/types";

// R1 Slice 2, PR 2f: resolveConflicts's unique-constraint DROP (opt-in dropUniqueConstraints) runs
// AFTER the entry point released the load's schema lock, so — to stay serialized against a concurrent
// load that would drop the same constraint — it must RE-ACQUIRE the per-table lock around the DROP.
// A real concurrency test would be timing-flaky; spying on acquire/release deterministically proves the
// serialization (and that release runs in the finally even when the DROP throws).

const meta: MetadataHeader = {
    id: { type: "int", primary: true },
    code: { type: "varchar", unique: true, uniqueName: "uq_code" },
};
const noChanges = (): AlterTableChanges => ({
    addColumns: {}, modifyColumns: {}, dropColumns: [], renameColumns: [],
    nullableColumns: [], noLongerUnique: [], primaryKeyChanges: [],
});
// Metadata derivable to { uniques: { uq_code: ["code"] }, primary: ["id"] } so resolveConflicts skips
// live introspection and reaches the conflict-check → DROP path directly.
const input = (): InsertInput[] => [{
    table: "t", data: [{ id: 1, code: "x" }], previousMetaData: null, metaData: meta,
    comparedMetaData: { changes: noChanges(), updatedMetaData: meta },
} as InsertInput];

function setup(over: Record<string, any> = {}) {
    const db: any = Database.create({ sqlDialect: "pgsql", host: "h", user: "u", password: "p", database: "d", dropUniqueConstraints: true, schemaLockTimeout: 30, ...over });
    db.getConstraintConflictQuery = jest.fn().mockReturnValue({ query: "conflict", params: [] });
    db.getDropUniqueConstraintQuery = jest.fn().mockReturnValue({ query: "DROP", params: [] });
    // The conflict query reports a violating unique index (count > 0); the DROP succeeds.
    db.runTransactionsWithConcurrency = jest.fn(async (groups: any[]) =>
        groups.map((g: any) => (g?.[0]?.query === "conflict"
            ? { success: true, results: [{ uq_code: 1 }] }
            : { success: true })));
    db.acquireSchemaLock = jest.fn().mockResolvedValue(undefined);
    db.releaseSchemaLock = jest.fn().mockResolvedValue(undefined);
    db.warn = jest.fn();
    return { db, staging: db.autoSQLHandler.staging };
}

describe("resolveConflicts — serializes the unique-constraint DROP under the schema lock (2f)", () => {
    test("useSchemaLock:true → acquires + releases the table's schema lock around the DROP", async () => {
        const { db, staging } = setup({ useSchemaLock: true });
        await staging.resolveConflicts(input());
        expect(db.acquireSchemaLock).toHaveBeenCalledWith("t", 30);
        expect(db.releaseSchemaLock).toHaveBeenCalledWith("t");
        expect(db.getDropUniqueConstraintQuery).toHaveBeenCalledWith("t", "uq_code"); // the DROP still ran
    });

    test("useSchemaLock:false → no lock (nothing to serialize on); DROP still runs", async () => {
        const { db, staging } = setup({ useSchemaLock: false });
        await staging.resolveConflicts(input());
        expect(db.acquireSchemaLock).not.toHaveBeenCalled();
        expect(db.releaseSchemaLock).not.toHaveBeenCalled();
        expect(db.getDropUniqueConstraintQuery).toHaveBeenCalledWith("t", "uq_code");
    });

    test("useSchemaLock:true → releases the lock even if the DROP throws (finally)", async () => {
        const { db, staging } = setup({ useSchemaLock: true });
        db.runTransactionsWithConcurrency = jest.fn(async (groups: any[]) => {
            if (groups?.[0]?.[0]?.query === "conflict") return [{ success: true, results: [{ uq_code: 1 }] }];
            throw new Error("drop boom");
        });
        await expect(staging.resolveConflicts(input())).rejects.toThrow("drop boom");
        expect(db.acquireSchemaLock).toHaveBeenCalledWith("t", 30);
        expect(db.releaseSchemaLock).toHaveBeenCalledWith("t"); // finally ran despite the throw
    });
});
