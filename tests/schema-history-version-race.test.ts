import { recordMigrationStart } from "../src/helpers/schemaHistory";

// recordMigrationStart computes version = MAX(version)+1 against a UNIQUE(table_name, version)
// constraint. Under concurrency without a schema lock, two migrations can collide on the same
// version; the loser must recompute and retry rather than fail.

function makeDb(insertResults: { success: boolean; error?: string; results?: any[] }[]) {
    let call = 0;
    return {
        getConfig: () => ({ sqlDialect: "mysql" as const, schema: "s" }),
        runQuery: jest.fn().mockResolvedValue({ success: true }), // sweepStalePending
        runTransaction: jest.fn().mockImplementation(() => Promise.resolve(insertResults[call++])),
    } as any;
}

describe("schema-history version-race handling", () => {
    test("retries on a unique-constraint collision and returns the eventual id", async () => {
        const db = makeDb([
            { success: false, error: "ER_DUP_ENTRY: Duplicate entry '3' for key 'uq_table_version'" },
            { success: true, results: [{ id: 7 }] },
        ]);
        const id = await recordMigrationStart(db, "t", {}, {});
        expect(id).toBe(7);
        expect((db.runTransaction as jest.Mock)).toHaveBeenCalledTimes(2);
    });

    test("does not retry on a non-collision failure", async () => {
        const db = makeDb([{ success: false, error: "ER_BAD_FIELD_ERROR: Unknown column" }]);
        const id = await recordMigrationStart(db, "t", {}, {});
        expect(id).toBeUndefined();
        expect((db.runTransaction as jest.Mock)).toHaveBeenCalledTimes(1);
    });
});
