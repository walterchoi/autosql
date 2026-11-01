import { RowStoreLoadStrategy } from "../src/db/loadStrategy";
import type { LoadContext } from "../src/db/loadStrategy";

// RowStoreLoadStrategy.load's `finally` drops staging even when a step above throws, "so a failed
// load leaves no orphaned temp table" (mirrors the streaming end() path). White-box unit test —
// no existing test ever forces a mid-load step to throw and then checks cleanup actually ran, or
// that a SECOND failure (the cleanup itself) doesn't swallow the original error.

function makeStrategy(overrides: { resolveConflictsThrows?: Error; removeStagingTablesThrows?: Error } = {}) {
    const dbError = jest.fn();
    const db: any = {
        getConfig: () => ({ useStagingInsert: true, addHistory: false }),
        error: dbError,
    };
    const removeStagingTables = overrides.removeStagingTablesThrows
        ? jest.fn().mockRejectedValue(overrides.removeStagingTablesThrows)
        : jest.fn().mockResolvedValue(undefined);
    const staging: any = {
        prepareStagingTables: jest.fn().mockResolvedValue(undefined),
        insertStagingTables: jest.fn().mockResolvedValue(undefined),
        resolveConflicts: overrides.resolveConflictsThrows
            ? jest.fn().mockRejectedValue(overrides.resolveConflictsThrows)
            : jest.fn().mockResolvedValue(undefined),
        insertFromStagingTables: jest.fn().mockResolvedValue([{ success: true }]),
        removeStagingTables,
    };
    const history: any = { insertHistory: jest.fn().mockResolvedValue(undefined) };
    const handler: any = {};
    const strategy = new RowStoreLoadStrategy(handler, db, staging, history);
    const ctx: LoadContext = { insertInput: [{ table: "t" } as any], table: "t", label: "autoSQL" };
    return { strategy, ctx, staging, dbError };
}

describe("RowStoreLoadStrategy.load — staging cleanup on failure", () => {
    test("a mid-load throw still drops staging, and the original error propagates", async () => {
        const boom = new Error("resolveConflicts boom");
        const { strategy, ctx, staging } = makeStrategy({ resolveConflictsThrows: boom });

        await expect(strategy.load(ctx)).rejects.toThrow("resolveConflicts boom");
        expect(staging.removeStagingTables).toHaveBeenCalledWith(ctx.insertInput);
    });

    test("a cleanup failure on top of a load failure does NOT swallow the original error", async () => {
        const loadError = new Error("resolveConflicts boom");
        const cleanupError = new Error("DROP TABLE denied");
        const { strategy, ctx, dbError } = makeStrategy({
            resolveConflictsThrows: loadError,
            removeStagingTablesThrows: cleanupError,
        });

        await expect(strategy.load(ctx)).rejects.toThrow("resolveConflicts boom"); // not "DROP TABLE denied"
        expect(dbError).toHaveBeenCalledWith(expect.stringContaining("failed to drop staging table(s) for 't'"));
        expect(dbError).toHaveBeenCalledWith(expect.stringContaining("DROP TABLE denied"));
    });

    test("cleanup still runs on the success path too, not only on failure", async () => {
        const { strategy, ctx, staging } = makeStrategy();
        const result = await strategy.load(ctx);
        expect(result).toEqual([{ success: true }]);
        expect(staging.removeStagingTables).toHaveBeenCalledWith(ctx.insertInput);
    });
});
