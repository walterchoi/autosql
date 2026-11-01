import { HistoryCoordinator } from "../src/db/historyCoordinator";
import type { InsertInput } from "../src/config/types";

// buildHistoryInputs (private, reached via insertHistory/configureHistoryTables) throws
// "Could not find structure of {table} for history table creation" when fetchTableMetadata can't
// find the real table's current structure while building its history-table input — e.g. the table
// was never created, or vanished between configureTables and this step. White-box unit test — no
// existing test anywhere forces fetchTableMetadata to report a missing table at this point.

function makeCoordinator(opts: { tableExists: boolean }) {
    const db: any = { getConfig: () => ({ addHistory: true, historyTables: ["t"], useStagingInsert: true }) };
    const handler: any = {
        fetchTableMetadata: jest.fn(async (table: string) =>
            table === "t" && !opts.tableExists
                ? { currentMetaData: null, tableExists: false }
                : { currentMetaData: { id: { type: "int", primary: true } }, tableExists: true }
        ),
        configureTables: jest.fn().mockResolvedValue([{ success: true }]),
    };
    return { coordinator: new HistoryCoordinator(handler, db), handler };
}

const INPUT: InsertInput[] = [{ table: "t", data: [{ id: 1 }], metaData: { id: { type: "int", primary: true } }, previousMetaData: null, comparedMetaData: undefined }];

describe("HistoryCoordinator — missing table structure during history-input build", () => {
    test("insertHistory throws a clear error when the real table's structure can't be found", async () => {
        const { coordinator } = makeCoordinator({ tableExists: false });
        await expect(coordinator.insertHistory(INPUT)).rejects.toThrow(/Could not find structure of t for history table creation/);
    });

    test("configureHistoryTables (the atomic-path entry point) throws the same error", async () => {
        const { coordinator } = makeCoordinator({ tableExists: false });
        await expect(coordinator.configureHistoryTables(INPUT)).rejects.toThrow(/Could not find structure of t for history table creation/);
    });

    test("regression guard: a table that genuinely exists does not throw", async () => {
        const { coordinator, handler } = makeCoordinator({ tableExists: true });
        const result = await coordinator.configureHistoryTables(INPUT);
        expect(handler.configureTables).toHaveBeenCalled();
        expect(result).toHaveLength(1);
        expect(result[0].table).toBe("t__history");
    });
});
