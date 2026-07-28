import { getDataHeaders } from "../src/helpers/metadata";
import { DatabaseConfig } from "../src/config/types";

// R12: a column that arrives with no data (all null) has no inferable type. AutoSQL defers it —
// it is excluded from inference (no type is guessed) and created, correctly typed, when a later
// batch first carries data. Guessing a type (e.g. varchar) would lock the column so later int/date
// data collates back to varchar. See decisions.md D-C.

describe("all-null column deferral (R12)", () => {
    test("an all-null column is excluded from inference (no guessed type), even with excludeBlankColumns:false", async () => {
        const cfg = { sqlDialect: "mysql", excludeBlankColumns: false } as DatabaseConfig;
        const meta = await getDataHeaders([{ id: 1000, notes: null }, { id: 1001, notes: null }], cfg);
        expect(meta.notes).toBeUndefined(); // deferred, not guessed as varchar
        expect(meta.id).toBeDefined();
    });

    test("a column with real data is typed and kept", async () => {
        const cfg = { sqlDialect: "mysql", excludeBlankColumns: false } as DatabaseConfig;
        const meta = await getDataHeaders([{ id: 1000, notes: 500 }, { id: 1001, notes: 600 }], cfg);
        expect(meta.notes).toBeDefined();
        expect(meta.notes.type).toBe("smallint"); // typed from the data, not varchar
    });
});
