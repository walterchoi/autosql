import { getDataHeaders } from "../src/helpers/metadata";
import { DatabaseConfig } from "../src/config/types";

const BASE: DatabaseConfig = { sqlDialect: "pgsql", autoIndexing: false };

// A17: a column that first appears partway through the data had the earlier rows uncounted as nulls,
// so a SPARSE column (present in a few rows) looked NOT-NULL and fully-unique — a spurious primary-key
// candidate the very same batch then fails to insert. It must infer as nullable.
describe("sparse late-appearing columns infer as nullable (A17)", () => {
    test("a column present in 1 of 4 rows is nullable, not NOT-NULL", async () => {
        const data = [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4, code: "X" }];
        const r = await getDataHeaders(data, BASE);
        // Nullable is the fix that matters: a nullable column can't be chosen as the primary key, so
        // the fresh CREATE no longer emits a NOT-NULL column the same sparse batch fails to insert.
        expect(r.code.allowNull).toBe(true);   // was false before the fix (rows 1-3 uncounted)
    });

    test("a column present in every row is unaffected (still NOT-NULL, still unique-capable)", async () => {
        const data = [{ id: 1 }, { id: 2 }, { id: 3 }];
        const r = await getDataHeaders(data, BASE);
        expect(r.id.allowNull).toBe(false);
    });
});
