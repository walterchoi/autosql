import { isCombinationUnique, estimateRowSize } from "../src/helpers/utilities";
import { Database } from "../src/db/database";

// A24: isCombinationUnique built its key by joining on "|", which collided on an embedded "|", coerced
// null to "" and read 1 the same as "1". It now JSON-encodes the tuple.
describe("isCombinationUnique — unambiguous composite key (A24)", () => {
    test("an embedded delimiter no longer causes a false collision", () => {
        // Old '|' join: ["a|b","c"] and ["a","b|c"] both became "a|b|c".
        expect(isCombinationUnique([{ x: "a|b", y: "c" }, { x: "a", y: "b|c" }], ["x", "y"])).toBe(true);
    });
    test("null vs empty-string, and number vs numeric-string, are distinct", () => {
        expect(isCombinationUnique([{ x: null }, { x: "" }], ["x"])).toBe(true);
        expect(isCombinationUnique([{ x: 1 }, { x: "1" }], ["x"])).toBe(true);
    });
    test("a genuine duplicate is still detected", () => {
        expect(isCombinationUnique([{ x: "a", y: "b" }, { x: "a", y: "b" }], ["x", "y"])).toBe(false);
    });
});

// A25: estimateRowSize counted varchar by CHARACTERS; a char can be multi-byte (utf8mb4 up to 4, SQL
// Server NVARCHAR = 2), so the row-size limit under-counted and autoSplit under-triggered.
describe("estimateRowSize counts varchar bytes per dialect (A25)", () => {
    const meta: any = { c: { type: "varchar", length: 100 } };
    test("MySQL counts a varchar char as up to 4 bytes", () => {
        expect(estimateRowSize(meta, "mysql").rowSize).toBeGreaterThanOrEqual(400);
    });
    test("SQL Server counts NVARCHAR as 2 bytes/char (and MySQL is larger)", () => {
        expect(estimateRowSize(meta, "sqlserver").rowSize).toBeGreaterThanOrEqual(200);
        expect(estimateRowSize(meta, "mysql").rowSize).toBeGreaterThan(estimateRowSize(meta, "sqlserver").rowSize);
    });
});

// A20: openStream limped into Postgres-shaped SQL on SQL Server (streaming deferred, D-F). Fail loud.
describe("openStream fails fast on SQL Server (A20)", () => {
    test("rejects with a clear message, without connecting", async () => {
        const db: any = Database.create({ sqlDialect: "sqlserver", host: "h", user: "u", password: "p", database: "d" });
        await expect(db.openStream("t")).rejects.toThrow(/not yet supported on SQL Server/i);
    });
});
