import { splitInsertData } from "../src/helpers/utilities";
import type { DatabaseConfig } from "../src/config/types";

// Pure chunking by config.insertStack (default 100) — simple, but the boundary math
// (i += insertStack, slice(i, i+insertStack)) is exactly the kind of thing that silently breaks
// on an off-by-one edit. Zero coverage anywhere.

const rows = (n: number) => Array.from({ length: n }, (_, i) => ({ id: i }));
const cfg = (insertStack?: number): DatabaseConfig => ({ sqlDialect: "mysql", insertStack } as DatabaseConfig);

describe("splitInsertData", () => {
    test("data length exactly divisible by insertStack: no trailing empty chunk", () => {
        const chunks = splitInsertData(rows(6), cfg(3));
        expect(chunks).toHaveLength(2);
        expect(chunks[0]).toHaveLength(3);
        expect(chunks[1]).toHaveLength(3);
    });

    test("data length NOT divisible: the last chunk is smaller, not padded or dropped", () => {
        const chunks = splitInsertData(rows(7), cfg(3));
        expect(chunks).toHaveLength(3);
        expect(chunks.map((c) => c.length)).toEqual([3, 3, 1]);
    });

    test("insertStack larger than data.length: a single chunk holding everything", () => {
        const chunks = splitInsertData(rows(5), cfg(100));
        expect(chunks).toHaveLength(1);
        expect(chunks[0]).toHaveLength(5);
    });

    test("insertStack: 1 — every row is its own chunk", () => {
        const chunks = splitInsertData(rows(4), cfg(1));
        expect(chunks).toHaveLength(4);
        expect(chunks.every((c) => c.length === 1)).toBe(true);
    });

    test("an empty data array produces zero chunks, not one empty chunk", () => {
        const chunks = splitInsertData([], cfg(10));
        expect(chunks).toEqual([]);
    });

    test("no insertStack configured: defaults to 100", () => {
        const chunks = splitInsertData(rows(150), cfg(undefined));
        expect(chunks).toHaveLength(2);
        expect(chunks.map((c) => c.length)).toEqual([100, 50]);
    });

    test("chunks preserve row order and identity, not just count", () => {
        const data = rows(5);
        const chunks = splitInsertData(data, cfg(2));
        expect(chunks).toEqual([[{ id: 0 }, { id: 1 }], [{ id: 2 }, { id: 3 }], [{ id: 4 }]]);
    });
});
