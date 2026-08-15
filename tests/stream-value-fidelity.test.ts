import { buildInsertIntoStreamStagingQuery } from "../src/helpers/streamHelpers";
import { DatabaseConfig } from "../src/config/types";

// A18: the streaming staging insert used String(v), turning objects/arrays into "[object Object]" /
// "a,b,c" — a JSON source that round-trips via autoSQL corrupted via openStream. It now JSON-stringifies.
describe("stream staging insert serialises objects as JSON, not [object Object] (A18)", () => {
    const cfg = { sqlDialect: "pgsql", schema: "test_schema" } as DatabaseConfig;

    test("object and array values become JSON strings", () => {
        const q = buildInsertIntoStreamStagingQuery("t", ["id", "payload", "tags"],
            [{ id: 1, payload: { a: 1, b: [2, 3] }, tags: ["x", "y"] }], cfg) as { params: any[] };
        expect(q.params).toContain('{"a":1,"b":[2,3]}');
        expect(q.params).toContain('["x","y"]');
        expect(q.params.some((p) => String(p).includes("[object Object]"))).toBe(false);
    });

    test("null/undefined stay null; scalars stay strings", () => {
        const q = buildInsertIntoStreamStagingQuery("t", ["a", "b", "c"],
            [{ a: null, b: undefined, c: 42 }], cfg) as { params: any[] };
        expect(q.params).toEqual([null, null, "42"]);
    });
});
