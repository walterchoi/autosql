import { getDataHeaders } from "../src/helpers/metadata";
import { DatabaseConfig } from "../src/config/types";

// Decimal precision = max integer digits + max scale, taken as INDEPENDENT running maxes across rows.
// The old logic summed each row's integer length with the running scale, so when the widest-integer
// value and the widest-scale value were different rows it under-counted precision and the load
// overflowed (e.g. [10.5, 5.25] -> decimal(3,2), which cannot store 10.5). Affected all dialects.

const BASE: DatabaseConfig = { sqlDialect: "pgsql", autoIndexing: false };

describe("decimal precision inference", () => {
    test("widest-integer and widest-scale from different rows -> precision covers both", async () => {
        const r = await getDataHeaders([{ amount: 10.5 }, { amount: 20.0 }, { amount: 5.25 }], BASE);
        expect(r.amount.type).toBe("decimal");
        // 2 integer digits (10, 20) + 2 scale (5.25) => decimal(4, 2)
        expect(r.amount.length).toBe(4);
        expect(r.amount.decimal).toBe(2);
    });

    test("larger integer part widens precision", async () => {
        const r = await getDataHeaders([{ amount: 123.4 }, { amount: 5.25 }], BASE);
        // 3 integer digits (123) + 2 scale (5.25) => decimal(5, 2)
        expect(r.amount.length).toBe(5);
        expect(r.amount.decimal).toBe(2);
    });

    test("uniform-scale data is unchanged", async () => {
        const r = await getDataHeaders([{ amount: 1.25 }, { amount: 9.99 }], BASE);
        expect(r.amount.length).toBe(3); // 1 + 2
        expect(r.amount.decimal).toBe(2);
    });

    test("integer-only numeric keeps no scale", async () => {
        const r = await getDataHeaders([{ n: 100 }, { n: 5 }], BASE);
        expect(r.n.decimal).toBe(0);
    });
});
