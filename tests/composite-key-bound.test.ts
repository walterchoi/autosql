import { predictIndexes } from "../src/helpers/keys";
import { MetadataHeader } from "../src/config/types";

// P4: the composite-primary-key search is bounded by maxCompositeKeyColumns. Without a cap it tries
// every subset of pseudo-unique columns (O(2^N)), each scanned over all rows — a real hang on a wide,
// key-less table. Composite keys within the cap are still found; the cap prevents the blow-up.

const clone = (m: MetadataHeader): MetadataHeader => JSON.parse(JSON.stringify(m));

// (order_id, product_id) is unique per row; neither column is unique alone.
const META: MetadataHeader = {
    order_id: { type: "int", pseudounique: true, allowNull: false, length: 11 },
    product_id: { type: "int", pseudounique: true, allowNull: false, length: 11 },
};
const DATA: Record<string, any>[] = [
    { order_id: 1, product_id: 101 },
    { order_id: 2, product_id: 102 },
    { order_id: 2, product_id: 104 }, // dup order_id
    { order_id: 3, product_id: 101 }, // dup product_id
    { order_id: 3, product_id: 105 },
];

describe("P4 — bounded composite-key search", () => {
    test("a 2-column composite key is found under the default cap (4)", () => {
        const r = predictIndexes(clone(META), undefined, undefined, DATA);
        expect(r.order_id.primary).toBe(true);
        expect(r.product_id.primary).toBe(true);
    });

    test("maxCompositeKeyColumns=1 prevents the 2-column search (key not auto-detected)", () => {
        const r = predictIndexes(clone(META), undefined, undefined, DATA, 1);
        expect(r.order_id.primary).toBeFalsy();
        expect(r.product_id.primary).toBeFalsy();
    });

    test("a wide, key-less dataset is handled without an O(2^N) blow-up", () => {
        // 20 pseudo-unique columns, no unique combination exists. Unbounded this is 2^20 subsets,
        // each scanned over all rows. Capped, it is bounded to combinations of <= 4 columns.
        const wide: MetadataHeader = {};
        for (let c = 0; c < 20; c++) wide[`c${c}`] = { type: "int", pseudounique: true, allowNull: false, length: 11 };
        const rows: Record<string, any>[] = [];
        for (let r = 0; r < 50; r++) {
            const row: Record<string, any> = {};
            for (let c = 0; c < 20; c++) row[`c${c}`] = r % 3; // no unique key exists
            rows.push(row);
        }
        const res = predictIndexes(wide, undefined, undefined, rows);
        // No unique key exists → nothing primary; and it must return (not hang).
        expect(Object.values(res).some((c: any) => c.primary)).toBe(false);
    });
});
