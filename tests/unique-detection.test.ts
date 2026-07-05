import { getDataHeaders } from "../src/helpers/metadata";
import { DatabaseConfig } from "../src/config/types";

// I1: the uniqueSet cap was tied to the pseudounique ratio (ceil(0.9 * N)), so a truly-unique
// column saturated at ~0.9N rows and was mislabeled `pseudounique` — `unique` was effectively
// unreachable for any dense column of ~10+ rows.

const config = { sqlDialect: "mysql" } as DatabaseConfig;

describe("unique detection for dense columns", () => {
    test("a fully-unique column across 100 rows is labeled unique, not pseudounique", async () => {
        const data = Array.from({ length: 100 }, (_, i) => ({ id: i + 1, category: i % 5 }));
        const meta = await getDataHeaders(data, config);

        expect(meta.id.unique).toBe(true);
        expect(meta.id.pseudounique).not.toBe(true);

        // a low-cardinality column is not unique
        expect(meta.category.unique).not.toBe(true);
    });

    test("a 95%-unique column is pseudounique, not unique", async () => {
        // 100 rows, 5 duplicated values → 95 distinct
        const data = Array.from({ length: 100 }, (_, i) => ({ v: i < 95 ? i : 0 }));
        const meta = await getDataHeaders(data, config);

        expect(meta.v.unique).not.toBe(true);
        expect(meta.v.pseudounique).toBe(true);
    });
});
