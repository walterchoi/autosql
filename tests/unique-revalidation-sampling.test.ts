// When sampling is enabled, unique/pseudounique flags are derived from the sample only.
// A column unique across the sample but duplicated in the unsampled remainder must be
// demoted, otherwise it gets a UNIQUE constraint (and possibly the primary key) that fails
// on insert of the full data. Shuffle is mocked to the identity so the sample is a
// deterministic prefix and the duplicate can be placed squarely in the remainder.
jest.mock("../src/helpers/utilities", () => {
    const actual = jest.requireActual("../src/helpers/utilities");
    return { ...actual, shuffleArray: (arr: any[]) => arr };
});

import { getDataHeaders } from "../src/helpers/metadata";
import { DatabaseConfig } from "../src/config/types";

// sampleSize = max(round(100 * 0.1), 10) = 10 → rows 0-9 sampled, 10-99 are the remainder.
const config = { sqlDialect: "mysql", sampling: 0.1, samplingMinimum: 10 } as DatabaseConfig;

describe("unique flag re-validation under sampling", () => {
    test("a column unique in the sample but duplicated in the remainder is demoted", async () => {
        const data = Array.from({ length: 100 }, (_, i) => ({ code: `c${i}` }));
        // Duplicate row 0's value in the remainder (index 50 ≥ sampleSize).
        data[50].code = "c0";

        const meta = await getDataHeaders(data, config);

        expect(meta.code.unique).not.toBe(true);
        // 99 distinct / 100 rows = 99% ≥ pseudoUnique threshold → still pseudounique.
        expect(meta.code.pseudounique).toBe(true);
    });

    test("a genuinely unique column stays unique after re-validation", async () => {
        const data = Array.from({ length: 100 }, (_, i) => ({ id: i + 1 }));
        const meta = await getDataHeaders(data, config);
        expect(meta.id.unique).toBe(true);
    });

    test("a duplicate confined to the sample is still caught (baseline)", async () => {
        const data = Array.from({ length: 100 }, (_, i) => ({ code: `c${i}` }));
        data[5].code = "c0"; // duplicate within the first 10 (the sample)
        const meta = await getDataHeaders(data, config);
        expect(meta.code.unique).not.toBe(true);
    });
});
