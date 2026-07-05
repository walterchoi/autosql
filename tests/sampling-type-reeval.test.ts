import { getDataHeaders } from "../src/helpers/metadata";
import { DatabaseConfig } from "../src/config/types";

// I2: with sampling enabled, the remaining-data pass must re-evaluate the type (not only the
// length). A wide value outside the sample must still widen the inferred type, otherwise it
// overflows/truncates on insert.

describe("sampling re-evaluates numeric type on remaining data (I2)", () => {
    test("a bigint-range value outside the sample widens the type from the sample's narrow int", async () => {
        const config = { sqlDialect: "mysql", sampling: 0.1, samplingMinimum: 10 } as DatabaseConfig;
        // 99 small values (fit a narrow int) + one bigint-range value. With 10% sampling the
        // wide value is almost always in the non-sampled remainder — the final type must still
        // be bigint regardless of which partition it lands in.
        const data = Array.from({ length: 100 }, (_, i) => ({ v: i === 0 ? 9999999999 : (i % 90) + 1 }));
        const meta = await getDataHeaders(data, config);
        expect(meta.v.type).toBe("bigint");
    });

    test("a decimal value outside the sample upgrades an integer column", async () => {
        const config = { sqlDialect: "mysql", sampling: 0.1, samplingMinimum: 10 } as DatabaseConfig;
        const data = Array.from({ length: 100 }, (_, i) => ({ n: i === 0 ? 3.14159 : (i % 90) + 1 }));
        const meta = await getDataHeaders(data, config);
        // decimal/int collate to decimal — the column must not stay a plain integer type
        expect(["decimal", "double", "exponent"]).toContain(meta.n.type);
    });
});
