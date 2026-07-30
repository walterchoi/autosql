import { predictType, collateTypes } from "../src/helpers/columnTypes";
import { getDataHeaders } from "../src/helpers/metadata";
import { DatabaseConfig } from "../src/config/types";

// Number-like fidelity guard. Native JS numbers lose leading zeros and round beyond ~15 digits, so
// number-like fields (phone numbers, zip codes, account IDs, high-precision values) must be fed as
// STRINGS to preserve them. This suite pins that contract — especially that the native-number
// inference fast path (predictType short-circuits `typeof value === "number"`) does NOT leak into or
// weaken the string path. If someone "optimises" the fast-path and breaks text fidelity, this fails.

const BASE: DatabaseConfig = { sqlDialect: "pgsql", autoIndexing: false };

describe("number-like fidelity — string inputs keep their exact representation", () => {
    test("leading-zero strings infer varchar (identifiers, not integers)", () => {
        for (const s of ["01", "007", "07030", "0123456789", "00000", "0412345678"]) {
            expect(predictType(s)).toBe("varchar");
        }
    });

    test("numeric strings beyond bigint infer varchar (precision preserved as text)", () => {
        expect(predictType("9223372036854775807")).toBe("bigint"); // fits
        expect(predictType("9223372036854775808")).toBe("varchar"); // one past bigint -> text, not rounded
        expect(predictType("12345678901234567890")).toBe("varchar"); // 20 digits -> text
    });

    test("phone-like strings with separators infer varchar", () => {
        expect(predictType("+61 400 000 000")).toBe("varchar");
        expect(predictType("(555) 123-4567")).toBe("varchar");
    });
});

describe("number-like fidelity — the fast path is native-only and doesn't change string handling", () => {
    // The SAME digits: as a native number they are a number; as a leading-zero string they are text.
    test("native number vs leading-zero string produce different, correct types", () => {
        expect(predictType(7030)).toBe("smallint");     // native number -> numeric
        expect(predictType("07030")).toBe("varchar");   // string identifier -> text (0 preserved)
    });

    test("a native integer and its plain string form agree (no fast-path divergence)", () => {
        for (const n of [0, 1, 42, 128, 32768, 2147483648]) {
            expect(predictType(n)).toBe(predictType(String(n)));
        }
    });

    test("collating a leading-zero string with an integer stays text (never coerced to a number)", () => {
        // e.g. a code column that is mostly "007"-style but has one bare "7" must not become integer.
        expect(collateTypes(["varchar", "tinyint"])).toBe("varchar");
    });
});

describe("deep-decimal fidelity — precision handling (D-G)", () => {
    test("by default a high-precision decimal is preserved (scale fits the data, up to the dialect max — no rounding, no warn)", async () => {
        const warnings: string[] = [];
        const cfg: DatabaseConfig = { ...BASE, logger: { warn: (m: string) => warnings.push(m) } };
        const data = [{ val: "3.14159265358979323846" }, { val: "2.5" }]; // 20 vs 1 fractional digits
        const r = await getDataHeaders(data, cfg);
        expect(r.val.type).toBe("decimal");
        expect(r.val.decimal).toBe(20);          // full scale kept (Postgres ceiling is 16383)
        expect(r.val.length).toBe(21);           // 1 integer digit + 20 fractional
        expect(warnings).toHaveLength(0);        // nothing rounded, so nothing to warn about
    });

    test("a deliberately low decimalMaxLength caps the scale and WARNS (rounding is observable, not silent)", async () => {
        const warnings: string[] = [];
        const cfg: DatabaseConfig = { ...BASE, decimalMaxLength: 6, logger: { warn: (m: string) => warnings.push(m) } };
        const r = await getDataHeaders([{ val: "3.14159265358979323846" }], cfg);
        expect(r.val.type).toBe("decimal");
        expect(r.val.decimal).toBe(6);
        expect(warnings.some((w) => /val/.test(w) && /ROUNDED/.test(w))).toBe(true);
    });

    test("decimalToVarchar stores the column as text when a value exceeds the cap (exact, no rounding)", async () => {
        const warnings: string[] = [];
        const cfg: DatabaseConfig = { ...BASE, decimalMaxLength: 6, decimalToVarchar: true, logger: { warn: (m: string) => warnings.push(m) } };
        const r = await getDataHeaders([{ val: "3.14159265358979323846" }], cfg);
        expect(r.val.type).toBe("varchar"); // promoted to text to preserve precision
        expect(warnings.some((w) => /val/.test(w) && /text/.test(w))).toBe(true);
    });

    test("forceStringColumns preserves a high-precision decimal exactly (as text)", async () => {
        const data = [{ val: "3.14159265358979323846" }];
        const r = await getDataHeaders(data, { ...BASE, forceStringColumns: ["val"] });
        expect(r.val.type).toBe("varchar"); // stored verbatim, no rounding
    });
});

describe("number-like fidelity — column-level inference", () => {
    test("a column of leading-zero zip strings infers varchar", async () => {
        const data = [{ zip: "07030" }, { zip: "10001" }, { zip: "02139" }];
        const r = await getDataHeaders(data, BASE);
        expect(r.zip.type).toBe("varchar");
    });

    test("forceStringColumns keeps a column text even when values arrive as native numbers", async () => {
        // Belt-and-suspenders: if upstream sends a zip as the number 7030 (already lossy), the caller
        // can still pin the column to text so it is at least stored/typed as a string going forward.
        const data = [{ zip: 7030 }, { zip: 10001 }];
        const r = await getDataHeaders(data, { ...BASE, forceStringColumns: ["zip"] });
        expect(r.zip.type).toBe("varchar");
    });
});
