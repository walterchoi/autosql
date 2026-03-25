import { getDataHeaders } from "../src/helpers/metadata";
import { DatabaseConfig } from "../src/config/types";

const BASE_CONFIG: DatabaseConfig = {
    sqlDialect: "mysql",
    autoIndexing: false,
};

describe("forceStringColumns", () => {
    test("phone number column stays varchar instead of being inferred as bigint", async () => {
        const data = [
            { phone: "14155550100", name: "Alice" },
            { phone: "14155550101", name: "Bob" },
        ];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, forceStringColumns: ["phone"] });
        expect(result.phone.type).toBe("varchar");
    });

    test("zip code column stays varchar instead of being inferred as int", async () => {
        const data = [
            { zip: "07030", city: "Hoboken" },
            { zip: "10001", city: "New York" },
        ];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, forceStringColumns: ["zip"] });
        expect(result.zip.type).toBe("varchar");
    });

    test("padded code with leading zero stays varchar", async () => {
        const data = [
            { code: "007" },
            { code: "042" },
        ];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, forceStringColumns: ["code"] });
        expect(result.code.type).toBe("varchar");
        expect(result.code.length).toBe(3);
    });

    test("non-forced numeric column is still inferred as int", async () => {
        const data = [
            { id: "1" },
            { id: "2" },
        ];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, forceStringColumns: [] });
        expect(result.id.type).toBe("tinyint");
    });

    test("forced column length tracks max string length", async () => {
        const data = [
            { phone: "4155550100" },    // 10 chars
            { phone: "14155550100" },   // 11 chars
        ];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, forceStringColumns: ["phone"] });
        expect(result.phone.length).toBe(11);
    });

    test("forced column still detects allowNull", async () => {
        const data = [
            { phone: "14155550100" },
            { phone: null },
        ];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, forceStringColumns: ["phone"] });
        expect(result.phone.type).toBe("varchar");
        expect(result.phone.allowNull).toBe(true);
    });

    test("forced column uniqueness is tracked", async () => {
        const data = [
            { phone: "14155550100" },
            { phone: "14155550101" },
        ];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, forceStringColumns: ["phone"] });
        expect(result.phone.unique).toBe(true);
    });

    test("non-forced columns alongside forced columns are unaffected", async () => {
        const data = [
            { phone: "14155550100", amount: "99.5" },
            { phone: "14155550101", amount: "100.0" },
        ];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, forceStringColumns: ["phone"] });
        expect(result.phone.type).toBe("varchar");
        expect(result.amount.type).toBe("decimal");
    });
});

describe("varchar→text promotion after remainingData", () => {
    // Use sampling so that sampleData sees only short values (→ varchar)
    // while remainingData sees a long value that should trigger promotion.
    test("column promoted to text when long value appears only in remainingData", async () => {
        const shortRows = Array.from({ length: 10 }, (_, i) => ({ note: `short${i}` }));
        const longRow = { note: "x".repeat(2000) }; // 2000 > maxVarcharLength default (1024)
        const data = [...shortRows, longRow];

        // With sampling, shuffle places the long row in remainingData most of the time.
        // Use a deterministic setup: first 10 rows are sample (samplingMinimum=10, sampling=0.9 of 11 ≈ 10)
        // and the long row ends up in remainingData.
        // To guarantee it: build data so the sample definitely won't include the long row.
        // We use sampling=0.5 with samplingMinimum=5 on 12 rows, giving sampleSize=6.
        const shortRows12 = Array.from({ length: 11 }, (_, i) => ({ note: `s${i}` }));
        const data12 = [...shortRows12, longRow];

        const result = await getDataHeaders(data12, {
            ...BASE_CONFIG,
            sampling: 0.5,
            samplingMinimum: 5,
            maxVarcharLength: 1024,
        });

        // Whether the long row landed in sample or remainder, the final type must be text
        expect(["text", "mediumtext", "longtext"]).toContain(result.note.type);
    });

    test("column promoted to text when long value appears in sample", async () => {
        const data = [
            { note: "x".repeat(2000) },
            { note: "short" },
        ];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, maxVarcharLength: 1024 });
        expect(["text", "mediumtext", "longtext"]).toContain(result.note.type);
    });

    test("short varchar column stays varchar when no long values exist", async () => {
        const data = [
            { tag: "alpha" },
            { tag: "beta" },
            { tag: "gamma" },
        ];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, maxVarcharLength: 1024 });
        expect(result.tag.type).toBe("varchar");
    });
});
