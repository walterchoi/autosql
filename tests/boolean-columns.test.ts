import { getDataHeaders } from "../src/helpers/metadata";
import { sqlize } from "../src/helpers/utilities";
import { mysqlConfig } from "../src/db/config/mysqlConfig";
import { pgsqlConfig } from "../src/db/config/pgsqlConfig";
import { DatabaseConfig } from "../src/config/types";

const BASE_CONFIG: DatabaseConfig = {
    sqlDialect: "mysql",
    autoIndexing: false,
};

// R3: a bare 0/1 is inferred as an integer, not a boolean. The `booleanColumns` hint is the
// explicit opt-in for flags stored as 0/1 (or true/false); out-of-domain values are rejected.

describe("R3 default inference — 0/1 is integer, not boolean", () => {
    test("a 0/1 column is inferred as a small integer without the hint", async () => {
        const data = [{ flag: 1 }, { flag: 0 }, { flag: 1 }];
        const result = await getDataHeaders(data, BASE_CONFIG);
        expect(result.flag.type).toBe("tinyint");
    });

    test("a true/false column is still inferred as boolean", async () => {
        const data = [{ active: true }, { active: false }, { active: true }];
        const result = await getDataHeaders(data, BASE_CONFIG);
        expect(result.active.type).toBe("boolean");
    });
});

describe("booleanColumns hint", () => {
    test("forces a 0/1 column to boolean", async () => {
        const data = [{ flag: 1 }, { flag: 0 }, { flag: 1 }];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, booleanColumns: ["flag"] });
        expect(result.flag.type).toBe("boolean");
    });

    test("forces a true/false column to boolean (and leaves it boolean)", async () => {
        const data = [{ flag: true }, { flag: false }];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, booleanColumns: ["flag"] });
        expect(result.flag.type).toBe("boolean");
    });

    test("accepts the string forms '0'/'1'/'true'/'false' case-insensitively", async () => {
        const data = [{ flag: "1" }, { flag: "0" }, { flag: "TRUE" }, { flag: "False" }];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, booleanColumns: ["flag"] });
        expect(result.flag.type).toBe("boolean");
    });

    test("tracks allowNull for a hinted column (nulls are allowed, not rejected)", async () => {
        const data = [{ flag: 1 }, { flag: null }, { flag: 0 }];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, booleanColumns: ["flag"] });
        expect(result.flag.type).toBe("boolean");
        expect(result.flag.allowNull).toBe(true);
    });

    test("rejects an out-of-domain numeric value with an error (never silently coerces)", async () => {
        const data = [{ flag: 1 }, { flag: 2 }];
        await expect(getDataHeaders(data, { ...BASE_CONFIG, booleanColumns: ["flag"] }))
            .rejects.toThrow(/booleanColumns/);
    });

    test("rejects an out-of-domain string value with an error", async () => {
        const data = [{ flag: "yes" }];
        await expect(getDataHeaders(data, { ...BASE_CONFIG, booleanColumns: ["flag"] }))
            .rejects.toThrow(/non-boolean value/);
    });

    test("only the hinted column is forced; siblings infer normally", async () => {
        const data = [{ flag: 1, count: 5 }, { flag: 0, count: 9 }];
        const result = await getDataHeaders(data, { ...BASE_CONFIG, booleanColumns: ["flag"] });
        expect(result.flag.type).toBe("boolean");
        expect(result.count.type).toBe("tinyint");
    });
});

// A boolean column stores a canonical 0/1. Without normalization a string flag ("true"/"false",
// as CSV sources deliver them) reaches the driver unchanged and MySQL rejects it against a
// TINYINT(1). This is not hint-specific — plain inference of literal true/false hits the same path.
describe("sqlize boolean normalization", () => {
    for (const [name, cfg] of [["mysql", mysqlConfig], ["pgsql", pgsqlConfig]] as const) {
        describe(name, () => {
            test("truthy forms normalize to '1'", () => {
                for (const v of [1, "1", true, "true", "TRUE", "t", "yes"]) {
                    expect(sqlize(v, "boolean", cfg)).toBe("1");
                }
            });
            test("falsy forms normalize to '0'", () => {
                for (const v of [0, "0", false, "false", "False", "f", "no"]) {
                    expect(sqlize(v, "boolean", cfg)).toBe("0");
                }
            });
            test("null and empty/absent stay null (a missing flag is not false)", () => {
                expect(sqlize(null, "boolean", cfg)).toBeNull();
                expect(sqlize("", "boolean", cfg)).toBeNull();
                expect(sqlize("  ", "boolean", cfg)).toBeNull();
                expect(sqlize("null", "boolean", cfg)).toBeNull();
            });
        });
    }
});
