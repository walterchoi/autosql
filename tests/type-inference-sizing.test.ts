import { predictIndexes } from "../src/helpers/keys";
import { predictType } from "../src/helpers/columnTypes";
import { getDataHeaders } from "../src/helpers/metadata";
import { MetadataHeader, DatabaseConfig } from "../src/config/types";

const BASE_CONFIG: DatabaseConfig = { sqlDialect: "mysql", autoIndexing: false };

describe("id-like primary key preference is anchored", () => {
    test("an ordinary word ending in 'id' is not preferred over a real _id column", () => {
        const meta: MetadataHeader = {
            amount_paid: { type: "varchar", length: 10, unique: true, allowNull: false },
            customer_id: { type: "int", length: 4, unique: true, allowNull: false },
        };
        const result = predictIndexes(meta);
        expect(result.customer_id.primary).toBe(true);
        expect(result.amount_paid.primary).toBeUndefined();
    });
});

describe("configurable number-format separators", () => {
    test("a lone separator defaults to decimal, but an explicit thousands separator overrides it", () => {
        expect(predictType("1.000")).toBe("decimal"); // auto-heuristic: '.' is the decimal point
        // With '.' declared as the thousands separator, "1.000" is the integer 1000.
        expect(predictType("1.000", ".", ",")).toBe("smallint");
    });

    test("getDataHeaders honors the configured separators", async () => {
        const data = [{ amount: "1.000" }, { amount: "2.000" }];
        const result = await getDataHeaders(data, {
            ...BASE_CONFIG,
            thousandsSeparator: ".",
            decimalSeparator: ",",
        });
        expect(["tinyint", "smallint", "int"]).toContain(result.amount.type);
    });
});

describe("explicit primary key on a long column is honored", () => {
    test("a primaryKey longer than maxKeyLength still gets primary=true", () => {
        const meta: MetadataHeader = {
            long_code: { type: "varchar", length: 600, unique: true, allowNull: false },
            other: { type: "int", length: 4 },
        };
        // maxKeyLength = 255; long_code (600) would previously be skipped before the
        // explicit-primary-key handling and silently dropped.
        const result = predictIndexes(meta, 255, ["long_code"]);
        expect(result.long_code.primary).toBe(true);
    });
});

describe("byte-length sizing for multibyte text", () => {
    test("CJK text under the char cap but over the byte cap promotes past TEXT", async () => {
        // 22,000 CJK chars ≈ 66,000 UTF-8 bytes: under TEXT's 65,535-char intuition but over
        // its 65,535-byte cap. Must promote to mediumtext or the DB truncates on insert.
        const cjk = "字".repeat(22000);
        const result = await getDataHeaders([{ note: cjk }], BASE_CONFIG);
        expect(["mediumtext", "longtext"]).toContain(result.note.type);
    });

    test("ASCII text of the same char length stays TEXT (no false promotion)", async () => {
        const ascii = "a".repeat(22000); // 22,000 bytes < 65,535
        const result = await getDataHeaders([{ note: ascii }], BASE_CONFIG);
        expect(result.note.type).toBe("text");
    });
});
