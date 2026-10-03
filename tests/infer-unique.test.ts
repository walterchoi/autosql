import { Database } from "../src/db/database";
import { getMetaData, getDataHeaders, compareMetaData, restrictInferredUniques } from "../src/helpers/metadata";
import { DatabaseConfig, MetadataHeader } from "../src/config/types";

// inferUnique / inferAdditionalUniques. A column that is 100% distinct within one batch gets a UNIQUE
// constraint, even beside an explicit primary key. On MySQL a stray UNIQUE(id) next to
// PRIMARY KEY (conn, id) makes ON DUPLICATE KEY UPDATE overwrite another connection's row. Both options
// default to true (unchanged behaviour); false opts out. Pure SQL generation, no live database (the live
// regression is in infer-unique-live.test.ts).

const mk = (d: string, extra: Partial<DatabaseConfig> = {}) =>
    Database.create({ sqlDialect: d as any, host: "h", user: "u", password: "p", database: "d", schema: "s", ...extra }) as any;
const cfg = (d: string, extra: Partial<DatabaseConfig> = {}): DatabaseConfig => ({ sqlDialect: d as any, ...extra });
const createSql = (d: string, meta: MetadataHeader) => mk(d).getCreateTableQuery("t", meta).map((q: any) => q.query).join("\n");
const uniques = (meta: MetadataHeader) => Object.keys(meta).filter(c => meta[c].unique);
const primaries = (meta: MetadataHeader) => Object.keys(meta).filter(c => meta[c].primary).sort();
const NO_EXTRA = { inferAdditionalUniques: false };

const ROWS = [
    { conn: "a", id: 1, name: "Alpha", code: "x1" },
    { conn: "a", id: 2, name: "Beta", code: "x2" },
    { conn: "a", id: 3, name: "Gamma", code: "x3" },
];

describe.each(["mysql", "pgsql"])("inferUnique / inferAdditionalUniques on a new table (%s)", (d) => {
    test("defaults with a primary key still infer uniques (unchanged)", async () => {
        const meta = await getMetaData(cfg(d), ROWS, ["conn", "id"]);
        expect(uniques(meta)).toEqual(expect.arrayContaining(["id", "name", "code"]));
        expect(createSql(d, meta)).toMatch(/UNIQUE/i);
    });

    test("defaults on a keyless table still infer uniques (unchanged)", async () => {
        const meta = await getMetaData(cfg(d), ROWS);
        expect(uniques(meta)).toEqual(expect.arrayContaining(["id", "name", "code"]));
        expect(primaries(meta)).toEqual(["id"]);
        expect(createSql(d, meta)).toMatch(/UNIQUE/i);
    });

    test("inferAdditionalUniques: false + primaryKey argument → only the PK, no UNIQUE", async () => {
        const meta = await getMetaData(cfg(d, NO_EXTRA), ROWS, ["conn", "id"]);
        expect(uniques(meta)).toEqual([]);
        expect(primaries(meta)).toEqual(["conn", "id"]);
        const sql = createSql(d, meta);
        expect(sql).toMatch(/PRIMARY KEY/i);
        expect(sql).not.toMatch(/UNIQUE/i);
    });

    test("inferAdditionalUniques: false treats config.primaryKey as an explicit key", async () => {
        const meta = await getMetaData(cfg(d, { ...NO_EXTRA, primaryKey: ["conn", "id"] }), ROWS);
        expect(uniques(meta)).toEqual([]);
        expect(primaries(meta)).toEqual(["conn", "id"]);
        expect(createSql(d, meta)).not.toMatch(/UNIQUE/i);
    });

    test("inferAdditionalUniques: false on a keyless new table still infers uniques", async () => {
        const meta = await getMetaData(cfg(d, NO_EXTRA), ROWS);
        expect(uniques(meta)).toEqual(expect.arrayContaining(["id", "name", "code"]));
    });

    test("stripping uniques leaves PK, index and pseudounique predictions unchanged", async () => {
        const restricted = await getMetaData(cfg(d, NO_EXTRA), ROWS, ["conn", "id"]);
        const defaults = await getMetaData(cfg(d), ROWS, ["conn", "id"]);
        for (const col of Object.keys(defaults)) {
            expect(restricted[col].primary).toBe(defaults[col].primary);
            expect(restricted[col].index).toBe(defaults[col].index);
            expect(restricted[col].pseudounique).toBe(defaults[col].pseudounique);
        }
    });

    test("inferUnique: false on a keyless table → no uniques, same predicted PK", async () => {
        const meta = await getMetaData(cfg(d, { inferUnique: false }), ROWS);
        expect(uniques(meta)).toEqual([]);
        expect(primaries(meta)).toEqual(["id"]);
        expect(createSql(d, meta)).not.toMatch(/UNIQUE/i);
    });

    test("inferUnique: false wins over inferAdditionalUniques: true", async () => {
        const meta = await getMetaData(cfg(d, { inferUnique: false, inferAdditionalUniques: true }), ROWS, ["conn", "id"]);
        expect(uniques(meta)).toEqual([]);
    });

    test("autoIndexing: false still honours an explicit key", async () => {
        const meta = await getMetaData(cfg(d, { ...NO_EXTRA, autoIndexing: false }), ROWS, ["conn", "id"]);
        expect(uniques(meta)).toEqual([]);
    });
});

describe("getDataHeaders stays a raw profiler", () => {
    test("reports in-batch uniqueness regardless of the options", async () => {
        const headers = await getDataHeaders(ROWS, cfg("mysql", { inferUnique: false }));
        expect(headers.id.unique).toBe(true);
    });
});

describe("restrictInferredUniques (existing table)", () => {
    const current: MetadataHeader = {
        id: { type: "int", primary: true, unique: false },
        email: { type: "varchar", length: 50, unique: true },
        code: { type: "varchar", length: 10, unique: false },
    };
    const inferred: MetadataHeader = {
        id: { type: "int", primary: true, unique: true },
        email: { type: "varchar", length: 50, unique: true },
        code: { type: "varchar", length: 10, unique: true },   // newly 100% distinct in this batch
        extra: { type: "varchar", length: 10, unique: true },  // brand-new column
    };

    test("keeps uniques already on the live column, drops the rest", () => {
        const out = restrictInferredUniques(inferred, current, cfg("mysql", NO_EXTRA));
        expect(uniques(out)).toEqual(["email"]);
        expect(out.id.primary).toBe(true);
    });

    test("does not mutate its input", () => {
        const before = JSON.stringify(inferred);
        restrictInferredUniques(inferred, current, cfg("mysql", NO_EXTRA));
        expect(JSON.stringify(inferred)).toBe(before);
    });

    test("no-op for a new table (null or empty current metadata)", () => {
        expect(restrictInferredUniques(inferred, null, cfg("mysql", NO_EXTRA))).toBe(inferred);
        expect(restrictInferredUniques(inferred, {}, cfg("mysql", NO_EXTRA))).toBe(inferred);
    });

    test("no-op with defaults", () => {
        expect(restrictInferredUniques(inferred, current, cfg("mysql"))).toBe(inferred);
    });

    test("an existing unique is not reported as noLongerUnique", () => {
        const { changes } = compareMetaData(current, restrictInferredUniques(inferred, current, cfg("mysql", NO_EXTRA)));
        expect(changes.noLongerUnique).toEqual([]);
        expect(changes.addColumns.extra.unique).toBe(false);
    });
});

describe.each(["mysql", "pgsql"])("ALTER never adds a UNIQUE (%s)", (d) => {
    test("an added column flagged unique gets a plain ADD COLUMN", async () => {
        const old: MetadataHeader = { id: { type: "int", primary: true, length: 0 } };
        const next: MetadataHeader = {
            id: { type: "int", primary: true, length: 0 },
            extra: { type: "varchar", length: 10, unique: true },
        };
        const { changes } = compareMetaData(old, next);
        // compareMetaData passes the flag through on addColumns; the ALTER builder must ignore it.
        expect(changes.addColumns.extra.unique).toBe(true);
        const sql = (await mk(d).getAlterTableQuery("t", changes)).map((q: any) => q.query).join("\n");
        expect(sql).toMatch(/ADD COLUMN/i);
        expect(sql).not.toMatch(/UNIQUE/i);
    });
});
