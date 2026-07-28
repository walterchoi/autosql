import { applySurrogateKey } from "../src/helpers/keys";
import { compareMetaData } from "../src/helpers/metadata";
import { validateConfig } from "../src/helpers/utilities";
import { MySQLInsertQueryBuilder } from "../src/db/queryBuilders/mysql/insertBuilder";
import { PostgresInsertQueryBuilder } from "../src/db/queryBuilders/pgsql/insertBuilder";
import { mysqlConfig } from "../src/db/config/mysqlConfig";
import { ColumnDefinition, MetadataHeader, DatabaseConfig } from "../src/config/types";
import { DB_CONFIG, Database } from "./utils/testConfig";

const col = (o: Partial<ColumnDefinition> = {}): ColumnDefinition => ({
    type: "varchar",
    length: 10,
    allowNull: false,
    unique: false,
    index: false,
    pseudounique: false,
    primary: false,
    autoIncrement: false,
    decimal: 0,
    ...o,
});

const on: DatabaseConfig = { sqlDialect: "mysql", surrogateKey: true } as DatabaseConfig;

describe("applySurrogateKey", () => {
    test("is a no-op when surrogateKey is disabled", () => {
        const meta: MetadataHeader = { name: col() };
        expect(applySurrogateKey(meta, null, { sqlDialect: "mysql" } as DatabaseConfig)).toBe(meta);
    });

    test("injects a bigint auto-increment PK on a new table with no natural key", () => {
        const meta: MetadataHeader = { name: col(), age: col({ type: "int" }) };
        const result = applySurrogateKey(meta, null, on);
        expect(Object.keys(result)[0]).toBe("autosql_id"); // prepended
        expect(result.autosql_id).toMatchObject({ type: "bigint", primary: true, autoIncrement: true, allowNull: false });
        expect(result.autosql_id.unique).toBe(false); // PK is implicitly unique — no redundant constraint
    });

    test("does not inject when a natural primary key exists", () => {
        const meta: MetadataHeader = { id: col({ type: "int", primary: true }), name: col() };
        expect(applySurrogateKey(meta, null, on)).toBe(meta);
    });

    test("honours a custom surrogateKeyColumn name", () => {
        const meta: MetadataHeader = { name: col() };
        const cfg = { sqlDialect: "mysql", surrogateKey: true, surrogateKeyColumn: "sk" } as DatabaseConfig;
        const result = applySurrogateKey(meta, null, cfg);
        expect(result.sk?.primary).toBe(true);
        expect(result.autosql_id).toBeUndefined();
    });

    test("throws on a name collision with an existing data column", () => {
        const meta: MetadataHeader = { autosql_id: col(), name: col() };
        expect(() => applySurrogateKey(meta, null, on)).toThrow(/already exists/);
    });

    describe("stickiness to the existing table", () => {
        test("keeps the existing surrogate as sole PK even if this batch inferred a natural key", () => {
            const existing: MetadataHeader = { autosql_id: col({ type: "bigint", primary: true, autoIncrement: true }), code: col() };
            // A coincidentally-unique batch inferred `code` as primary.
            const inferred: MetadataHeader = { code: col({ primary: true, unique: true }) };
            const result = applySurrogateKey(inferred, existing, on);
            expect(result.autosql_id).toMatchObject({ primary: true, autoIncrement: true });
            expect(result.code.primary).toBe(false); // competing natural key demoted
        });

        test("never introduces a surrogate when the existing table has none", () => {
            const existing: MetadataHeader = { id: col({ type: "int", primary: true }), name: col() };
            const inferred: MetadataHeader = { name: col() }; // no PK found this run
            const result = applySurrogateKey(inferred, existing, on);
            expect(result.autosql_id).toBeUndefined();
        });
    });

    test("re-ingestion is idempotent — compareMetaData sees no key change", () => {
        const existing: MetadataHeader = {
            autosql_id: col({ type: "bigint", length: 0, primary: true, autoIncrement: true }),
            name: col(),
            age: col({ type: "int" }),
        };
        const inferred: MetadataHeader = { name: col(), age: col({ type: "int" }) };
        const reconciled = applySurrogateKey(inferred, existing, on);
        const { changes } = compareMetaData(existing, reconciled, mysqlConfig);
        expect(changes.addColumns).toEqual({});
        expect(changes.dropColumns).toEqual([]);
        expect(changes.primaryKeyChanges).toEqual([]);
    });
});

describe("insert builders exclude auto-increment (surrogate) columns", () => {
    const meta: MetadataHeader = {
        autosql_id: col({ type: "bigint", primary: true, autoIncrement: true }),
        name: col(),
        age: col({ type: "int" }),
    };
    const rows = [{ name: "a", age: 1 }, { name: "b", age: 2 }];

    test("MySQL: surrogate omitted from columns and params", () => {
        const q = MySQLInsertQueryBuilder.getInsertStatementQuery("t", rows, meta, { sqlDialect: "mysql", surrogateKey: true } as DatabaseConfig, "INSERT") as { query: string; params: any[] };
        expect(q.query).not.toContain("autosql_id");
        expect(q.query).toContain("`name`");
        expect(q.query).toContain("`age`");
        expect(q.params).toEqual(["a", 1, "b", 2]);
    });

    test("Postgres: surrogate omitted from columns and params", () => {
        const q = PostgresInsertQueryBuilder.getInsertStatementQuery("t", rows, meta, { sqlDialect: "pgsql", surrogateKey: true } as DatabaseConfig, "INSERT") as { query: string; params: any[] };
        expect(q.query).not.toContain("autosql_id");
        expect(q.query).toContain('"name"');
        expect(q.query).toContain('"age"');
        expect(q.params).toEqual(["a", 1, "b", 2]);
    });

    // Regression guard: exclusion is gated on surrogateKey. A genuine AUTO_INCREMENT / SERIAL
    // primary key (introspected as autoIncrement:true) whose values a caller supplies for upsert
    // must NOT be dropped when surrogateKey is off — otherwise the ON DUPLICATE/ON CONFLICT match
    // fails and rows are appended instead of upserted.
    const idMeta: MetadataHeader = {
        id: col({ type: "int", primary: true, autoIncrement: true }),
        name: col(),
    };
    const idRows = [{ id: 1, name: "a" }, { id: 2, name: "b" }];

    test("MySQL: auto_increment id is INCLUDED when surrogateKey is off", () => {
        const q = MySQLInsertQueryBuilder.getInsertStatementQuery("t", idRows, idMeta, { sqlDialect: "mysql" } as DatabaseConfig, "UPDATE") as { query: string; params: any[] };
        expect(q.query).toContain("`id`");
        expect(q.params).toEqual([1, "a", 2, "b"]);
    });

    test("Postgres: auto_increment id is INCLUDED when surrogateKey is off", () => {
        const q = PostgresInsertQueryBuilder.getInsertStatementQuery("t", idRows, idMeta, { sqlDialect: "pgsql" } as DatabaseConfig, "UPDATE") as { query: string; params: any[] };
        expect(q.query).toContain('"id"');
        expect(q.params).toEqual([1, "a", 2, "b"]);
    });
});

describe("validateConfig guards surrogateKey against incompatible modes", () => {
    test.each(["addHistory", "addNested", "autoSplit"])("rejects surrogateKey + %s", (mode) => {
        expect(() =>
            validateConfig({ sqlDialect: "mysql", surrogateKey: true, [mode]: true } as DatabaseConfig)
        ).toThrow(/surrogateKey is not compatible/);
    });

    test("allows surrogateKey on its own", () => {
        expect(() => validateConfig({ sqlDialect: "mysql", surrogateKey: true } as DatabaseConfig)).not.toThrow();
    });
});

describe("surrogate DDL rendering", () => {
    // Build the fixture the way the library does, so the surrogate column def is faithful.
    const meta: MetadataHeader = applySurrogateKey({ name: col() }, null, on);

    test("Postgres renders BIGSERIAL for a bigint auto-increment key", () => {
        const db = Database.create(DB_CONFIG.pgsql);
        const queries = db.createTableQuery("t", meta);
        const sql = typeof queries[0] === "string" ? queries[0] : (queries[0] as any).query;
        expect(sql).toContain('"autosql_id" BIGSERIAL');
        expect(sql).toContain('PRIMARY KEY ("autosql_id")');
    });

    test("Postgres still renders SERIAL (not BIGSERIAL) for an int auto-increment key", () => {
        const db = Database.create(DB_CONFIG.pgsql);
        const intMeta: MetadataHeader = { id: col({ type: "int", primary: true, autoIncrement: true }) };
        const queries = db.createTableQuery("t", intMeta);
        const sql = typeof queries[0] === "string" ? queries[0] : (queries[0] as any).query;
        expect(sql).toContain('"id" SERIAL');
        expect(sql).not.toContain("BIGSERIAL");
    });

    test("MySQL renders bigint AUTO_INCREMENT", () => {
        const db = Database.create(DB_CONFIG.mysql);
        const queries = db.createTableQuery("t", meta);
        const sql = typeof queries[0] === "string" ? queries[0] : (queries[0] as any).query;
        expect(sql).toContain("`autosql_id` bigint AUTO_INCREMENT");
    });
});
