import { Database } from "../src/db/database";
import { DatabaseConfig } from "../src/config/types";
import { escapeIdentifier } from "../src/db/utils/escape";

// SQL Server / Azure SQL adapter — live core-ETL coverage against the docker `sqlserver` service
// (mcr.microsoft.com/azure-sql-edge). Covers the row-store (Class A) path: create, insert, idempotent
// re-ingest (introspection round-trip), MERGE upsert, add-column evolution, and multilingual/emoji
// round-trip via NVARCHAR. Advanced features (streaming, schema history, split tables, bulk-copy) are
// not yet implemented for SQL Server — see decisions.md D-F.

const CONFIG: DatabaseConfig = {
    sqlDialect: "sqlserver",
    host: "localhost",
    user: "sa",
    password: "Str0ng!Passw0rd",
    database: "master",
    schema: "test_schema",
    port: 1433,
    useWorkers: false,
};

const qi = (n: string) => escapeIdentifier(n, "sqlserver");
const ref = (t: string) => `${qi("test_schema")}.${qi(t)}`;
const NAMES = ["alpha", "beta", "gamma", "delta"];
// Consistent 2-decimal amounts (avoids a shared, dialect-agnostic decimal-precision inference edge).
const batch1 = Array.from({ length: 12 }, (_, i) => ({
    id: i + 1,
    name: NAMES[i % 4],
    amount: Number(((i + 1) * 1.25).toFixed(2)),
    active: i % 2 === 0,
    note: i === 5 ? "日本語 😀 café" : "n" + i,
}));

describe("SQL Server adapter (live)", () => {
    const TABLE = "ss_live_test";
    let db: Database;

    const count = async (): Promise<number> => {
        const r = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref(TABLE)}`, params: [] });
        return Number(Object.values(r.results![0])[0]);
    };
    const colTypes = async (): Promise<Record<string, string | null>> => {
        const { currentMetaData } = await (db as any).autoSQLHandler.fetchTableMetadata(TABLE);
        const m: Record<string, string | null> = {};
        for (const [c, d] of Object.entries((currentMetaData || {}) as Record<string, any>)) m[c] = d.type;
        return m;
    };

    beforeAll(async () => {
        db = Database.create(CONFIG);
        await db.establishConnection();
        await db.runQuery(db.getCreateSchemaQuery("test_schema"));
        await db.runQuery(db.getDropTableQuery(TABLE)).catch(() => {});
    });

    afterAll(async () => {
        await db.runQuery(db.getDropTableQuery(TABLE)).catch(() => {});
        await db.closeConnection();
    });

    test("create + insert; emoji/multilingual round-trips via NVARCHAR", async () => {
        expect((await db.autoSQL(TABLE, batch1)).success).toBe(true);
        expect(await count()).toBe(12);
        const r = await db.runQuery({ query: `SELECT ${qi("note")} AS note FROM ${ref(TABLE)} WHERE ${qi("id")} = 6`, params: [] });
        expect((r.results![0] as any).note).toBe("日本語 😀 café");
    });

    test("re-ingesting identical data is schema-idempotent (introspection round-trip)", async () => {
        const before = await colTypes();
        expect((await db.autoSQL(TABLE, batch1)).success).toBe(true);
        const after = await colTypes();
        expect(after).toEqual(before); // no spurious re-typing ALTER
        expect(await count()).toBe(12);
    });

    test("MERGE upsert updates matched rows and inserts new ones", async () => {
        const res = await db.autoSQL(TABLE, [
            { id: 6, name: "beta", amount: 99.99, active: false, note: "updated 🚀" },
            { id: 13, name: "alpha", amount: 1.0, active: true, note: "new" },
        ]);
        expect(res.success).toBe(true);
        expect(await count()).toBe(13); // 12 + 1 inserted (id 6 updated in place)
        const r = await db.runQuery({ query: `SELECT ${qi("note")} AS note, ${qi("amount")} AS amount FROM ${ref(TABLE)} WHERE ${qi("id")} = 6`, params: [] });
        expect((r.results![0] as any).note).toBe("updated 🚀");
        expect(Number((r.results![0] as any).amount)).toBe(99.99);
    });

    test("adding a new column on re-ingest evolves the schema (ALTER ADD)", async () => {
        const withNewCol = batch1.slice(0, 3).map((row) => ({ ...row, category: "C" + (row.id % 2) }));
        expect((await db.autoSQL(TABLE, withNewCol)).success).toBe(true);
        const types = await colTypes();
        expect(types.category).toBeDefined(); // new column exists
        const r = await db.runQuery({ query: `SELECT ${qi("category")} AS category FROM ${ref(TABLE)} WHERE ${qi("id")} = 1`, params: [] });
        expect((r.results![0] as any).category).toBe("C1");
    });
});
