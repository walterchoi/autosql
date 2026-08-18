import { Database } from "../src/db/database";
import { DatabaseConfig } from "../src/config/types";
import { escapeIdentifier } from "../src/db/utils/escape";

// Spec-4 slice B — openStream on SQL Server (live). The stream builders (create/insert/merge/drop/orphan)
// were Postgres/MySQL-only; this slice adds T-SQL branches (NVARCHAR(MAX) staging, @pN inserts,
// MERGE WITH (HOLDLOCK) + CAST/NULLIF casts, IF OBJECT_ID guards) and removes the openStream guard.
// Covers: typed round-trip, streaming upsert, the 2,100-parameter sub-batching, and abort cleanup.

const CONFIG: DatabaseConfig = {
    sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd",
    database: "master", schema: "test_schema", port: 1433, useWorkers: false,
};
const qi = (n: string) => escapeIdentifier(n, "sqlserver");
const ref = (t: string) => `${qi("test_schema")}.${qi(t)}`;

describe("SQL Server streaming openStream (live, spec-4 slice B)", () => {
    let db: Database;
    const count = async (t: string) => Number(Object.values((await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref(t)}`, params: [] })).results![0])[0]);
    const drop = async (t: string) => { await db.runQuery(db.getDropTableQuery(t)).catch(() => {}); };

    beforeAll(async () => { db = Database.create(CONFIG); await db.establishConnection(); await db.runQuery(db.getCreateSchemaQuery("test_schema")); });
    afterAll(async () => { await db.closeConnection(); });

    test("streams typed values (decimal/bool/datetime/nvarchar) and merges into the real table", async () => {
        const TABLE = "ss_stream_types";
        await drop(TABLE);
        const h = await db.openStream(TABLE);
        await h.write([
            { id: 1, amount: "9.99", active: true, ts: "2024-01-15 10:00:00", note: "日本語 😀 café" },
            { id: 2, amount: "3.50", active: false, ts: "2024-07-20 18:30:00", note: "n2" },
        ]);
        expect((await h.end()).success).toBe(true);
        expect(await count(TABLE)).toBe(2);
        const r = await db.runQuery({ query: `SELECT ${qi("amount")} AS amount, ${qi("active")} AS active, ${qi("note")} AS note FROM ${ref(TABLE)} WHERE ${qi("id")} = 1`, params: [] });
        expect(Number((r.results![0] as any).amount)).toBe(9.99);
        expect(Number((r.results![0] as any).active)).toBe(1);     // boolean → BIT
        expect((r.results![0] as any).note).toBe("日本語 😀 café"); // NVARCHAR round-trip
        await drop(TABLE);
    });

    test("streaming upsert updates matched rows and inserts new ones (MERGE)", async () => {
        const TABLE = "ss_stream_upsert";
        await drop(TABLE);
        let h = await db.openStream(TABLE);
        await h.write([{ id: 1, val: 10 }, { id: 2, val: 20 }]);
        expect((await h.end()).success).toBe(true);

        h = await db.openStream(TABLE);
        await h.write([{ id: 2, val: 25 }, { id: 3, val: 30 }]); // update id=2, insert id=3
        expect((await h.end()).success).toBe(true);

        expect(await count(TABLE)).toBe(3);
        const r = await db.runQuery({ query: `SELECT ${qi("val")} AS val FROM ${ref(TABLE)} WHERE ${qi("id")} = 2`, params: [] });
        expect(Number((r.results![0] as any).val)).toBe(25);
        await drop(TABLE);
    });

    test("a single write() exceeding the 2,100-parameter cap sub-batches and lands every row", async () => {
        const TABLE = "ss_stream_bigchunk";
        await drop(TABLE);
        const h = await db.openStream(TABLE);
        // 800 rows × 3 cols = 2400 bound params > 2100 → write() must split into sub-batches.
        const big = Array.from({ length: 800 }, (_, i) => ({ id: i + 1, a: "x" + i, b: i % 7 }));
        await h.write(big);
        expect((await h.end()).success).toBe(true);
        expect(await count(TABLE)).toBe(800);
        await drop(TABLE);
    });

    test("abort() drops staging and leaves no real table", async () => {
        const TABLE = "ss_stream_abort";
        await drop(TABLE);
        const h = await db.openStream(TABLE);
        await h.write([{ id: 1, val: 1 }]);
        await h.abort();
        const exists = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'test_schema' AND TABLE_NAME = 'ss_stream_abort'`, params: [] });
        expect(Number((exists.results![0] as any).c)).toBe(0);
    });
});
