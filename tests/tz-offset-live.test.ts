import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Perf/i18n: a genuinely tz-aware value ("2024-01-15T10:00:00+05:00" → inferred `datetimetz`) now keeps
// its offset on dialects whose type STORES one — SQL Server `datetimeoffset` and Postgres `timestamptz`
// (which stores the correct instant). MySQL `timestamp` can't hold an offset, so it still UTC-normalises.

const SS: any = { sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd", database: "master", port: 1433, useWorkers: false };
const VALUE = "2024-01-15T10:00:00+05:00"; // 10:00 at +05:00 == 05:00 UTC

describe("datetimetz offset round-trip (live)", () => {
    test("SQL Server: the +05:00 offset is PRESERVED in datetimeoffset (not flattened to UTC)", async () => {
        const qi = (n: string) => escapeIdentifier(n, "sqlserver");
        const TABLE = "tz_offset_ss";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const db: any = Database.create({ ...SS, schema: "test_schema" });
        await db.establishConnection();
        await db.runQuery(db.getCreateSchemaQuery("test_schema"));
        await db.runQuery(db.getDropTableQuery(TABLE)).catch(() => {});
        try {
            expect((await db.autoSQL(TABLE, [{ id: 1, ts: VALUE }])).success).toBe(true);
            const r = await db.runQuery({ query: `SELECT DATEPART(TZOFFSET, ${qi("ts")}) AS tzmin, CONVERT(varchar(40), ${qi("ts")}, 126) AS iso FROM ${ref} WHERE ${qi("id")} = 1`, params: [] });
            expect(Number((r.results![0] as any).tzmin)).toBe(300);            // +05:00 == 300 minutes, preserved
            expect(String((r.results![0] as any).iso)).toContain("+05:00");    // and the wall-clock/offset survive
            expect(String((r.results![0] as any).iso)).toContain("10:00:00");
            await db.runQuery(db.getDropTableQuery(TABLE)).catch(() => {});
        } finally { await db.closeConnection(); }
    });

    const pg = Object.values(DB_CONFIG).find((c: any) => c.sqlDialect === "pgsql") as any;
    (pg ? test : test.skip)("Postgres: timestamptz stores the correct instant (05:00 UTC)", async () => {
        const qi = (n: string) => escapeIdentifier(n, "pgsql");
        const TABLE = "tz_offset_pg";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const db: any = Database.create({ ...pg, schema: "test_schema" });
        await db.establishConnection();
        await db.runQuery(db.getDropTableQuery(TABLE)).catch(() => {});
        try {
            expect((await db.autoSQL(TABLE, [{ id: 1, ts: VALUE }])).success).toBe(true);
            const r = await db.runQuery({ query: `SELECT ${qi("ts")} AS ts FROM ${ref} WHERE ${qi("id")} = 1`, params: [] });
            expect(new Date((r.results![0] as any).ts).toISOString()).toBe("2024-01-15T05:00:00.000Z");
            await db.runQuery(db.getDropTableQuery(TABLE)).catch(() => {});
        } finally { await db.closeConnection(); }
    });

    const my = Object.values(DB_CONFIG).find((c: any) => c.sqlDialect === "mysql") as any;
    (my ? test : test.skip)("MySQL: an offset value still loads (UTC-normalised, timestamp can't hold an offset)", async () => {
        const TABLE = "tz_offset_my";
        const db: any = Database.create({ ...my, schema: "test_schema" });
        await db.establishConnection();
        await db.runQuery(db.getDropTableQuery(TABLE)).catch(() => {});
        try {
            expect((await db.autoSQL(TABLE, [{ id: 1, ts: VALUE }])).success).toBe(true); // no offset-literal error
            await db.runQuery(db.getDropTableQuery(TABLE)).catch(() => {});
        } finally { await db.closeConnection(); }
    });
});
