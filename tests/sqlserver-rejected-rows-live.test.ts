import { Database } from "../src/db/database";
import { DatabaseConfig } from "../src/config/types";
import { escapeIdentifier } from "../src/db/utils/escape";

// Spec-2 slice 4 — rejectedRowsTable (dead-letter) on SQL Server (live). The bootstrap + insert builders
// were Postgres/MySQL-only (BIGSERIAL/JSONB/TIMESTAMPTZ, $n/::jsonb); this slice adds a T-SQL branch
// (BIGINT IDENTITY, NVARCHAR(MAX) JSON text, DATETIME2, @pN) and removes the validateConfig guard.
// Guard-removal rule (spec §7): the sad path must FIRE and land data — a CHECK-violating row must divert
// to the dead-letter table with the RIGHT raw_data + error_message, not vanish.

const CONFIG: DatabaseConfig = {
    sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd",
    database: "master", schema: "test_schema", port: 1433, useWorkers: false,
};
const qi = (n: string) => escapeIdentifier(n, "sqlserver");

describe("SQL Server rejectedRowsTable dead-letter (live, spec-2 slice 4)", () => {
    const TABLE = "ss_reject_test";
    const REJECTED = "ss_reject_dlq";
    const ref = `${qi("test_schema")}.${qi(TABLE)}`;
    const rejRef = `${qi("test_schema")}.${qi(REJECTED)}`;
    const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
    const baseConfig = { ...CONFIG, addTimestamps: false, streamMaxRetries: 1 };

    let admin: Database;
    const dropAll = async () => { for (const r of [tempRef, ref, rejRef]) await admin.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {}); };
    const rowCount = async (t: string) => Number(Object.values((await admin.runQuery({ query: `SELECT COUNT(*) AS c FROM ${t}`, params: [] })).results![0])[0]);
    const valOf = async (id: number) => { const r = await admin.runQuery({ query: `SELECT ${qi("val")} AS v FROM ${ref} WHERE ${qi("id")} = ${id}`, params: [] }); return r.results!.length ? Object.values(r.results![0])[0] : null; };
    const dlqRow = async () => (await admin.runQuery({ query: `SELECT target_table, error_message, raw_data FROM ${rejRef}`, params: [] })).results![0] as any;

    beforeAll(async () => { admin = Database.create(baseConfig); await admin.establishConnection(); await admin.runQuery(admin.getCreateSchemaQuery("test_schema")); });
    afterAll(async () => { await dropAll(); await admin.closeConnection(); });
    beforeEach(async () => {
        await dropAll();
        // A CHECK on amount forces a bad row's insert to fail (inference can't "fix" it), triggering the
        // per-row degradation → divert. val is a plain INT for the surviving good row.
        await admin.runQuery({ query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("amount")} INT, ${qi("val")} INT, CONSTRAINT ${qi("ss_amt_nonneg")} CHECK (${qi("amount")} >= 0))`, params: [] });
    });

    for (const useStagingInsert of [false, true]) {
        test(`${useStagingInsert ? "staging" : "direct"} path: a CHECK-violating row diverts with correct raw_data + error_message`, async () => {
            const db = Database.create({ ...baseConfig, useStagingInsert, rejectedRowsTable: REJECTED });
            await db.establishConnection();
            try {
                const r = await db.autoSQL(TABLE, [
                    { id: 2, amount: -5, val: 100 }, // violates CHECK → diverted
                    { id: 3, amount: 20, val: 200 }, // good → lands
                ]);
                expect(r.success).toBe(true);           // degraded gracefully, not a hard failure
                expect(Number(await valOf(3))).toBe(200); // good row landed
                expect(await valOf(2)).toBeNull();      // bad row not in the target
                expect(await rowCount(rejRef)).toBe(1); // exactly the bad row diverted

                // The dead-letter row carries the right target, a non-empty error, and the ORIGINAL row
                // as JSON (NVARCHAR(MAX) round-trips through JSON.parse).
                const dlq = await dlqRow();
                expect(dlq.target_table).toBe(TABLE);
                expect(String(dlq.error_message).length).toBeGreaterThan(0);
                const raw = typeof dlq.raw_data === "string" ? JSON.parse(dlq.raw_data) : dlq.raw_data;
                expect(Number(raw.id)).toBe(2);
                expect(Number(raw.amount)).toBe(-5);
            } finally { await db.closeConnection(); }
        });
    }
});
