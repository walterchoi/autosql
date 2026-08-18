import { Database } from "../src/db/database";
import { DatabaseConfig } from "../src/config/types";
import { escapeIdentifier } from "../src/db/utils/escape";

// Spec-2 slice 3 — schemaHistory on SQL Server (live). The bootstrap DDL + record/detect queries were
// Postgres/MySQL-only (BIGSERIAL/JSONB/TIMESTAMPTZ, LIMIT, RETURNING); this slice retargets them to
// T-SQL (BIGINT IDENTITY, NVARCHAR(MAX), DATETIME2, TOP, OUTPUT INSERTED.id) and removes the
// validateConfig guard. Guard-removal rule (spec §7): the sad path must actually FIRE — an out-of-band
// change is DETECTED as drift, not merely "bootstrap didn't throw".
//
// A baseline is recorded only on an actual schema CHANGE (not the initial CREATE — its changeset is
// empty), so these tests evolve an existing table to force a recorded 'applied' version, then drift it.

const CONFIG: DatabaseConfig = {
    sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd",
    database: "master", schema: "test_schema", port: 1433, useWorkers: false,
};
const qi = (n: string) => escapeIdentifier(n, "sqlserver");

describe("SQL Server schemaHistory + drift detection (live, spec-2 slice 3)", () => {
    const TABLE = "ss_drift_live";
    const HISTORY = "autosql_schema_history";
    const ref = `${qi("test_schema")}.${qi(TABLE)}`;
    const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;
    const histRef = `${qi("test_schema")}.${qi(HISTORY)}`;
    const baseConfig = { ...CONFIG, addTimestamps: true, schemaHistory: true, strictDriftDetection: true };

    let admin: Database;
    const dropTable = async () => { for (const r of [tempRef, ref]) await admin.runQuery({ query: `DROP TABLE IF EXISTS ${r}`, params: [] }).catch(() => {}); };
    const clearHistory = async () => { await admin.runQuery({ query: `IF OBJECT_ID('test_schema.${HISTORY}','U') IS NOT NULL DELETE FROM ${histRef} WHERE table_name = @p0`, params: [TABLE] }).catch(() => {}); };
    const historyRows = async () => (await admin.runQuery({ query: `SELECT version, status, checksum FROM ${histRef} WHERE table_name = @p0 ORDER BY version`, params: [TABLE] })).results as any[];

    beforeAll(async () => { admin = Database.create(baseConfig); await admin.establishConnection(); await admin.runQuery(admin.getCreateSchemaQuery("test_schema")); });
    afterAll(async () => { await dropTable(); await admin.closeConnection(); });
    beforeEach(async () => { await dropTable(); await clearHistory(); });

    test("a schema change records an 'applied' migration with a checksum (bootstrap + record path)", async () => {
        const db = Database.create(baseConfig); await db.establishConnection();
        try {
            expect((await db.autoSQL(TABLE, [{ id: 1, name: "alpha" }])).success).toBe(true); // CREATE: no baseline yet
            expect((await historyRows()).length).toBe(0);

            // Evolve the schema (ADD a column) → a real changeset → recorded as version 1, 'applied'.
            expect((await db.autoSQL(TABLE, [{ id: 1, name: "alpha", score: 42 }])).success).toBe(true);
            const rows = await historyRows();
            expect(rows.length).toBe(1);
            expect(rows[0].status).toBe("applied");
            expect(rows[0].checksum).toBeTruthy(); // baseline checksum stored (NVARCHAR/DATETIME2 path works)

            // Re-ingesting the same shape must NOT false-positive drift (strict would throw).
            expect((await db.autoSQL(TABLE, [{ id: 2, name: "beta", score: 7 }])).success).toBe(true);
        } finally { await db.closeConnection(); }
    });

    test("discriminating: an out-of-band ALTER IS detected as drift (detection isn't vacuous)", async () => {
        const db = Database.create(baseConfig); await db.establishConnection();
        try {
            await db.autoSQL(TABLE, [{ id: 1, name: "alpha" }]);
            expect((await db.autoSQL(TABLE, [{ id: 1, name: "alpha", score: 42 }])).success).toBe(true); // records baseline
            expect((await historyRows()).length).toBe(1);

            // Modify the table outside autosql, then re-ingest: strict drift must reject it.
            await admin.runQuery({ query: `ALTER TABLE ${ref} ADD ${qi("sneaky")} INT`, params: [] });
            const after = await db.autoSQL(TABLE, [{ id: 1, name: "alpha", score: 42 }]);
            expect(after.success).toBe(false);
            expect(String(after.error)).toMatch(/drift/i);
        } finally { await db.closeConnection(); }
    });
});
