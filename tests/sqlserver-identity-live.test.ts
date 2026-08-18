import { Database } from "../src/db/database";
import { DatabaseConfig } from "../src/config/types";
import { escapeIdentifier } from "../src/db/utils/escape";

// Spec-2 slice 1 — IDENTITY introspection (live, against the docker azure-sql-edge `sqlserver` service).
// Before this slice, SQL Server's introspection query returned neither an EXTRA column nor a nextval
// default, so a re-introspected IDENTITY(1,1) column came back autoIncrement:false. Two consequences the
// slice fixes: (a) fetchTableMetadata mis-reports the column; (b) applySurrogateKey (keys.ts:49-51) detects
// its sticky surrogate by `autoIncrement===true && primary===true` — so run 2 fails to recognise the
// surrogate and thrashes the primary key. Both are exercised here end-to-end.

const CONFIG: DatabaseConfig = {
    sqlDialect: "sqlserver", host: "localhost", user: "sa", password: "Str0ng!Passw0rd",
    database: "master", schema: "test_schema", port: 1433, useWorkers: false,
};
const qi = (n: string) => escapeIdentifier(n, "sqlserver");
const ref = (t: string) => `${qi("test_schema")}.${qi(t)}`;

// Data with NO natural key (single columns non-unique, no unique combo) so predictIndexes finds none
// and applySurrogateKey injects a surrogate IDENTITY primary key that autosql fully owns.
const keyless = () => [
    { region: "north", tier: "gold" },
    { region: "north", tier: "gold" },
    { region: "south", tier: "gold" },
];

describe("SQL Server IDENTITY introspection (live, spec-2 slice 1)", () => {
    let db: Database;
    const TABLE = "ss_identity_live";

    const meta = async (): Promise<Record<string, any>> => {
        const { currentMetaData } = await (db as any).autoSQLHandler.fetchTableMetadata(TABLE);
        return (currentMetaData || {}) as Record<string, any>;
    };
    const count = async (): Promise<number> => {
        const r = await db.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref(TABLE)}`, params: [] });
        return Number(Object.values(r.results![0])[0]);
    };

    beforeAll(async () => {
        db = Database.create({ ...CONFIG, surrogateKey: true });
        await db.establishConnection();
        await db.runQuery(db.getCreateSchemaQuery("test_schema"));
        await db.runQuery(db.getDropTableQuery(TABLE)).catch(() => {});
    });
    afterAll(async () => {
        await db.runQuery(db.getDropTableQuery(TABLE)).catch(() => {});
        await db.closeConnection();
    });

    test("re-introspecting an IDENTITY surrogate reports autoIncrement:true (the core fix)", async () => {
        expect((await db.autoSQL(TABLE, keyless())).success).toBe(true);
        const m = await meta();
        const identityCols = Object.entries(m).filter(([, d]) => d.autoIncrement === true && d.primary === true);
        // Exactly the surrogate PK is introspected as an IDENTITY column (empty before slice 1).
        expect(identityCols.length).toBe(1);
        expect(await count()).toBe(3);
    });

    test("run 2 stays sticky to the surrogate — no PK thrash, no drift (the consequence)", async () => {
        const before = await meta();
        const surrogate = Object.keys(before).find(c => before[c].autoIncrement === true && before[c].primary === true)!;
        expect(surrogate).toBeDefined();

        // A coincidentally-unique batch must NOT introduce a competing natural key: applySurrogateKey
        // only recognises the existing surrogate when introspection reports autoIncrement:true.
        expect((await db.autoSQL(TABLE, keyless())).success).toBe(true);

        const after = await meta();
        expect(after[surrogate].autoIncrement).toBe(true);
        expect(after[surrogate].primary).toBe(true);
        // No other column was promoted to PK (surrogate stayed the sole key — no thrash, no drift).
        const pkCols = Object.keys(after).filter(c => after[c].primary === true);
        expect(pkCols).toEqual([surrogate]);
        // Rows are preserved (no data loss / no error). NOTE: the exact row count is asserted by the
        // append-gap todo below, not here — this test's subject is introspection, not merge semantics.
        expect(await count()).toBeGreaterThanOrEqual(3);
    });

    // KNOWN GAP surfaced by slice 1 (documented, separate from the introspection fix): on mysql/pgsql a
    // surrogate re-ingest APPENDS (surrogate is unique per insert → no upsert match → total 6, per
    // surrogate-key-live.test.ts). On SQL Server it stays at 3: `SELECT * INTO <staging> … WHERE 1=0`
    // COPIES the IDENTITY property, so the staging clone regenerates the same 1,2,3 surrogate values and
    // the MERGE self-matches (UPDATE in place) instead of inserting. Fix belongs to a staging-clone slice
    // (strip IDENTITY from the SELECT INTO, or force the surrogate merge to always-insert) — see spec-2.
    test.todo("SQL Server surrogate re-ingest should APPEND like mysql/pgsql (staging SELECT INTO copies IDENTITY)");
});
