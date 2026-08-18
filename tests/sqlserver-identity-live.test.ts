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

    test("run 2 stays sticky to the surrogate AND appends — no PK thrash, no self-match (§3.7)", async () => {
        const before = await meta();
        const surrogate = Object.keys(before).find(c => before[c].autoIncrement === true && before[c].primary === true)!;
        expect(surrogate).toBeDefined();

        // A coincidentally-unique batch must NOT introduce a competing natural key: applySurrogateKey
        // only recognises the existing surrogate when introspection reports autoIncrement:true (slice 1).
        expect((await db.autoSQL(TABLE, keyless())).success).toBe(true);

        const after = await meta();
        expect(after[surrogate].autoIncrement).toBe(true);
        expect(after[surrogate].primary).toBe(true);
        // Surrogate stayed the sole key — no thrash, no drift.
        expect(Object.keys(after).filter(c => after[c].primary === true)).toEqual([surrogate]);

        // §3.7: the insert-from-staging no longer MERGEs on the surrogate PK (excluded from the insert
        // columns → merging on it self-matched the staging clone's regenerated IDENTITY). It now plain-
        // INSERTs, so run 2 APPENDS like mysql/pgsql (3 + 3 = 6, all keys distinct), matching
        // surrogate-key-live.test.ts. Before the fix it stayed at 3 (UPDATE-in-place).
        expect(await count()).toBe(6);
        const distinct = await db.runQuery({ query: `SELECT COUNT(DISTINCT ${qi(surrogate)}) AS c FROM ${ref(TABLE)}`, params: [] });
        expect(Number(Object.values(distinct.results![0])[0])).toBe(6);
    });
});
