import { Database } from "../src/db/database";
import { DatabaseConfig } from "../src/config/types";
import { escapeIdentifier } from "../src/db/utils/escape";

// SQL Server counterpart of resolve-conflicts-derivation-live: proves the metadata-derivation win
// end-to-end on the sqlserver MERGE-from-staging path (which also reaches resolveConflicts). The
// enriched sys.indexes name must feed getDropUniqueConstraintQuery correctly — sqlserver isn't in
// the shared DB_CONFIG (its ~250-test parity isn't complete, D-F), so it gets its own focused test.

const CONFIG: DatabaseConfig = {
    sqlDialect: "sqlserver",
    host: "localhost",
    user: "sa",
    password: "Str0ng!Passw0rd",
    database: "master",
    schema: "test_schema",
    port: 1433,
    useWorkers: false,
    addTimestamps: false,
    dropUniqueConstraints: true, // this suite exercises the auto-drop, now opt-in (A10)
};

const qi = (n: string) => escapeIdentifier(n, "sqlserver");

describe("resolveConflicts metadata derivation (live) for SQLSERVER", () => {
    const TABLE = "ss_resolve_derivation_test";
    const ref = `${qi("test_schema")}.${qi(TABLE)}`;
    let db: Database;
    let admin: Database;

    const rowCount = async () => {
        const r = await admin.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
        return Number(Object.values(r.results![0])[0]);
    };
    const uniqueIndexNames = async () => {
        const r = await admin.runQuery(admin.getUniqueIndexesQuery(TABLE));
        return (r.results ?? []).map((row: any) => String(row.index_name ?? row.INDEX_NAME));
    };

    beforeAll(async () => {
        admin = Database.create(CONFIG);
        await admin.establishConnection();
        await admin.runQuery(admin.getCreateSchemaQuery("test_schema"));
    });
    afterAll(async () => {
        await admin.runQuery(admin.getDropTableQuery(TABLE)).catch(() => {});
        await admin.closeConnection();
    });
    beforeEach(async () => {
        await admin.runQuery(admin.getDropTableQuery(TABLE)).catch(() => {});
        await admin.runQuery({
            query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("code")} VARCHAR(20), ${qi("val")} INT, CONSTRAINT ${qi("uq_code")} UNIQUE (${qi("code")}))`,
            params: [],
        });
        await admin.runQuery({ query: `INSERT INTO ${ref} (${qi("id")}, ${qi("code")}, ${qi("val")}) VALUES (1, 'A', 10)`, params: [] });
    });

    test("derives (no introspection round-trip) and drops the right unique by its real name", async () => {
        db = Database.create(CONFIG);
        await db.establishConnection();
        const uniqueIdxSpy = jest.spyOn(db, "getUniqueIndexesQuery");
        const pkSpy = jest.spyOn(db, "getPrimaryKeysQuery");
        try {
            const r = await db.autoSQL(TABLE, [{ id: 2, code: "A", val: 20 }]);
            expect(r.success).toBe(true);
            expect(uniqueIdxSpy).not.toHaveBeenCalled();
            expect(pkSpy).not.toHaveBeenCalled();
            expect(await rowCount()).toBe(2);
            expect(await uniqueIndexNames()).not.toContain("uq_code");
        } finally {
            uniqueIdxSpy.mockRestore();
            pkSpy.mockRestore();
            await db.closeConnection();
        }
    });
});
