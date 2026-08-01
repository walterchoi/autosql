import { DB_CONFIG, Database } from "./utils/testConfig";
import { escapeIdentifier } from "../src/db/utils/escape";

// Live proof for the resolveConflicts metadata-derivation win (D-J follow-up): on a stable-schema
// staging load, resolveConflicts derives the drop-target constraint structure from already-known
// metadata and SKIPS the unique-index/primary-key introspection round-trip — while still dropping
// the *right* constraint by its real database name.
//
// The scenario: a table with a PK and a SECONDARY unique constraint whose name is chosen by the DB
// (uq_code). New data collides on that unique (same code, different PK), so the staging merge would
// violate it; resolveConflicts must DROP uq_code so the merge proceeds. This is the exact case that
// would break if derivation synthesised the name instead of using the introspected one — MySQL
// auto-names a unique after its column, and getDropUniqueConstraintQuery drops by real name — so a
// green result proves the derived path targets the true constraint.

Object.values(DB_CONFIG).forEach((config) => {
    const qi = (n: string) => escapeIdentifier(n, config.sqlDialect);

    describe(`resolveConflicts metadata derivation (live) for ${config.sqlDialect.toUpperCase()}`, () => {
        const TABLE = "resolve_derivation_test";
        const ref = `${qi("test_schema")}.${qi(TABLE)}`;
        const tempRef = `${qi("test_schema")}.${qi("temp_staging__" + TABLE)}`;

        const baseConfig = { ...config, schema: "test_schema", useWorkers: false, addTimestamps: false };
        let admin: Database;

        beforeAll(async () => {
            admin = Database.create(baseConfig);
            await admin.establishConnection();
        });
        afterAll(async () => {
            await dropAll();
            await admin.closeConnection();
        });
        beforeEach(async () => {
            await dropAll();
            // PK id + a DB-named secondary unique on `code`.
            await admin.runQuery({
                query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("code")} VARCHAR(20), ${qi("val")} INT, CONSTRAINT ${qi("uq_code")} UNIQUE (${qi("code")}))`,
                params: [],
            });
            await admin.runQuery({
                query: `INSERT INTO ${ref} (${qi("id")}, ${qi("code")}, ${qi("val")}) VALUES (1, 'A', 10)`,
                params: [],
            });
        });

        async function dropAll() {
            await admin.runQuery({ query: `DROP TABLE IF EXISTS ${tempRef}`, params: [] }).catch(() => {});
            await admin.runQuery({ query: `DROP TABLE IF EXISTS ${ref}`, params: [] }).catch(() => {});
        }

        const rowCount = async () => {
            const r = await admin.runQuery({ query: `SELECT COUNT(*) AS c FROM ${ref}`, params: [] });
            return Number(Object.values(r.results![0])[0]);
        };
        const uniqueIndexNames = async () => {
            const r = await admin.runQuery(admin.getUniqueIndexesQuery(TABLE));
            return (r.results ?? []).map((row: any) => String(row.index_name ?? row.INDEX_NAME));
        };

        test("derives the constraint structure (no introspection round-trip) and drops the right unique by its real name", async () => {
            const db = Database.create(baseConfig); // default useStagingInsert:true (the staging/merge path)
            await db.establishConnection();
            // Spy on the introspection the derived path must AVOID.
            const uniqueIdxSpy = jest.spyOn(db, "getUniqueIndexesQuery");
            const pkSpy = jest.spyOn(db, "getPrimaryKeysQuery");
            try {
                // New row collides with the seed on `code` ('A') but is a new PK (id=2). Merging it
                // would violate uq_code, so resolveConflicts must drop uq_code and let the merge land.
                const r = await db.autoSQL(TABLE, [{ id: 2, code: "A", val: 20 }]);
                expect(r.success).toBe(true);

                // The introspection round-trip was skipped — the structure came from metadata.
                expect(uniqueIdxSpy).not.toHaveBeenCalled();
                expect(pkSpy).not.toHaveBeenCalled();

                // The merge landed both rows, and the real constraint (uq_code) was the one dropped.
                expect(await rowCount()).toBe(2);
                expect(await uniqueIndexNames()).not.toContain("uq_code");
            } finally {
                uniqueIdxSpy.mockRestore();
                pkSpy.mockRestore();
                await db.closeConnection();
            }
        });

        test("falls back to live introspection when a column is in multiple unique indexes (no over-drop)", async () => {
            // `code` participates in TWO non-primary uniques: uq_code(code) and uq_code_val(code,val).
            // The per-column single-name model can't group them unambiguously, so derivation must bail
            // to live introspection rather than risk mis-scoping a drop.
            await dropAll();
            await admin.runQuery({
                query: `CREATE TABLE ${ref} (${qi("id")} INT PRIMARY KEY, ${qi("code")} VARCHAR(20), ${qi("val")} INT, `
                    + `CONSTRAINT ${qi("uq_code")} UNIQUE (${qi("code")}), CONSTRAINT ${qi("uq_code_val")} UNIQUE (${qi("code")}, ${qi("val")}))`,
                params: [],
            });
            await admin.runQuery({ query: `INSERT INTO ${ref} (${qi("id")}, ${qi("code")}, ${qi("val")}) VALUES (1, 'A', 10)`, params: [] });

            const db = Database.create(baseConfig);
            await db.establishConnection();
            const uniqueIdxSpy = jest.spyOn(db, "getUniqueIndexesQuery");
            try {
                // A benign stable re-ingest (identical row, no violation) — the point is the PATH.
                const r = await db.autoSQL(TABLE, [{ id: 1, code: "A", val: 10 }]);
                expect(r.success).toBe(true);
                expect(uniqueIdxSpy).toHaveBeenCalled(); // fell back — did NOT derive
                expect(await rowCount()).toBe(1);
                // Both uniques survive — nothing was (over-)dropped.
                const names = await uniqueIndexNames();
                expect(names).toContain("uq_code");
                expect(names).toContain("uq_code_val");
            } finally {
                uniqueIdxSpy.mockRestore();
                await db.closeConnection();
            }
        });
    });
});
