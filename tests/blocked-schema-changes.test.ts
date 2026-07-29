import { Database } from "../src/db/database";
import { AlterTableChanges, DatabaseConfig } from "../src/config/types";

// R9: deleteColumns / updatePrimaryKey default to false (the safe default), so compareMetaData can
// compute a column drop or PK change that is then not executed. That silent no-op must be observable:
// a warning naming the columns and the flag to flip, fired once on the real table only, and never on
// a steady-state re-ingest (no computed change).

const changesWith = (over: Partial<AlterTableChanges>): AlterTableChanges => ({
    addColumns: {}, modifyColumns: {}, dropColumns: [], renameColumns: [],
    nullableColumns: [], noLongerUnique: [], primaryKeyChanges: [], ...over,
});

function makeDb(dialect: "mysql" | "pgsql", extra: Partial<DatabaseConfig>) {
    const warnings: string[] = [];
    const config: DatabaseConfig = {
        sqlDialect: dialect, schema: "test_schema", autoIndexing: false,
        logger: { warn: (m: string) => warnings.push(m) },
        ...extra,
    };
    return { db: Database.create(config), warnings };
}

describe.each(["mysql", "pgsql"] as const)("R9 blocked schema-change warnings (%s)", (dialect) => {
    test("warns when a column drop is computed but deleteColumns is off", async () => {
        const { db, warnings } = makeDb(dialect, { deleteColumns: false });
        await db.getAlterTableQuery("orders", changesWith({ dropColumns: ["legacy_note"] }));
        expect(warnings.some((w) => /would be dropped/.test(w) && /legacy_note/.test(w) && /deleteColumns/.test(w))).toBe(true);
    });

    test("does NOT warn about drops when deleteColumns is on", async () => {
        const { db, warnings } = makeDb(dialect, { deleteColumns: true });
        await db.getAlterTableQuery("orders", changesWith({ dropColumns: ["legacy_note"] }));
        expect(warnings.some((w) => /would be dropped/.test(w))).toBe(false);
    });

    test("warns when a primary-key change is computed but updatePrimaryKey is off", async () => {
        const { db, warnings } = makeDb(dialect, { updatePrimaryKey: false });
        await db.getAlterTableQuery("orders", changesWith({ primaryKeyChanges: ["id", "tenant_id"] }));
        expect(warnings.some((w) => /primary-key change is pending/.test(w) && /updatePrimaryKey/.test(w))).toBe(true);
    });

    test("does NOT warn about PK when updatePrimaryKey is on", async () => {
        const { db, warnings } = makeDb(dialect, { updatePrimaryKey: true });
        await db.getAlterTableQuery("orders", changesWith({ primaryKeyChanges: ["id", "tenant_id"] }));
        expect(warnings.some((w) => /primary-key change is pending/.test(w))).toBe(false);
    });

    test("does NOT warn on a staging temp table (real table only)", async () => {
        const { db, warnings } = makeDb(dialect, { deleteColumns: false, updatePrimaryKey: false });
        await db.getAlterTableQuery("temp_staging__orders", changesWith({ dropColumns: ["legacy_note"], primaryKeyChanges: ["id"] }));
        expect(warnings).toHaveLength(0);
    });

    test("does NOT warn on a steady-state alter with no computed changes", async () => {
        const { db, warnings } = makeDb(dialect, { deleteColumns: false, updatePrimaryKey: false });
        await db.getAlterTableQuery("orders", changesWith({}));
        expect(warnings).toHaveLength(0);
    });
});
