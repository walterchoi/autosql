import { PostgresTableQueryBuilder } from "../src/db/queryBuilders/pgsql/tableBuilder";
import { AlterTableChanges, DatabaseConfig } from "../src/config/types";

// R1: the Postgres modify path comma-joined multiple sub-actions under a single `ALTER COLUMN`
// prefix (`... SET DATA TYPE x, SET DEFAULT y`), which is a syntax error at the bare second
// action — it broke widening an existing column on re-ingest. Each action must be its own
// `ALTER COLUMN`, and a degenerate null default (an introspection artefact) must not be emitted.

const emptyChanges = (): AlterTableChanges => ({
    addColumns: {}, modifyColumns: {}, dropColumns: [], renameColumns: [],
    nullableColumns: [], noLongerUnique: [], primaryKeyChanges: [],
});
const cfg = { sqlDialect: "pgsql" } as DatabaseConfig;
const sqlOf = (changes: AlterTableChanges) => {
    const q = PostgresTableQueryBuilder.getAlterTableQuery("t", changes, "test_schema", cfg);
    return (q[0] as { query: string }).query;
};

describe("Postgres ALTER COLUMN generation (R1)", () => {
    test("widening a column emits separate ALTER COLUMN actions, no bare SET/DROP", () => {
        const changes = emptyChanges();
        changes.modifyColumns = {
            note: { type: "varchar", length: 40, allowNull: true, default: null, previousType: "varchar" },
        };
        const sql = sqlOf(changes);
        expect(sql).toContain('ALTER COLUMN "note" SET DATA TYPE varchar(40)');
        expect(sql).toContain('ALTER COLUMN "note" DROP NOT NULL');
        // No sub-action left bare after a comma (the syntax error), and no degenerate null default.
        expect(sql).not.toMatch(/,\s*(SET DEFAULT|DROP NOT NULL|SET DATA TYPE)/);
        expect(sql).not.toContain("SET DEFAULT NULL");
    });

    test("a genuine (non-null) default is still emitted as its own ALTER COLUMN action", () => {
        const changes = emptyChanges();
        changes.modifyColumns = {
            status: { type: "varchar", length: 20, allowNull: true, default: "'active'::character varying" },
        };
        const sql = sqlOf(changes);
        expect(sql).toContain(`ALTER COLUMN "status" SET DEFAULT 'active'::character varying`);
        expect(sql).not.toMatch(/,\s*SET DEFAULT/); // its own action, not comma-appended
    });
});
