import { PostgresTableQueryBuilder } from "../src/db/queryBuilders/pgsql/tableBuilder";
import { AlterTableChanges, QueryInput } from "../src/config/types";

const sql = (q: QueryInput): string => (typeof q === "string" ? q : q.query);

// M1: Postgres cannot combine RENAME COLUMN with other actions (or another rename) in one
// ALTER TABLE statement — doing so is a syntax error. Each rename must be its own statement.

describe("Postgres ALTER splits RENAME COLUMN into its own statement", () => {
    test("a rename co-occurring with an add is emitted as a separate ALTER TABLE", () => {
        const changes: AlterTableChanges = {
            addColumns: { added_col: { type: "varchar", length: 20 } },
            modifyColumns: {},
            dropColumns: [],
            renameColumns: [
                { oldName: "old_a", newName: "new_a" },
                { oldName: "old_b", newName: "new_b" },
            ],
            nullableColumns: [],
            noLongerUnique: [],
            primaryKeyChanges: [],
        };

        const queries = PostgresTableQueryBuilder.getAlterTableQuery("t", changes, "s").map(sql);

        // each rename is a standalone statement, never combined with a comma / another action
        const renameStmts = queries.filter((q) => q.includes("RENAME COLUMN"));
        expect(renameStmts).toHaveLength(2);
        for (const stmt of renameStmts) {
            expect(stmt).not.toContain(","); // no combined actions / multiple renames
            expect(stmt).not.toContain("ADD COLUMN");
        }
        expect(renameStmts.some((q) => q.includes('RENAME COLUMN "old_a" TO "new_a"'))).toBe(true);
        expect(renameStmts.some((q) => q.includes('RENAME COLUMN "old_b" TO "new_b"'))).toBe(true);

        // the ADD lives in a different (non-rename) statement
        expect(queries.some((q) => q.includes("ADD COLUMN") && !q.includes("RENAME"))).toBe(true);
    });
});
