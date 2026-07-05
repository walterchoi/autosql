import { buildCompensatingDDL } from "../src/helpers/compensatingDDL";
import { mysqlConfig } from "../src/db/config/mysqlConfig";
import { escapeIdentifier } from "../src/db/utils/escape";
import { AlterTableChanges, MetadataHeader, QueryInput } from "../src/config/types";

const sql = (q: QueryInput): string => (typeof q === "string" ? q : q.query);

// H3: the MySQL compensating-DDL builder interpolated column names (inferred from arbitrary
// caller JSON keys) with bare backticks and no quote-doubling — the same identifier break-out
// class as H1, reached whenever a MySQL DDL transaction fails (configureTables rollback).

describe("compensating DDL escapes identifiers (H3)", () => {
    const evil = "a`, DROP COLUMN x, ADD COLUMN evil INT-- ";

    test("malicious column names in rename/modify/add are quote-escaped and validated", () => {
        const changes: AlterTableChanges = {
            addColumns: { [evil]: { type: "int" } },
            modifyColumns: { [evil]: { type: "varchar", length: 10, previousType: "int" } },
            dropColumns: [],
            renameColumns: [{ oldName: "old_col", newName: evil }],
            nullableColumns: [],
            noLongerUnique: [],
            primaryKeyChanges: [],
        };
        const meta: MetadataHeader = { [evil]: { type: "int" }, old_col: { type: "int" } };

        const { queries } = buildCompensatingDDL("t", changes, meta, mysqlConfig, "s");
        expect(queries.length).toBeGreaterThan(0);

        const esc = escapeIdentifier(evil, "mysql"); // backtick-doubled, wrapped
        for (const q of queries) {
            const stmt = sql(q);
            expect(stmt).toContain(esc); // identifier is fully escaped
            // the un-escaped break-out (single backtick closing the identifier) never appears
            expect(stmt).not.toContain("`a`, DROP COLUMN x");
        }
    });
});
