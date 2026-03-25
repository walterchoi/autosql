import { buildCompensatingDDL } from "../src/helpers/compensatingDDL";
import { AlterTableChanges, MetadataHeader, QueryWithParams } from "../src/config/types";
import { mysqlConfig } from "../src/db/config/mysqlConfig";
import { pgsqlConfig } from "../src/db/config/pgsqlConfig";

// All queries emitted by buildCompensatingDDL are always QueryWithParams objects.
const asQWP = (q: any) => q as QueryWithParams;

const EMPTY_CHANGES: AlterTableChanges = {
    addColumns: {},
    modifyColumns: {},
    dropColumns: [],
    renameColumns: [],
    nullableColumns: [],
    noLongerUnique: [],
    primaryKeyChanges: [],
};

const EMPTY_META: MetadataHeader = {};

describe("buildCompensatingDDL — MySQL", () => {
    test("emits DROP COLUMN IF EXISTS for each added column", () => {
        const changes: AlterTableChanges = {
            ...EMPTY_CHANGES,
            addColumns: {
                phone: { type: "varchar", length: 20 },
                notes: { type: "text" },
            },
        };
        const { queries, warnings } = buildCompensatingDDL("users", changes, EMPTY_META, mysqlConfig);
        expect(warnings).toHaveLength(0);
        expect(queries).toHaveLength(1);
        expect(asQWP(queries[0]).query).toMatch(/DROP COLUMN IF EXISTS `phone`/);
        expect(asQWP(queries[0]).query).toMatch(/DROP COLUMN IF EXISTS `notes`/);
        expect(asQWP(queries[0]).query).toMatch(/ALTER TABLE `users`/);
    });

    test("emits MODIFY COLUMN back to previousType for modified columns", () => {
        const changes: AlterTableChanges = {
            ...EMPTY_CHANGES,
            modifyColumns: {
                age: { type: "varchar", previousType: "int", length: 10, allowNull: false },
            },
        };
        const { queries, warnings } = buildCompensatingDDL("users", changes, EMPTY_META, mysqlConfig);
        expect(warnings).toHaveLength(0);
        expect(queries).toHaveLength(1);
        expect(asQWP(queries[0]).query).toMatch(/MODIFY COLUMN `age` int\(10\) NOT NULL/);
    });

    test("skips MODIFY COLUMN when type has not changed", () => {
        const changes: AlterTableChanges = {
            ...EMPTY_CHANGES,
            modifyColumns: {
                name: { type: "varchar", previousType: "varchar", length: 255, allowNull: true },
            },
        };
        const { queries } = buildCompensatingDDL("users", changes, EMPTY_META, mysqlConfig);
        expect(queries).toHaveLength(0);
    });

    test("emits CHANGE COLUMN to reverse a rename", () => {
        const updatedMeta: MetadataHeader = {
            full_name: { type: "varchar", length: 100, allowNull: false },
        };
        const changes: AlterTableChanges = {
            ...EMPTY_CHANGES,
            renameColumns: [{ oldName: "name", newName: "full_name" }],
        };
        const { queries, warnings } = buildCompensatingDDL("users", changes, updatedMeta, mysqlConfig);
        expect(warnings).toHaveLength(0);
        expect(queries).toHaveLength(1);
        expect(asQWP(queries[0]).query).toMatch(/CHANGE COLUMN `full_name` `name` varchar\(100\) NOT NULL/);
    });

    test("emits warning for dropped columns (cannot recover)", () => {
        const changes: AlterTableChanges = {
            ...EMPTY_CHANGES,
            dropColumns: ["old_col", "another_col"],
        };
        const { queries, warnings } = buildCompensatingDDL("users", changes, EMPTY_META, mysqlConfig);
        expect(queries).toHaveLength(0);
        expect(warnings).toHaveLength(1);
        expect(warnings[0]).toMatch(/old_col.*another_col/);
        expect(warnings[0]).toMatch(/permanently lost/);
    });

    test("emits warning for nullable columns (NOT NULL cannot be safely restored)", () => {
        const changes: AlterTableChanges = {
            ...EMPTY_CHANGES,
            nullableColumns: ["email"],
        };
        const { queries, warnings } = buildCompensatingDDL("users", changes, EMPTY_META, mysqlConfig);
        expect(queries).toHaveLength(0);
        expect(warnings).toHaveLength(1);
        expect(warnings[0]).toMatch(/email/);
        expect(warnings[0]).toMatch(/NOT NULL/);
    });

    test("includes schema prefix in generated queries", () => {
        const changes: AlterTableChanges = {
            ...EMPTY_CHANGES,
            addColumns: { status: { type: "varchar", length: 50 } },
        };
        const { queries } = buildCompensatingDDL("orders", changes, EMPTY_META, mysqlConfig, "mydb");
        expect(asQWP(queries[0]).query).toMatch(/ALTER TABLE `mydb`\.`orders`/);
    });

    test("applies noLength rules — text type gets no length", () => {
        const changes: AlterTableChanges = {
            ...EMPTY_CHANGES,
            modifyColumns: {
                body: { type: "varchar", previousType: "text", length: 5000, allowNull: true },
            },
        };
        const { queries } = buildCompensatingDDL("posts", changes, EMPTY_META, mysqlConfig);
        // text is in noLength — should NOT have (5000) appended
        expect(asQWP(queries[0]).query).not.toMatch(/text\(\d+\)/);
        expect(asQWP(queries[0]).query).toMatch(/MODIFY COLUMN `body` text NULL/);
    });

    test("handles multiple change types in one call", () => {
        const updatedMeta: MetadataHeader = {
            new_name: { type: "varchar", length: 50, allowNull: false },
        };
        const changes: AlterTableChanges = {
            ...EMPTY_CHANGES,
            addColumns: { created_at: { type: "datetime" } },
            modifyColumns: {
                score: { type: "varchar", previousType: "int", length: 10, allowNull: false },
            },
            renameColumns: [{ oldName: "old_name", newName: "new_name" }],
            dropColumns: ["legacy"],
        };
        const { queries, warnings } = buildCompensatingDDL("events", changes, updatedMeta, mysqlConfig);
        // rename + modify + drop(add cols)
        expect(queries.length).toBeGreaterThanOrEqual(3);
        expect(warnings).toHaveLength(1); // dropColumns warning
    });

    test("all generated queries have empty params array", () => {
        const changes: AlterTableChanges = {
            ...EMPTY_CHANGES,
            addColumns: { tag: { type: "varchar", length: 30 } },
            modifyColumns: {
                count: { type: "varchar", previousType: "int", length: 5, allowNull: false },
            },
        };
        const { queries } = buildCompensatingDDL("items", changes, EMPTY_META, mysqlConfig);
        for (const q of queries) {
            expect(asQWP(q).params).toEqual([]);
        }
    });
});

describe("buildCompensatingDDL — PostgreSQL", () => {
    test("returns no queries for PostgreSQL (transactional DDL handles rollback)", () => {
        const changes: AlterTableChanges = {
            ...EMPTY_CHANGES,
            addColumns: { col: { type: "varchar", length: 50 } },
            modifyColumns: {
                score: { type: "varchar", previousType: "int", length: 10, allowNull: false },
            },
            renameColumns: [{ oldName: "a", newName: "b" }],
        };
        const { queries, warnings } = buildCompensatingDDL("tbl", changes, EMPTY_META, pgsqlConfig);
        expect(queries).toHaveLength(0);
    });

    test("still emits warnings for dropped columns even on PostgreSQL", () => {
        const changes: AlterTableChanges = {
            ...EMPTY_CHANGES,
            dropColumns: ["gone"],
        };
        const { queries, warnings } = buildCompensatingDDL("tbl", changes, EMPTY_META, pgsqlConfig);
        expect(queries).toHaveLength(0);
        expect(warnings).toHaveLength(1);
        expect(warnings[0]).toMatch(/gone/);
    });

    test("still emits warnings for nullable columns on PostgreSQL", () => {
        const changes: AlterTableChanges = {
            ...EMPTY_CHANGES,
            nullableColumns: ["required_field"],
        };
        const { queries, warnings } = buildCompensatingDDL("tbl", changes, EMPTY_META, pgsqlConfig);
        expect(queries).toHaveLength(0);
        expect(warnings).toHaveLength(1);
    });

    test("no queries and no warnings for an empty change set", () => {
        const { queries, warnings } = buildCompensatingDDL("tbl", EMPTY_CHANGES, EMPTY_META, pgsqlConfig);
        expect(queries).toHaveLength(0);
        expect(warnings).toHaveLength(0);
    });
});
