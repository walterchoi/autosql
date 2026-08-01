import { Database } from "../src/db/database";
import { InsertInput, MetadataHeader, AlterTableChanges } from "../src/config/types";

// White-box unit tests for the derive-with-fallback gate (AutoSQLHandler.deriveConstraintStructure).
// It may derive the constraint structure from metadata ONLY when doing so is provably identical to
// the live catalog; otherwise it returns null and resolveConflicts introspects live. These lock in
// each fallback trigger so a future change can't silently reintroduce the over-drop hazard.

function handler() {
    const db: any = Database.create({ sqlDialect: "pgsql", host: "localhost", user: "u", password: "p", database: "d" });
    return db.autoSQLHandler as any;
}

const noChanges = (): AlterTableChanges => ({
    addColumns: {}, modifyColumns: {}, dropColumns: [], renameColumns: [],
    nullableColumns: [], noLongerUnique: [], primaryKeyChanges: [],
});

function input(updatedMetaData: MetadataHeader, changes: AlterTableChanges = noChanges()): InsertInput {
    return { table: "t", data: [], previousMetaData: null, metaData: updatedMetaData, comparedMetaData: { changes, updatedMetaData } } as InsertInput;
}

describe("deriveConstraintStructure — derive-with-fallback gate", () => {
    const h = handler();

    test("derives non-primary unique indexes (by real name) + primary key on a stable schema", () => {
        const meta: MetadataHeader = {
            id: { type: "int", primary: true },
            code: { type: "varchar", unique: true, uniqueName: "uq_code" },
            val: { type: "int" },
        };
        expect(h.deriveConstraintStructure(input(meta))).toEqual({
            uniques: { uq_code: ["code"] },
            primary: ["id"],
        });
    });

    test("groups a composite unique's columns under their shared real index name", () => {
        const meta: MetadataHeader = {
            id: { type: "int", primary: true },
            a: { type: "varchar", unique: true, uniqueName: "uq_ab" },
            b: { type: "varchar", unique: true, uniqueName: "uq_ab" },
        };
        expect(h.deriveConstraintStructure(input(meta))).toEqual({
            uniques: { uq_ab: ["a", "b"] },
            primary: ["id"],
        });
    });

    test("includes a bare unique INDEX (uniqueName present even when unique flag is false)", () => {
        const meta: MetadataHeader = {
            id: { type: "int", primary: true },
            code: { type: "varchar", unique: false, uniqueName: "ix_code_u" },
        };
        expect(h.deriveConstraintStructure(input(meta))).toEqual({
            uniques: { ix_code_u: ["code"] },
            primary: ["id"],
        });
    });

    test("falls back (null) when a unique column has no real index name — inferred / just-created", () => {
        const meta: MetadataHeader = {
            id: { type: "int", primary: true },
            code: { type: "varchar", unique: true }, // no uniqueName
        };
        expect(h.deriveConstraintStructure(input(meta))).toBeNull();
    });

    test("falls back when a unique was dropped this run (noLongerUnique)", () => {
        const meta: MetadataHeader = { id: { type: "int", primary: true }, code: { type: "varchar", unique: true, uniqueName: "uq_code" } };
        const changes = { ...noChanges(), noLongerUnique: ["other"] };
        expect(h.deriveConstraintStructure(input(meta, changes))).toBeNull();
    });

    test("falls back when the primary key changed this run (primaryKeyChanges)", () => {
        const meta: MetadataHeader = { id: { type: "int", primary: true }, code: { type: "varchar", unique: true, uniqueName: "uq_code" } };
        const changes = { ...noChanges(), primaryKeyChanges: ["id", "code"] };
        expect(h.deriveConstraintStructure(input(meta, changes))).toBeNull();
    });

    test("falls back when a column is in >1 non-primary unique index (comma-joined uniqueName)", () => {
        // external-table shape: `c` participates in uq_c(c) AND uq_cd(c,d). The single-name model
        // can't group the composite unambiguously, so it must bail rather than mis-scope a drop.
        const meta: MetadataHeader = {
            id: { type: "int", primary: true },
            c: { type: "varchar", unique: true, uniqueName: "uq_c,uq_cd" },
            d: { type: "varchar", unique: false, uniqueName: "uq_cd" },
        };
        expect(h.deriveConstraintStructure(input(meta))).toBeNull();
    });

    test("falls back when a newly-added column is unique (its index isn't introspected yet)", () => {
        const meta: MetadataHeader = { id: { type: "int", primary: true } };
        const changes = { ...noChanges(), addColumns: { email: { type: "varchar", unique: true } } as MetadataHeader };
        expect(h.deriveConstraintStructure(input(meta, changes))).toBeNull();
    });

    test("falls back when there is no compared/resolved metadata to reason about", () => {
        expect(h.deriveConstraintStructure({ table: "t", data: [], previousMetaData: null, metaData: { id: { type: "int", primary: true } } } as unknown as InsertInput)).toBeNull();
        expect(h.deriveConstraintStructure(undefined)).toBeNull();
    });

    test("derives an empty structure (no round-trip) for a PK-only table with no uniques", () => {
        const meta: MetadataHeader = { id: { type: "int", primary: true }, val: { type: "int" } };
        expect(h.deriveConstraintStructure(input(meta))).toEqual({ uniques: {}, primary: ["id"] });
    });
});
