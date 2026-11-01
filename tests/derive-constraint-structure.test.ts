import { StagingPipeline } from "../src/db/stagingPipeline";
import type { InsertInput, AlterTableChanges, MetadataHeader } from "../src/config/types";

// deriveConstraintStructure: derives the unique/primary-key structure from already-known metadata
// to skip a live-introspection round-trip, but must bail to null (forcing introspection) on ANY
// of 5 conditions where the derived structure could be wrong. Only 2 of the 5 had live-test
// coverage (resolve-conflicts-derivation-*-live.test.ts); this covers all 5 directly, pure/no DB.

// The method under test never touches `this.db`/`this.handler` — only `input` — so a bare
// instance with no real db/handler is enough.
const pipeline = new StagingPipeline({} as any, {} as any) as any;

const NO_CHANGES: AlterTableChanges = {
    addColumns: {}, modifyColumns: {}, dropColumns: [], renameColumns: [],
    nullableColumns: [], noLongerUnique: [], primaryKeyChanges: [],
};

function makeInput(meta: MetadataHeader, changesOverrides: Partial<AlterTableChanges> = {}): InsertInput {
    const changes: AlterTableChanges = { ...NO_CHANGES, ...changesOverrides };
    return {
        table: "t", data: [], metaData: meta, previousMetaData: null,
        comparedMetaData: { changes, updatedMetaData: meta },
    };
}

describe("deriveConstraintStructure — bail conditions", () => {
    test("no input at all: bails", () => {
        expect(pipeline.deriveConstraintStructure(undefined)).toBeNull();
    });

    test("no comparedMetaData.changes at all: bails", () => {
        const input: InsertInput = { table: "t", data: [], metaData: {}, previousMetaData: null, comparedMetaData: undefined };
        expect(pipeline.deriveConstraintStructure(input)).toBeNull();
    });

    test("noLongerUnique is non-empty: bails (a dropped unique needs live introspection to find its real name)", () => {
        const input = makeInput({ id: { type: "int", primary: true } }, { noLongerUnique: ["code"] });
        expect(pipeline.deriveConstraintStructure(input)).toBeNull();
    });

    test("primaryKeyChanges is non-empty: bails", () => {
        const input = makeInput({ id: { type: "int", primary: true } }, { primaryKeyChanges: ["id"] });
        expect(pipeline.deriveConstraintStructure(input)).toBeNull();
    });

    test("a newly-added unique column: bails (addColumns entry with unique:true)", () => {
        const input = makeInput(
            { id: { type: "int", primary: true } },
            { addColumns: { code: { type: "varchar", unique: true } } }
        );
        expect(pipeline.deriveConstraintStructure(input)).toBeNull();
    });

    test("a unique column with no real DB index name: bails (just-created/inferred unique)", () => {
        const input = makeInput({
            id: { type: "int", primary: true },
            code: { type: "varchar", unique: true }, // no uniqueName
        });
        expect(pipeline.deriveConstraintStructure(input)).toBeNull();
    });

    test("a column in >1 unique index (comma-joined uniqueName): bails, can't disambiguate composites", () => {
        const input = makeInput({
            id: { type: "int", primary: true },
            code: { type: "varchar", unique: true, uniqueName: "uq_code,uq_code_val" },
        });
        expect(pipeline.deriveConstraintStructure(input)).toBeNull();
    });

    test("none of the bail conditions: derives the structure directly from metadata", () => {
        const input = makeInput({
            id: { type: "int", primary: true },
            code: { type: "varchar", unique: true, uniqueName: "uq_code" },
            val: { type: "int" },
        });
        expect(pipeline.deriveConstraintStructure(input)).toEqual({
            uniques: { uq_code: ["code"] },
            primary: ["id"],
        });
    });

    test("a composite primary key is collected in full", () => {
        const input = makeInput({
            a: { type: "int", primary: true },
            b: { type: "int", primary: true },
        });
        expect(pipeline.deriveConstraintStructure(input)).toEqual({ uniques: {}, primary: ["a", "b"] });
    });

    test("multiple columns sharing the same uniqueName group under one key (composite unique, derivable)", () => {
        const input = makeInput({
            id: { type: "int", primary: true },
            a: { type: "varchar", unique: true, uniqueName: "uq_ab" },
            b: { type: "varchar", unique: true, uniqueName: "uq_ab" },
        });
        expect(pipeline.deriveConstraintStructure(input)).toEqual({
            uniques: { uq_ab: ["a", "b"] },
            primary: ["id"],
        });
    });
});
