import { collectDataColumns, schemaCoversColumns, overlaySchema, fillColumnDefaults } from "../src/helpers/metadata";
import { MetadataHeader } from "../src/config/types";

// A-4 provided-schema fast path — pure helpers that let AutoSQL skip inference when the caller
// supplies the schema.

describe("assumeSchema helpers", () => {
    test("collectDataColumns unions keys across all rows", () => {
        expect(collectDataColumns([{ a: 1, b: 2 }, { a: 1, c: 3 }])).toEqual(new Set(["a", "b", "c"]));
    });

    test("schemaCoversColumns is true only when every data column is declared", () => {
        const schema = { a: { type: "int" }, b: { type: "varchar" } } as MetadataHeader;
        expect(schemaCoversColumns(schema, new Set(["a", "b"]))).toBe(true);
        expect(schemaCoversColumns(schema, new Set(["a"]))).toBe(true);
        expect(schemaCoversColumns(schema, new Set(["a", "c"]))).toBe(false);
    });

    test("overlaySchema — provided definition wins, inferred fills the rest", () => {
        const inferred = { a: { type: "boolean" }, b: { type: "int" } } as MetadataHeader;
        const provided = { a: { type: "bigint" } } as MetadataHeader;
        const r = overlaySchema(inferred, provided);
        expect(r.a.type).toBe("bigint"); // provided wins (kills the 0/1 boolean trap)
        expect(r.b.type).toBe("int");    // inferred column kept
    });

    test("fillColumnDefaults fills sparse fields and defaults varchar length", () => {
        const r = fillColumnDefaults({
            name: { type: "varchar" },
            id: { type: "bigint", primary: true },
        } as MetadataHeader);
        expect(r.name).toMatchObject({ type: "varchar", length: 255, allowNull: false, primary: false, decimal: 0 });
        expect(r.id).toMatchObject({ type: "bigint", primary: true, allowNull: false });
    });

    test("fillColumnDefaults keeps an explicitly provided length", () => {
        const r = fillColumnDefaults({ name: { type: "varchar", length: 100 } } as MetadataHeader);
        expect(r.name.length).toBe(100);
    });

    test("fillColumnDefaults throws when a column is missing a type", () => {
        expect(() => fillColumnDefaults({ x: {} as any })).toThrow(/missing a required "type"/);
    });
});
