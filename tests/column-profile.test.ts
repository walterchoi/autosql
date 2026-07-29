import { buildColumnProfile } from "../src/helpers/columnProfile";
import { MetadataHeader } from "../src/config/types";

// Physical column profile — the semantic-layer generator's input (doc 06). Pure derivation from the
// resolved metadata; no business meaning, no DB.

const col = (o: Partial<any> = {}) => ({ type: "varchar", allowNull: false, ...o });

describe("buildColumnProfile", () => {
    test("classifies keys, dimensions, measures, flags, times and identifiers", () => {
        const meta: MetadataHeader = {
            id: col({ type: "int", primary: true }),
            customer_id: col({ type: "int", index: true }),
            region: col({ type: "varchar", categorical: true }),
            revenue: col({ type: "decimal" }),
            is_active: col({ type: "boolean" }),
            created_at: col({ type: "datetime" }),
            free_text: col({ type: "text" }),
        } as any;

        const profile = Object.fromEntries(buildColumnProfile(meta).map(p => [p.column, p]));

        expect(profile.id).toMatchObject({ role: "primary", semanticHint: "identifier" });
        expect(profile.customer_id.semanticHint).toBe("identifier");     // *_id is a join key, not a measure
        expect(profile.customer_id.foreignKey).toEqual({ referencesEntity: "customer" });
        expect(profile.region).toMatchObject({ cardinality: "categorical", semanticHint: "dimension" });
        expect(profile.revenue.semanticHint).toBe("measure");            // high-cardinality numeric, not a key
        expect(profile.is_active.semanticHint).toBe("flag");
        expect(profile.created_at.semanticHint).toBe("time");
        expect(profile.free_text.semanticHint).toBe("attribute");
    });

    test("matches a foreign-key candidate to a sibling table (name-based, singular/plural)", () => {
        const meta: MetadataHeader = { order_id: col({ type: "int" }) } as any;
        const others: Record<string, MetadataHeader> = {
            orders: { id: col({ type: "int", primary: true }) } as any,
        };
        const [p] = buildColumnProfile(meta, others);
        expect(p.foreignKey).toEqual({ referencesEntity: "order", matchedTable: "orders" });
    });

    test("reports cardinality and nullability from the metadata flags", () => {
        const meta: MetadataHeader = {
            email: col({ type: "varchar", unique: true, allowNull: true }),
            status: col({ type: "varchar", pseudounique: true }),
        } as any;
        const profile = Object.fromEntries(buildColumnProfile(meta).map(p => [p.column, p]));
        expect(profile.email).toMatchObject({ role: "unique", cardinality: "unique", nullable: true, semanticHint: "identifier" });
        expect(profile.status.cardinality).toBe("pseudounique");
    });
});
