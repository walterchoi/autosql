import { MetadataHeader, ColumnDefinition } from "../config/types";
import { isNumeric, isDate, isBoolean } from "../config/groupings";

/**
 * Physical "column profile" — the input a semantic-layer generator (the platform / Roots, see
 * `prompt/autosql-improvements/06-semantic-layer-osi-placement.md`) uses to *suggest* measures,
 * dimensions and relationships. It is derived purely from AutoSQL's resolved metadata (which already
 * carries type, key and cardinality signals) — it describes the *data*, not its business meaning, and
 * does no DB access. AutoSQL deliberately stops here: turning these physical signals into a semantic
 * model is the platform's job.
 */

export type ColumnRole = "primary" | "unique" | "index" | "none";
export type Cardinality = "single" | "unique" | "pseudounique" | "categorical" | "normal";
/** A hint, not a decision — the platform's semantic generator makes the real call. */
export type SemanticHint = "identifier" | "flag" | "time" | "dimension" | "measure" | "attribute";

export interface ColumnProfile {
    column: string;
    type: string | null;
    role: ColumnRole;
    nullable: boolean;
    cardinality: Cardinality;
    semanticHint: SemanticHint;
    /** Present when the column name looks like a foreign key (`<entity>_id`). `matchedTable` is set
     *  only if a sibling table in `otherTables` plausibly matches the entity name. */
    foreignKey?: { referencesEntity: string; matchedTable?: string };
}

const idLike = (name: string) => /(^id$|_id$)/i.test(name);

function roleOf(col: ColumnDefinition): ColumnRole {
    if (col.primary) return "primary";
    if (col.unique) return "unique";
    if (col.index) return "index";
    return "none";
}

function cardinalityOf(col: ColumnDefinition): Cardinality {
    if (col.singleValue) return "single";
    if (col.unique) return "unique";
    if (col.pseudounique) return "pseudounique";
    if (col.categorical) return "categorical";
    return "normal";
}

function semanticHintOf(name: string, col: ColumnDefinition, role: ColumnRole, cardinality: Cardinality): SemanticHint {
    const type = col.type ?? "";
    // A key, or an `*_id` column, is an identifier / join key — never a measure.
    if (role === "primary" || role === "unique" || idLike(name)) return "identifier";
    if (isBoolean(type)) return "flag";
    if (isDate(type)) return "time";
    // Low-cardinality → a dimension candidate.
    if (cardinality === "categorical" || cardinality === "single") return "dimension";
    // A high-cardinality numeric that isn't a key → a measure candidate.
    if (isNumeric(type)) return "measure";
    return "attribute"; // e.g. high-cardinality free text
}

/**
 * Build the physical profile for a resolved schema. `otherTables` (optional) lets an `<entity>_id`
 * column be matched to a sibling table named like `<entity>` (name-based FK candidacy only —
 * value-based matching is left to the caller / future work).
 */
export function buildColumnProfile(
    metaData: MetadataHeader,
    otherTables?: Record<string, MetadataHeader>
): ColumnProfile[] {
    const tableNames = otherTables ? Object.keys(otherTables) : [];
    return Object.entries(metaData).map(([column, col]) => {
        const role = roleOf(col);
        const cardinality = cardinalityOf(col);
        const profile: ColumnProfile = {
            column,
            type: col.type ?? null,
            role,
            nullable: col.allowNull === true,
            cardinality,
            semanticHint: semanticHintOf(column, col, role, cardinality),
        };
        const m = /^(.*)_id$/i.exec(column);
        if (m && m[1]) {
            const entity = m[1];
            const el = entity.toLowerCase();
            const matchedTable = tableNames.find(t => {
                const tl = t.toLowerCase();
                return tl === el || tl === el + "s" || tl === el + "es"; // simple singular/plural
            });
            profile.foreignKey = matchedTable ? { referencesEntity: entity, matchedTable } : { referencesEntity: entity };
        }
        return profile;
    });
}
