import type { QueryInput, supportedDialects } from "../../../config/types";
import { escapeIdentifier } from "../../utils/escape";
import { generateSafeConstraintName } from "../../../helpers/utilities";

// One CREATE INDEX per flagged column, shared across all three dialects. `schemaPrefix` is
// passed in rather than computed here because each dialect's default differs (sqlserver always
// defaults to "dbo"; mysql/pgsql default to no prefix) — that belongs to the caller, not here.
export function buildCreateIndexQueries(dialect: supportedDialects, table: string, indexes: string[], remainingIndexSlots: number, schemaPrefix: string): QueryInput[] {
    const q = (name: string) => escapeIdentifier(name, dialect);
    const queries: QueryInput[] = [];
    const limitedIndexes = indexes.slice(0, remainingIndexSlots);
    for (const index of limitedIndexes) {
        if (!index) continue;
        const indexName = generateSafeConstraintName(table, index, "index");
        queries.push({
            query: `CREATE INDEX ${q(indexName)} ON ${schemaPrefix}${q(table)} (${q(index)});`,
            params: []
        });
    }
    return queries;
}
