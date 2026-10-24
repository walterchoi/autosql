import type { QueryInput, supportedDialects } from "../../../config/types";
import { escapeIdentifier } from "../../utils/escape";
import { generateSafeConstraintName } from "../../../helpers/utilities";

// The "one CREATE INDEX per flagged column" loop at the end of getCreateTableQuery was
// near-identical across all three dialects (mysql/pgsql ~80-87, sqlserver ~113-122) — only the
// dialect each file's local `q` closed over actually varied. `schemaPrefix` is passed in rather
// than computed here because each dialect's default differs (sqlserver always defaults to "dbo";
// mysql/pgsql default to no prefix) — that difference belongs to the caller, not this helper.
export function buildCreateIndexQueries(dialect: supportedDialects, table: string, indexes: string[], remainingIndexSlots: number, schemaPrefix: string): QueryInput[] {
    const q = (name: string) => escapeIdentifier(name, dialect);
    const queries: QueryInput[] = [];
    const limitedIndexes = indexes.slice(0, remainingIndexSlots);
    for (const index of limitedIndexes) {
        if (!index) continue; // Skip empty index names
        const indexName = generateSafeConstraintName(table, index, "index");
        queries.push({
            query: `CREATE INDEX ${q(indexName)} ON ${schemaPrefix}${q(table)} (${q(index)});`,
            params: []
        });
    }
    return queries;
}
