import type { QueryInput, supportedDialects } from "../../../config/types";
import { escapeIdentifier } from "../../utils/escape";

// Shared across all three dialects — `ADD PRIMARY KEY (...)` has no dialect variance.
export function getAddPrimaryKeyQuery(dialect: supportedDialects, table: string, primaryKeys: string[], schema?: string): QueryInput {
    const q = (name: string) => escapeIdentifier(name, dialect);
    const schemaPrefix = schema ? `${q(schema)}.` : "";
    return {
        query: `ALTER TABLE ${schemaPrefix}${q(table)} ADD PRIMARY KEY (${primaryKeys.map(pk => q(pk)).join(", ")});`,
        params: []
    };
}
