import type { QueryInput, supportedDialects } from "../../../config/types";
import { escapeIdentifier } from "../../utils/escape";

// `ADD PRIMARY KEY (...)` has no dialect variance — this was byte-identical across all three
// indexBuilders (confirmed via MD5 of the whitespace-normalised method body) except for which
// dialect each file's local `q` closed over, so the dialect is now a parameter instead.
export function getAddPrimaryKeyQuery(dialect: supportedDialects, table: string, primaryKeys: string[], schema?: string): QueryInput {
    const q = (name: string) => escapeIdentifier(name, dialect);
    const schemaPrefix = schema ? `${q(schema)}.` : "";
    return {
        query: `ALTER TABLE ${schemaPrefix}${q(table)} ADD PRIMARY KEY (${primaryKeys.map(pk => q(pk)).join(", ")});`,
        params: []
    };
}
