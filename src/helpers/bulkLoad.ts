/**
 * Serialization for the bulk-load path (Postgres `COPY` / MySQL `LOAD DATA LOCAL INFILE`).
 *
 * Both dialects consume the same tab-delimited text format, and values arrive already
 * dialect-sqlized, so this layer only handles the text framing.
 */

/** Escape one already-sqlized value for the COPY / LOAD DATA text format. */
export function escapeCopyValue(value: unknown): string {
    if (value === null || value === undefined) return "\\N";
    let s: string;
    if (typeof value === "string") s = value;
    else if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") s = String(value);
    else throw new Error(`escapeCopyValue: unsupported value type "${typeof value}" for bulk load — values must already be sqlized to a string/number/boolean/bigint/null.`);
    return s
        .replace(/\\/g, "\\\\")
        .replace(/\t/g, "\\t")
        .replace(/\n/g, "\\n")
        .replace(/\r/g, "\\r");
}

/** Serialize rows (arrays of sqlized values, aligned to the column list) into the load body. */
export function serializeRowsToCopyText(rows: unknown[][]): string {
    let out = "";
    for (const row of rows) {
        for (let i = 0; i < row.length; i++) {
            if (i > 0) out += "\t";
            out += escapeCopyValue(row[i]);
        }
        out += "\n";
    }
    return out;
}
