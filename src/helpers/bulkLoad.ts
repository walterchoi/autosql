/**
 * Serialization for the bulk-load path (Postgres `COPY` / MySQL `LOAD DATA LOCAL INFILE`).
 *
 * Both dialects can consume the same tab-delimited text format: fields separated by TAB, rows by
 * newline, `\N` for NULL, and backslash-escaping of the specials (`\` `\t` `\n` `\r`). The values are
 * already the dialect-sqlized values (the same ones the parameterised INSERT path binds), so numbers,
 * booleans (0/1) and normalised dates are load-ready — this layer only handles the text framing.
 */

/** Escape one already-sqlized value for the COPY / LOAD DATA text format. */
export function escapeCopyValue(value: any): string {
    if (value === null || value === undefined) return "\\N";
    const s = typeof value === "string" ? value : String(value);
    return s
        .replace(/\\/g, "\\\\")
        .replace(/\t/g, "\\t")
        .replace(/\n/g, "\\n")
        .replace(/\r/g, "\\r");
}

/** Serialize rows (arrays of sqlized values, aligned to the column list) into the load body. */
export function serializeRowsToCopyText(rows: any[][]): string {
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
