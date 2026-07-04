import { supportedDialects } from "../../config/types";

/**
 * SQL identifier / literal escaping helpers.
 *
 * autosql builds DDL and DML by string interpolation, and its identifiers come from
 * arbitrary caller data (JSON keys become column names, `config.metaData` supplies
 * table/column/type/length). Data *values* are parameter-bound by the drivers and are
 * safe, but identifiers, type tokens and lengths are interpolated into the statement
 * text and must be escaped/validated at generation time. These helpers are the single
 * place that does that — every builder routes its identifiers through `escapeIdentifier`.
 *
 * For a well-formed identifier (letters/digits/underscore) the output is byte-identical
 * to the previous hand-written quoting, so escaping is transparent to existing callers.
 */

const IDENTIFIER_QUOTE: Record<supportedDialects, string> = {
    mysql: "`",
    pgsql: '"',
};

/**
 * Wrap a SQL identifier (table / column / schema / index / constraint name) in the
 * dialect's quote character, doubling any embedded quote character (the SQL-standard
 * escape). This closes identifier-injection via attacker-controlled names such as a JSON
 * key like `` a`, ADD COLUMN evil TEXT, ADD COLUMN `b `` (MySQL) or `x" ... --` (Postgres).
 *
 * Throws on an empty/non-string identifier or one containing a NUL byte (neither dialect
 * permits NUL in an identifier, and it can truncate the surrounding statement).
 */
export function escapeIdentifier(name: string, dialect: supportedDialects): string {
    if (typeof name !== "string" || name.length === 0) {
        throw new Error(`Invalid SQL identifier: expected a non-empty string, received ${JSON.stringify(name)}`);
    }
    if (name.includes("\0")) {
        throw new Error(`Invalid SQL identifier: NUL byte is not permitted (${JSON.stringify(name)})`);
    }
    const quote = IDENTIFIER_QUOTE[dialect];
    return `${quote}${name.split(quote).join(quote + quote)}${quote}`;
}

/**
 * Escape a scalar as a single-quoted SQL string literal, for the rare cases where a value
 * must be inlined into statement text (e.g. a column DEFAULT) and cannot be
 * parameter-bound. Doubles single quotes for both dialects; additionally escapes
 * backslashes for MySQL (which treats `\` as an escape character by default). Postgres,
 * with `standard_conforming_strings` on (the default), treats `\` literally, so it is
 * left untouched there.
 */
export function escapeLiteral(value: string | number | boolean, dialect: supportedDialects): string {
    const asString = String(value);
    if (asString.includes("\0")) {
        throw new Error(`Invalid SQL literal: NUL byte is not permitted`);
    }
    let escaped = asString.split("'").join("''");
    if (dialect === "mysql") {
        escaped = escaped.split("\\").join("\\\\");
    }
    return `'${escaped}'`;
}

const SAFE_TYPE_TOKEN = /^[a-z][a-z0-9_ ]*$/i;

/**
 * Validate a SQL type token (after local→server translation) before it is interpolated
 * into a column definition or a `USING`/`CAST` expression. Server type names can contain
 * spaces (`double precision`, `timestamp with time zone`, `character varying`), so those
 * are allowed, but quotes, parentheses, semicolons and other punctuation are rejected —
 * lengths are appended separately, so a legitimate type token never needs them. Returns
 * the validated token unchanged.
 */
export function assertSafeTypeToken(type: string): string {
    if (typeof type !== "string" || !SAFE_TYPE_TOKEN.test(type)) {
        throw new Error(`Invalid SQL column type: ${JSON.stringify(type)}`);
    }
    return type;
}

/**
 * Validate a length / precision / scale value before it is interpolated into a type
 * specifier such as `varchar(N)` or `decimal(P,S)`. Guards against a non-numeric value
 * (e.g. a runtime caller passing `"255) NOT NULL, ADD COLUMN ..."`) being spliced into the
 * statement. Returns the value as a non-negative integer.
 */
export function assertSafeLength(value: number, label = "length"): number {
    const n = typeof value === "number" ? value : Number(value);
    if (!Number.isInteger(n) || n < 0) {
        throw new Error(`Invalid SQL ${label}: expected a non-negative integer, received ${JSON.stringify(value)}`);
    }
    return n;
}
