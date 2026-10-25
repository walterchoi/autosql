import type { supportedDialects, DialectConfig } from "../../config/types";

/**
 * SQL identifier / literal escaping helpers.
 *
 * Data values are parameter-bound by the drivers and safe. Identifiers, type tokens and lengths are
 * interpolated into statement text and come from arbitrary caller data (JSON keys, `config.metaData`),
 * so they must be escaped at generation time. This is the single place that does it.
 */

const IDENTIFIER_QUOTE: Record<supportedDialects, string> = {
    mysql: "`",
    pgsql: '"',
    sqlserver: "]", // wraps in [ ... ]; the doubled closing bracket ]] is the escape
};

// Postgres SILENTLY TRUNCATES past its limit, which can collide two distinct names on one table.
// Validate here and fail loudly instead.
const IDENTIFIER_MAX_LENGTH: Record<supportedDialects, number> = {
    mysql: 64,
    pgsql: 63,
    sqlserver: 128,
};

/**
 * Wrap a SQL identifier in the dialect's quote char, doubling any embedded quote.
 *
 * Closes identifier-injection via attacker-controlled names, e.g. a JSON key
 * `` a`, ADD COLUMN evil TEXT, ADD COLUMN `b `` on MySQL. NUL is rejected because it can truncate
 * the statement.
 */
export function escapeIdentifier(name: string, dialect: supportedDialects): string {
    if (typeof name !== "string" || name.length === 0) {
        throw new Error(`Invalid SQL identifier: expected a non-empty string, received ${JSON.stringify(name)}`);
    }
    if (name.includes("\0")) {
        throw new Error(`Invalid SQL identifier: NUL byte is not permitted (${JSON.stringify(name)})`);
    }
    // Each dialect counts its limit differently, so measure it their way or a legal international
    // name is false-rejected: Postgres UTF-8 bytes, MySQL code points, SQL Server UTF-16 units.
    const maxLen = IDENTIFIER_MAX_LENGTH[dialect];
    let length: number;
    let unit: string;
    if (dialect === "pgsql") { length = Buffer.byteLength(name, "utf8"); unit = "bytes"; }
    // eslint-disable-next-line @typescript-eslint/no-misused-spread -- deliberate code-point count, not UTF-16 units.
    else if (dialect === "mysql") { length = [...name].length; unit = "characters"; }
    else { length = name.length; unit = "characters"; }
    if (length > maxLen) {
        throw new Error(`Invalid SQL identifier: "${name}" (${length} ${unit}) exceeds the ${maxLen}-${unit === "bytes" ? "byte" : "character"} identifier limit for ${dialect}; shorten the underlying table/column name (autosql derives staging/history names from it, so it can exceed the limit even when the base name does not).`);
    }
    // Asymmetric pair ([ ]), so it can't use the symmetric doubling below.
    if (dialect === "sqlserver") {
        return `[${name.split("]").join("]]")}]`;
    }
    const quote = IDENTIFIER_QUOTE[dialect];
    return `${quote}${name.split(quote).join(quote + quote)}${quote}`;
}

/**
 * Escape a scalar as a single-quoted SQL string literal, for the rare value that must be inlined
 * into statement text (e.g. a column DEFAULT) and can't be parameter-bound.
 *
 * Backslashes are escaped for MySQL only: Postgres with `standard_conforming_strings` on (default)
 * treats `\` literally.
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
 * Validate a SQL type token before it is interpolated into a column definition or a `USING`/`CAST`
 * expression. Spaces are allowed (`double precision`); punctuation is not, since lengths are
 * appended separately and a legitimate type token never needs it.
 */
export function assertSafeTypeToken(type: string): string {
    if (typeof type !== "string" || !SAFE_TYPE_TOKEN.test(type)) {
        throw new Error(`Invalid SQL column type: ${JSON.stringify(type)}`);
    }
    return type;
}

/**
 * Validate a length / precision / scale before it is interpolated into a type specifier such as
 * `varchar(N)`. Guards against a caller splicing in something like `"255) NOT NULL, ADD COLUMN ..."`.
 */
export function assertSafeLength(value: number, label = "length"): number {
    const n = typeof value === "number" ? value : Number(value);
    if (!Number.isInteger(n) || n < 0) {
        throw new Error(`Invalid SQL ${label}: expected a non-negative integer, received ${JSON.stringify(value)}`);
    }
    return n;
}

/**
 * Reject a column DEFAULT that could terminate the column definition and inject further DDL.
 *
 * Defaults are emitted verbatim (autosql's convention is a bare SQL expression), so a statement
 * separator, comment introducer, comma, NUL, or unbalanced quote has to be refused here.
 */
export function assertSafeDefaultExpression(expr: string): void {
    if (/[;,\0]|--|\/\*|\*\//.test(expr)) {
        throw new Error(`Unsafe SQL DEFAULT expression: ${JSON.stringify(expr)}`);
    }
    if (((expr.match(/'/g) || []).length) % 2 !== 0) {
        throw new Error(`Unsafe SQL DEFAULT expression (unbalanced quote): ${JSON.stringify(expr)}`);
    }
}

/**
 * Render a column DEFAULT for a DDL statement. Defaults are SQL expressions, not string literals, so
 * a string-literal default must be self-quoted by the caller (e.g. `"'active'"`).
 */
export function renderColumnDefault(value: unknown, dialectConfig: DialectConfig): string {
    if (typeof value === "string" && dialectConfig.defaultTranslation[value] !== undefined) {
        return dialectConfig.defaultTranslation[value];
    }
    if (value === null || value === undefined) return "NULL";
    if (typeof value === "boolean" || typeof value === "number") return String(value);
    if (typeof value !== "string") {
        throw new Error(`Unsupported column default value of type '${typeof value}'; expected a string, number, boolean, null, or undefined.`);
    }
    assertSafeDefaultExpression(value);
    return value;
}
