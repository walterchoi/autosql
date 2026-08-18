import { supportedDialects, DialectConfig } from "../../config/types";

/**
 * SQL identifier / literal escaping helpers.
 *
 * autosql interpolates identifiers/type tokens/lengths into DDL/DML statement text, and they
 * come from arbitrary caller data (JSON keys, `config.metaData`). Data *values* are parameter-
 * bound by the drivers and safe; identifiers etc. are not, so they must be escaped/validated at
 * generation time. This is the single place that does that — every builder routes identifiers
 * through `escapeIdentifier`. For a well-formed identifier the output is byte-identical to the
 * previous hand-written quoting, so escaping is transparent to existing callers.
 */

const IDENTIFIER_QUOTE: Record<supportedDialects, string> = {
    mysql: "`",
    pgsql: '"',
    sqlserver: "]", // SQL Server wraps in [ ... ] with a doubled closing bracket ]] as the escape
};

// Max identifier length per dialect. MySQL: 64 chars, errors past it. Postgres: 63 BYTES
// (NAMEDATALEN-1), SILENTLY TRUNCATES past it — which can collide two distinct names on one table.
// SQL Server: 128. Validate here and fail loudly rather than let Postgres truncate or MySQL die mid-load.
const IDENTIFIER_MAX_LENGTH: Record<supportedDialects, number> = {
    mysql: 64,
    pgsql: 63,
    sqlserver: 128,
};

/**
 * Wrap a SQL identifier in the dialect's quote char, doubling any embedded quote (SQL-standard
 * escape). Closes identifier-injection via attacker-controlled names (e.g. a JSON key
 * `` a`, ADD COLUMN evil TEXT, ADD COLUMN `b `` on MySQL, or `x" ... --` on Postgres).
 * Throws on empty/non-string, or a NUL byte (no dialect permits NUL, and it can truncate the statement).
 */
export function escapeIdentifier(name: string, dialect: supportedDialects): string {
    if (typeof name !== "string" || name.length === 0) {
        throw new Error(`Invalid SQL identifier: expected a non-empty string, received ${JSON.stringify(name)}`);
    }
    if (name.includes("\0")) {
        throw new Error(`Invalid SQL identifier: NUL byte is not permitted (${JSON.stringify(name)})`);
    }
    // Fail loudly on an over-long identifier (source-derived, or an autosql staging/history name built
    // from it) rather than emit a statement the DB rejects — or, on Postgres, silently truncates.
    // Measure the way each dialect counts its limit so a legal international name isn't false-rejected:
    // Postgres — UTF-8 BYTES (NAMEDATALEN-1 = 63); MySQL — Unicode CODE POINTS; SQL Server — UTF-16
    // code units (`sysname` = `nvarchar(128)`).
    const maxLen = IDENTIFIER_MAX_LENGTH[dialect];
    let length: number;
    let unit: string;
    if (dialect === "pgsql") { length = Buffer.byteLength(name, "utf8"); unit = "bytes"; }
    else if (dialect === "mysql") { length = [...name].length; unit = "characters"; }
    else { length = name.length; unit = "characters"; }
    if (length > maxLen) {
        throw new Error(`Invalid SQL identifier: "${name}" (${length} ${unit}) exceeds the ${maxLen}-${unit === "bytes" ? "byte" : "character"} identifier limit for ${dialect}; shorten the underlying table/column name (autosql derives staging/history names from it, so it can exceed the limit even when the base name does not).`);
    }
    // SQL Server: bracket-quote, doubling only the closing bracket ("]" -> "]]"). The pair is
    // asymmetric ([ ]), so it can't use the symmetric doubling below.
    if (dialect === "sqlserver") {
        return `[${name.split("]").join("]]")}]`;
    }
    const quote = IDENTIFIER_QUOTE[dialect];
    return `${quote}${name.split(quote).join(quote + quote)}${quote}`;
}

/**
 * Escape a scalar as a single-quoted SQL string literal, for the rare cases where a value must be
 * inlined into statement text (e.g. a column DEFAULT) and can't be parameter-bound. Doubles single
 * quotes for both dialects; also escapes backslashes for MySQL (which treats `\` as an escape by
 * default). Postgres with `standard_conforming_strings` on (default) treats `\` literally, so it's
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
 * Validate a SQL type token (after local→server translation) before it is interpolated into a
 * column definition or a `USING`/`CAST` expression. Spaces are allowed (`double precision`,
 * `timestamp with time zone`), but quotes, parens, semicolons and other punctuation are rejected —
 * lengths are appended separately, so a legit type token never needs them. Returns the token unchanged.
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

/**
 * Reject a column DEFAULT expression that could terminate the column definition and inject
 * further DDL. autosql's convention is that a default is a bare SQL expression — callers pass
 * `CURRENT_TIMESTAMP`, `UUID()`, a number, or a self-quoted `'literal'` — so the value is
 * emitted verbatim and must not contain a statement separator, comment introducer, a comma
 * (which would start a new column/ALTER clause), a NUL byte, or an unbalanced single quote.
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
 * Render a column DEFAULT for a DDL statement. Defaults are SQL expressions, not string literals:
 * a per-dialect translation (e.g. UUID() -> (UUID())), null, booleans and numbers emit directly;
 * any other value is emitted bare after `assertSafeDefaultExpression` confirms it can't break out
 * of the column definition. A string-literal default must be self-quoted by the caller (e.g.
 * `"'active'"`), matching the existing CREATE-path behavior.
 */
export function renderColumnDefault(value: unknown, dialectConfig: DialectConfig): string {
    if (typeof value === "string" && dialectConfig.defaultTranslation[value] !== undefined) {
        return dialectConfig.defaultTranslation[value];
    }
    if (value === null || value === undefined) return "NULL";
    if (typeof value === "boolean" || typeof value === "number") return String(value);
    const expr = String(value);
    assertSafeDefaultExpression(expr);
    return expr;
}
