// Defense-in-depth guard on caller-supplied SQL (runQuery/testQuery only — NOT the internal builders,
// which are parameter-bound and identifier-escaped, and NOT runTransaction, which intentionally runs
// multi-statement DDL/DML). Returns true when the string is a single statement.
//
// A left-to-right tokenizer, not layered regexes (A23): the previous version stripped comments before
// strings, so a `/*` inside one string literal and `*/` in a later one could make it swallow an
// intervening `; DROP …`; it also mishandled doubled quotes ('') and Postgres dollar-quoting. This
// walks the string once, skipping over string/identifier literals, comments and dollar-quoted bodies,
// and only counts semicolons that actually separate statements.
export function isValidSingleQuery(query: string): boolean {
    const n = query.length;
    let i = 0;
    let statementCount = 0;
    let sawContent = false; // non-whitespace seen since the last separator

    // Skip a quoted run delimited by `quote`, honouring a doubled-quote escape (''/""/``).
    const skipQuoted = (quote: string): void => {
        i++; // opening quote
        while (i < n) {
            if (query[i] === quote) {
                if (query[i + 1] === quote) { i += 2; continue; } // escaped quote
                i++; return; // closing quote
            }
            i++;
        }
    };

    while (i < n) {
        const c = query[i];

        if (c === '-' && query[i + 1] === '-') {                 // line comment
            i += 2;
            while (i < n && query[i] !== '\n') i++;
            continue;
        }
        if (c === '/' && query[i + 1] === '*') {                 // block comment
            i += 2;
            while (i < n && !(query[i] === '*' && query[i + 1] === '/')) i++;
            i += 2;
            continue;
        }
        if (c === "'" || c === '"' || c === '`') { skipQuoted(c); sawContent = true; continue; }

        if (c === '$') {                                         // Postgres dollar-quoting: $tag$ … $tag$
            const m = /^\$[A-Za-z0-9_]*\$/.exec(query.slice(i));
            if (m) {
                const tag = m[0];
                const end = query.indexOf(tag, i + tag.length);
                i = end === -1 ? n : end + tag.length;
                sawContent = true;
                continue;
            }
        }

        if (c === ';') {                                         // statement separator
            if (sawContent) { statementCount++; sawContent = false; }
            i++;
            continue;
        }

        if (c !== ' ' && c !== '\t' && c !== '\r' && c !== '\n') sawContent = true;
        i++;
    }

    if (sawContent) statementCount++;
    return statementCount <= 1;
}
