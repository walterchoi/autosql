export function isValidSingleQuery(query: string): boolean {
    // Remove single-line comments (-- this is a comment)
    let cleanedQuery = query.replace(/--.*?(\r?\n|$)/g, ' ');

    // Remove multi-line comments (/* this is a comment */)
    cleanedQuery = cleanedQuery.replace(/\/\*[\s\S]*?\*\//g, ' ');

    // Remove safe content first (strings, JSON)
    cleanedQuery = cleanedQuery.replace(/'[^']*'/g, '') // Remove single-quoted strings
                               .replace(/"[^"]*"/g, '') // Remove double-quoted strings
                               .replace(/\{.*?\}/g, ''); // Remove JSON-like content

    // Trim and remove a single trailing `;`, if present
    cleanedQuery = cleanedQuery.trim().replace(/;$/, '');

    // Check if multiple statements exist after cleaning
    return cleanedQuery.split(';').filter(Boolean).length <= 1;
}
