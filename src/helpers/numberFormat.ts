import { regexPatterns } from "../config/regex";

// Dataset-level number-format consensus (self-contained: reads ONLY the in-memory batch — no catalog
// or data queries). A single dataset is assumed to use one locale, so structural evidence in ANY
// column resolves ambiguity in every column. We resolve ONE {thousands, decimal} pair for the whole
// load; the caller then feeds it through the same config fields inference and sqlize already read.
//
// "Structural" evidence is a value that can only be one layout — so a single one is certainty, not a
// guess. Version strings ("1.2.3") and IPs ("192.168.1.1") fail the 3-digit-group numeric regex and
// never vote, so false positives are well-insulated.

export type SeparatorDecision =
    | { thousands: string; decimal: string } // resolved
    | { conflict: true }                      // contradictory structural evidence (mixed/corrupt data)
    | null;                                   // no sufficient evidence

// Classify one raw value's structural separator evidence.
//   "us" = comma-thousands / dot-decimal   (US/UK/IN — Indian grouping shares US separators)
//   "eu" = dot-thousands / comma-decimal
//   null = no evidence, or the genuinely-ambiguous "1,234" shape (lone sep + exactly 3 trailing digits)
export function classifySeparatorFormat(value: any): "us" | "eu" | null {
    if (typeof value !== "string") return null; // native numbers carry no separators
    let s = value.trim();
    if (!s.includes(",") && !s.includes(".")) return null;                     // no separator → no evidence
    if (!(regexPatterns.number.test(s) || regexPatterns.decimal.test(s))) return null; // not a numeric candidate
    if (s.startsWith("-")) s = s.slice(1);

    const commaCount = (s.match(/,/g) || []).length;
    const dotCount = (s.match(/\./g) || []).length;

    if (commaCount >= 2) return "us";  // "1,234,567" — two commas can only be thousands grouping
    if (dotCount >= 2) return "eu";    // "1.234.567" — two dots can only be thousands grouping
    if (commaCount === 1 && dotCount === 1) {
        // Both present → the LAST one is the decimal separator, the other is thousands.
        return s.lastIndexOf(".") > s.lastIndexOf(",") ? "us" : "eu";
    }
    if (commaCount === 1) {
        // Lone comma: decimal unless it could be a thousands group (exactly 3 trailing, ≤3 leading).
        const [before, after] = s.split(",");
        if (after.length !== 3 || before.length > 3) return "eu"; // comma is a decimal → EU
        return null; // "1,234" — ambiguous, no vote
    }
    if (dotCount === 1) {
        const [before, after] = s.split(".");
        if (after.length !== 3 || before.length > 3) return "us"; // dot is a decimal → US
        return null; // "1.234" — ambiguous, no vote
    }
    return null;
}

// Resolve one separator pair for the whole dataset from pooled structural evidence.
//   minEvidence — a layout only "counts" once it has ≥ this many votes (default 1). Raising it
//     tolerates a few stray/mis-parsed values before a format is trusted, AND before an opposing
//     minority is treated as a genuine conflict.
// Returns the pair, `{conflict:true}` (both layouts present → let the caller warn + default), or null.
export function resolveDatasetSeparators(data: Record<string, any>[], minEvidence = 1): SeparatorDecision {
    let us = 0;
    let eu = 0;
    for (const row of data) {
        if (!row || typeof row !== "object") continue;
        for (const key in row) {
            const vote = classifySeparatorFormat(row[key]);
            if (vote === "us") us++;
            else if (vote === "eu") eu++;
        }
        if (us >= minEvidence && eu >= minEvidence) break; // conflict is already certain — stop early
    }

    const usPresent = us >= minEvidence;
    const euPresent = eu >= minEvidence;
    if (usPresent && euPresent) return { conflict: true };
    if (usPresent) return { thousands: ",", decimal: "." };
    if (euPresent) return { thousands: ".", decimal: "," };
    return null;
}
