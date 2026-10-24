import tseslint from "typescript-eslint";

// Landing step 1 (see prompt/autosql-improvements/NEXT-CLI-PROMPT-lint-and-dedup-audit.md): config +
// script + CI step, every rule at "warn", zero source changes. Rules are promoted to "error" one at a
// time in follow-up commits so a reviewer can see what each promotion actually changes.
//
// Deliberately NOT `tseslint.configs.recommendedTypeChecked` — that preset defaults dozens of
// unrelated/stylistic rules to "error" we haven't vetted. Hand-picking the rules the audit actually
// asked for (bug-finding, not reformatting) keeps this landing reviewable.
export default tseslint.config(
    { ignores: ["dist/**", "coverage/**", "node_modules/**"] },
    {
        files: ["src/**/*.ts", "tests/**/*.ts"],
        languageOptions: {
            parser: tseslint.parser,
            parserOptions: {
                // `tests/` isn't in tsconfig.json's `include` (deliberately — it's not part of the
                // build), so pointing at that file directly would parse-error on every test file for
                // the type-aware rules below. tsconfig.eslint.json extends it and adds `tests`.
                project: ["./tsconfig.eslint.json"],
                tsconfigRootDir: import.meta.dirname
            }
        },
        plugins: { "@typescript-eslint": tseslint.plugin },
        rules: {
            // autosql's whole stream contract is "these methods reject and MUST be awaited" — today
            // enforced only by documentation. Highest-value rule in this config.
            "@typescript-eslint/no-floating-promises": "warn",
            "@typescript-eslint/no-misused-promises": "warn",
            "@typescript-eslint/await-thenable": "warn",
            "@typescript-eslint/no-unused-vars": "warn",
            "@typescript-eslint/no-explicit-any": "warn",
            "@typescript-eslint/consistent-type-imports": "warn",
            // Tolerates BOTH conventions already in use at module scope (UPPER_CASE constants,
            // camelCase constants — see the audit's table) rather than picking one; it exists to
            // catch a THIRD, genuinely inconsistent form, not to relitigate the two in use today.
            // Exported names are never auto-renamed (breaking change for consumers) regardless.
            "@typescript-eslint/naming-convention": [
                "warn",
                { selector: "variable", modifiers: ["const"], format: ["camelCase", "UPPER_CASE"] }
            ]
        }
    },
    {
        files: ["tests/**/*.ts"],
        rules: {
            // Tests use `any` and `(handle as any)` freely and on purpose.
            "@typescript-eslint/no-explicit-any": "off"
        }
    }
);
