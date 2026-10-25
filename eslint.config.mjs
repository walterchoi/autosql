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
            "@typescript-eslint/no-floating-promises": "error",
            "@typescript-eslint/no-misused-promises": "error",
            "@typescript-eslint/await-thenable": "error",
            // Respects the codebase's existing convention (predates this config: `_table`, `_client`
            // already appear in src/) that a leading underscore marks a deliberately-unused binding.
            "@typescript-eslint/no-unused-vars": ["error", {
                argsIgnorePattern: "^_",
                varsIgnorePattern: "^_",
                caughtErrorsIgnorePattern: "^_",
                // workerPool.ts's `const { logger, sshClient, sshStream, ...cloneable } = config`
                // destructures-to-omit on purpose (A8: strip non-cloneable keys before worker_threads
                // structured-clones the config) — the omitted names are never meant to be read.
                ignoreRestSiblings: true
            }],
            "@typescript-eslint/no-explicit-any": "warn",
            "@typescript-eslint/consistent-type-imports": "error"
        }
    },
    {
        // src/ only: Google TS style guide's scope-based naming rule. A module-level (file-scope)
        // `const` MAY use CONSTANT_CASE regardless of whether it holds a primitive, object or array;
        // a function/block-scoped binding MUST use camelCase, no exceptions. Chosen over Airbnb's
        // export-based rule (UPPER_CASE only for guaranteed-immutable exports) because it's
        // enforceable by the linter without relying on "is this part of the public API today" — a
        // fact that can drift — and because autosql's module-scope consts (dialect config tables,
        // permanent-error lists) are exactly the kind of fixed data CONSTANT_CASE is meant to flag
        // as "do not mutate". NOT applied to tests/ (see below) — test files routinely declare
        // fixture/config objects at module scope, where forcing CONSTANT_CASE is pure noise.
        files: ["src/**/*.ts"],
        rules: {
            "@typescript-eslint/naming-convention": [
                "error",
                // A function assigned to a module-scope const (helper aliases like `const q = (name)
                // => escapeIdentifier(...)`) is a function, not data — Google's rule exempts
                // functions from CONSTANT_CASE regardless of scope. More specific than the plain
                // "global const" selector below, so it wins where both would otherwise match.
                { selector: "variable", modifiers: ["const"], types: ["function"], format: ["camelCase"], leadingUnderscore: "allow" },
                { selector: "variable", modifiers: ["const", "global"], format: ["UPPER_CASE"], leadingUnderscore: "allow" },
                { selector: "variable", format: ["camelCase"], leadingUnderscore: "allow" }
            ]
        }
    },
    {
        files: ["tests/**/*.ts"],
        rules: {
            // Tests use `any` and `(handle as any)` freely and on purpose.
            "@typescript-eslint/no-explicit-any": "off",
            // Looser than src/: only catches a genuinely non-conforming local (snake_case, or a
            // PascalCase local shadowing its own type name) — module-scope fixture/config consts in
            // tests are NOT forced into CONSTANT_CASE the way src/'s rule forces them.
            "@typescript-eslint/naming-convention": [
                "error",
                { selector: "variable", modifiers: ["const"], format: ["camelCase", "UPPER_CASE"], leadingUnderscore: "allow" }
            ]
        }
    }
);
