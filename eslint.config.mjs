import tseslint from "typescript-eslint";

// Already-vetted rules and their final severity/options, applied on top of strict-type-checked's
// "everything else at warn" below. 4 of these 5 are already part of strict-type-checked (so this
// is really just "keep them at error, with our options" rather than a new promotion);
// consistent-type-imports is a separate, non-type-checked rule the preset doesn't include at all.
const PROMOTED_RULES = {
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
    "@typescript-eslint/consistent-type-imports": "error",
    // Deferred deliberately: 240 existing call sites need real type design, not a mechanical
    // fix. Tracked as its own follow-up initiative, not promoted alongside everything else here.
    "@typescript-eslint/no-explicit-any": "warn",
    // Both fight a deliberate pattern rather than finding a real bug, so kept at error with
    // scoped inline exceptions at each site instead of disabled outright:
    // - no-extraneous-class: the 9 query-builder "class { static ... }" shapes and
    //   WorkerHelper were converted to plain object namespaces (no caller changes needed —
    //   nothing ever `new`'d, extended, or exported them); the handful of remaining exceptions
    //   are genuine class/constructor references (Readable, Pg, SSHClient — see naming-convention).
    // - no-require-imports: the lazy require()s for the optional peer drivers (mysql2/pg/
    //   mssql/ssh2/pg-copy-streams) are deliberate — a static import would force every consumer
    //   to install all three/five drivers.
    "@typescript-eslint/no-extraneous-class": "error",
    "@typescript-eslint/no-require-imports": "error",
    // Every instance was a genuinely redundant `as T`/`!` — the compiler already inferred the
    // same type without it. Promoted now that the full `src/` tree is clean under it.
    "@typescript-eslint/no-unnecessary-type-assertion": "error",
    // Rule only flags `return await` outside a try (or a try with no finally) — every flagged
    // site here had no catch/finally depending on the await, so dropping it is behavior-neutral.
    // Sites inside a try/finally (e.g. loadStrategy.ts's staging path) were correctly left alone:
    // removing await there would let `finally` run before the returned promise settles.
    "@typescript-eslint/return-await": "error",
    // Each site was a `.catch(e => this.db.error(...))`/`setTimeout(() => fn())`-shaped arrow
    // shorthand implicitly returning a logger/void call's result — wrapped in braces so the
    // callback returns nothing instead of a discarded value.
    "@typescript-eslint/no-confusing-void-expression": "error"
};

// src/ adopts strict-type-checked wholesale rather than hand-picking rules (the more common
// starting point for a TS library serious about type safety) — but landed the same way every
// rule in this file has been: at "warn" with zero source changes first, so each later promotion
// to "error" is its own reviewable commit. PROMOTED_RULES overrides the handful already vetted.
const STRICT_TYPE_CHECKED_AT_WARN = Object.fromEntries(
    tseslint.configs.strictTypeChecked
        .flatMap(block => (block.rules ? Object.entries(block.rules) : []))
        // Preserve "off" as-is (the preset deliberately disables some core ESLint rules in favour
        // of TS's own checking, e.g. `no-undef` — downgrading those to "warn" would turn them back
        // ON and produce false positives TS already rules out, like flagging `setTimeout`).
        .map(([rule, severity]) => [rule, PROMOTED_RULES[rule] ?? (severity === "off" ? "off" : "warn")])
);

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
        plugins: { "@typescript-eslint": tseslint.plugin }
    },
    {
        // src/ only: strict-type-checked (102 rules) landed at warn, vetted ones at their real
        // severity. NOT applied to tests/ (see below) — the white-box test style here mocks
        // internals via `any`-typed fakes (`const db: any = Database.create(...)`), which trips
        // every `no-unsafe-*` rule on every mock interaction. That's test-mocking noise, not a
        // production type-safety gap; measured at ~2,300 problems on this file set alone before
        // deciding not to apply it there.
        files: ["src/**/*.ts"],
        rules: STRICT_TYPE_CHECKED_AT_WARN
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
        // tests/ only: the original hand-picked rule set (not strict-type-checked — see above),
        // plus the looser naming-convention.
        files: ["tests/**/*.ts"],
        rules: {
            "@typescript-eslint/no-floating-promises": "error",
            "@typescript-eslint/no-misused-promises": "error",
            "@typescript-eslint/await-thenable": "error",
            "@typescript-eslint/no-unused-vars": PROMOTED_RULES["@typescript-eslint/no-unused-vars"],
            "@typescript-eslint/consistent-type-imports": "error",
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
