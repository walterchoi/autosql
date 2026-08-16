// Run the suite in a NON-UTC zone by default so timezone-sensitive bugs (e.g. the A1 date-coercion
// shift) cannot hide behind a UTC CI host — this is the "test the defaults / non-UTC host" dimension.
// Set here, in the config module, so it is in the environment before jest forks its workers and each
// worker's V8 reads it at startup (mutating process.env.TZ mid-run is unreliable, esp. on Windows).
// Overridable: `TZ=UTC npm test` still works for anyone who needs it.
process.env.TZ = process.env.TZ || "Australia/Sydney";

module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  testTimeout: 30000,
  transform: {
    "^.+\\.tsx?$": ["ts-jest", { useESM: true }]
  },
  moduleFileExtensions: ["ts", "tsx", "js", "mjs", "cjs"],
  moduleNameMapper: {
    // Ensure .js imports resolve to .ts files
    "^(\\.{1,2}/src/.*)\\.js$": "$1.ts"
  },
  extensionsToTreatAsEsm: [".ts"],
  // Preflight: fail fast with a clear message if a test database is unreachable (see the file). The
  // unit config disables this (it needs no DB).
  globalSetup: "<rootDir>/tests/utils/globalSetup.ts"
};
