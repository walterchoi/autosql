/**
 * Jest config for unit tests only — no live database required.
 * Used by `prepublishOnly` so `npm publish` never depends on Docker being up.
 * Run the full suite (including integration tests) with `npm test`.
 */
const base = require('./jest.config.js');

module.exports = {
  ...base,
  testPathIgnorePatterns: [
    '/node_modules/',
    'tests/alter-table\\.test\\.ts',
    'tests/alter-table-complex\\.test\\.ts',
    'tests/auto-configure\\.test\\.ts',
    'tests/auto-sql\\.test\\.ts',
    'tests/check-table-exists\\.test\\.ts',
    'tests/create-table\\.test\\.ts',
    'tests/create-table-complex\\.test\\.ts',
    'tests/database-connection\\.test\\.ts',
    'tests/database-error-handling\\.test\\.ts',
    'tests/database-retry\\.test\\.ts',
    'tests/get-table-metadata\\.test\\.ts',
    'tests/index-query-builder\\.test\\.ts',
    'tests/query-validation\\.test\\.ts',
    'tests/test-split-table\\.test\\.ts',
  ],
};
