/**
 * Opt-in performance benchmark suite. Run with `npm run bench`.
 *
 * Kept OUT of the normal `npm test` / `npm run test:unit` runs (which only match *.test.ts) so
 * everyday runs stay fast. Benchmarks match *.bench.ts. Run this before merging a change that could
 * affect the inference or load hot paths; the scaling/fast-path assertions catch algorithmic
 * regressions, and the logged rows/s figures show throughput trends.
 */
const base = require('./jest.config.js');

module.exports = {
  ...base,
  testMatch: ['**/*.bench.ts'],
  // Benchmarks process large datasets; give them room beyond the default 30s.
  testTimeout: 120000,
};
