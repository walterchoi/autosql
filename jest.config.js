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
  extensionsToTreatAsEsm: [".ts"]
};
