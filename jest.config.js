export default {
  preset: "ts-jest",
  testEnvironment: "node",
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
