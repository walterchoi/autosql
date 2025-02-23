export default {
    preset: "ts-jest",
    testEnvironment: "node",
    roots: ["<rootDir>/tests"],
    transform: {
      "^.+\\.tsx?$": "ts-jest"
    },
    moduleFileExtensions: ["ts", "tsx", "js"],
    collectCoverage: true,
    coverageDirectory: "coverage",
    verbose: true
  };