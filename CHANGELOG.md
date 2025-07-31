## [1.0.2] - 2025-07-31
### 🐛 Bug Fixes
- Fixed issue where `package.json` dependencies were accidentally set to `^latest` instead of specific versions. This broke clean installs and has now been corrected.
- Corrected sampling behavior: when `sampling = 0`, the engine mistakenly applied the `samplingMinimum` instead of returning the full dataset. This caused inaccurate index prediction on large datasets.

## [1.0.1] - 2025-07-30
### ✨ What's New
- Added `excludeBlankColumns` feature to ignore completely empty columns across all rows.
- Updated test suite and README to document the new configuration flag.
- Upgraded all dependencies to their latest stable versions.

## [1.0.0] - 2025-04-02
### 🚨 Breaking Changes
- This is a complete rewrite of the library.
- All previous APIs have been replaced with a new architecture and new function names.

### ✨ What's New
- Entirely new class-based design
- Better error handling, logging, and modularity
- Improved performance and flexibility

### 💥 Upgrade Instructions
If you're not ready to upgrade, you can lock your version to `^0.7.6` in `package.json`.