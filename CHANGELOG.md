## [1.0.4] - 2025-08-11
### 🐛 Bug Fixes
- Updated length calculation when converting from a decimal type to a non-decimal type.  
  - In `1.0.3`, we added `+1` to account for the decimal point.  
  - In `1.0.4`, we now add `trueMaxDecimal` as well, as we found that in cases where the decimal was rounded due to exceeding `databaseConfig.decimalMaxLength`, the resulting length on conversion to `varchar` could still mismatch. This ensures the length matches the literal string being stored after rounding.

## [1.0.3] - 2025-08-09
### 🐛 Bug Fixes
- Fixed issue where going from decimal column to varchar column would result in length that was insufficient by 1. This was due to not counting the decimal point (since length was just set to length + decimal)
- Added a step before inserting data into staging table to alter the staging tables to reflect any other alterations that were made to the primary table.

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