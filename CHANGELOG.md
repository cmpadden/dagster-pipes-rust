# Changelog

## [UNRELEASED]
- (pull/9) Handle errors for `ParamsLoader` and `ContextLoader` implementations


### Added

- (pull/5) Setup `jsonschema` and `quicktype` to generate types shared with `dagster-pipes-java`
- (pull/4) Added ability to report asset check via `context.report_asset_check`
- (pull/4) Added `AssetCheckSeverity` `enum` for specifying check severity
- (pull/4) Updated example to include usage of `report_asset_check`
- (pull/3) Mirror Python `PipesContextLoader` and `PipesParamsLoader` ABC implementations as Rust traits
- (pull/3) Define `PipesDefaultContextLoader` to implement `PipesContextLoader` with net new implementation
- (pull/3) Define `PipesEnvVarParamsLoader` to implement `PipesParamsLoader` with logic refactored from lib.rs

## 0.1.5

### Added

- Implemented temporary file based message writing
- Added the `context.report_asset_materialization` method
