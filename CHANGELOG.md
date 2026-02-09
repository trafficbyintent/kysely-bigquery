# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] - 2026-02-08

### Breaking Changes
- **JSON auto-parsing removed**: Result strings are no longer speculatively `JSON.parse()`d. Only columns registered in `jsonColumns` config are parsed. If you relied on automatic parsing of unregistered columns, you must now register them or parse manually.
- **`isLikelyJsonColumn` removed**: The `JsonColumnDetector.isLikelyJsonColumn()` method has been removed. Use explicit column registration via `jsonColumns` config instead.

### Added
- **`defaultProject` config**: New dialect option to prepend a GCP project ID to all table references, enabling three-level `project.dataset.table` names within Kysely's two-level parser.
- Tests for `visitFunction` translations (NOW, DATE_FORMAT, LENGTH) through Kysely's query builder path.
- Test for introspector using provided `bigquery` instance instead of creating a new one.

### Fixed
- **Introspector auth bug**: `BigQueryIntrospector` now uses the configured `bigquery` instance instead of always creating a new unauthenticated client.
- **`#inferParamTypes` ordering**: Parameter types are now inferred from post-serialization values, matching what BigQuery actually receives.
- Import casing in test file (`bigQueryConnection` → `BigQueryConnection`) preventing builds on case-sensitive filesystems.

### Removed
- Dead code: `visitCreateTable` no-op override, `visitValue` BigInt no-op, unreachable `schemaName.includes('.')` branch, `isLikelyJsonColumn` method.
- Duplicated null parameter type inference (extracted to shared `#inferParamTypes` method).
- Duplicated fragment processing in `visitRaw` (extracted to `#appendFragmentsWithParams`).
- Performative tests that did not exercise source code.
- `c8 ignore` blocks from core compiler logic (now covered by query builder tests).

## [1.5.0] - 2025-08-24

### Added
- Simplified release workflow (`release-simplified.yml`) following TXI style-guide best practices
- Version bump reference workflow for documentation purposes
- Comprehensive release documentation in `.github/README.md`

### Changed
- Migrated `@trafficbyintent/style-guide` from GitHub Packages to private npm registry
- Updated style-guide dependency to v1.2.1
- Improved CI workflow to remove unnecessary GitHub Package authentication
- Enhanced release process documentation with step-by-step instructions

### Fixed
- CI authentication issues with private npm packages
- Workflow validation and syntax issues

## [1.4.4] - 2025-08-11

### Fixed
- Fixed CI workflow authentication for GitHub Packages
- Corrected ESLint configuration for proper type checking
- Fixed formatting in BigQueryIntrospector.ts

## [1.4.3] - 2025-08-11

### Fixed
- Fixed TypeScript import errors in CI/CD
- Corrected import paths for BigQueryDialectConfig
- Added proper type exports to index.ts
- Fixed type assertion for jsonColumns parameter

## [1.4.2] - 2025-08-11

### Fixed
- Resolved merge conflicts with upstream/main
- Improved GitHub Actions authentication for private packages
- Updated dependencies and configurations

## [1.4.1] - 2025-08-11

### Changed
- Version bump for GitHub Packages release
- Updated @types/node from ^20.10.0 to ^20.19.10 for better type definitions

### Fixed
- Resolved ESLint import resolver configuration issues
- Re-enabled all ESLint import rules by adding eslint-import-resolver-typescript
- Fixed tsconfig.json to properly extend style-guide configuration

## [1.4.0] - 2024-08-11

### Added
- JSDoc comments for all public methods
- c8 ignore comments for defensive programming patterns
- Additional test coverage achieving 100% for lines/statements/functions
- Comprehensive documentation in .github/README.md for release process

### Changed
- Switched from istanbul to v8 coverage provider with c8 ignore support
- Updated vitest configuration to accept 98% branch coverage (2 defensive branches)
- Improved error messages with better context

### Fixed
- Test coverage now properly excludes defensive programming patterns
- Memory issues documented for streaming tests

### Removed
- Removed unused bluebird dependency
- Removed unused @types/bluebird devDependency
- Removed manual BigQuery setup script (tests are now self-contained)

## [1.3.1] - 2024-12-15

### Added
- Support for passing existing BigQuery, Dataset, or Table instances to dialect
- JSON column configuration for automatic serialization/deserialization
- Comprehensive test coverage for all major components

### Changed
- Improved BigQuery SQL translation for better compatibility
- Enhanced error handling with contextual messages

### Fixed
- NULL parameter type declaration for BigQuery compatibility
- Constraint handling with proper NOT ENFORCED syntax

## [1.3.0] - 2024-11-01

### Added
- Initial release as @traffic.by.intent/kysely-bigquery
- Full BigQuery dialect implementation for Kysely
- Automatic SQL translation from MySQL to BigQuery syntax
- Streaming support for large result sets
- Comprehensive introspection support

### Changed
- Forked from @maktouch/kysely-bigquery
- Restructured for TXI coding standards

[Unreleased]: https://github.com/trafficbyintent/kysely-bigquery/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/trafficbyintent/kysely-bigquery/compare/v1.5.0...v2.0.0
[1.5.0]: https://github.com/trafficbyintent/kysely-bigquery/compare/v1.4.3...v1.5.0
[1.4.3]: https://github.com/trafficbyintent/kysely-bigquery/compare/v1.4.2...v1.4.3
[1.4.2]: https://github.com/trafficbyintent/kysely-bigquery/compare/v1.4.1...v1.4.2
[1.4.1]: https://github.com/trafficbyintent/kysely-bigquery/compare/v1.4.0...v1.4.1
[1.4.0]: https://github.com/trafficbyintent/kysely-bigquery/compare/v1.3.1...v1.4.0
[1.3.1]: https://github.com/trafficbyintent/kysely-bigquery/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/trafficbyintent/kysely-bigquery/releases/tag/v1.3.0