# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/trafficbyintent/kysely-bigquery/compare/v1.4.3...HEAD
[1.4.3]: https://github.com/trafficbyintent/kysely-bigquery/compare/v1.4.2...v1.4.3
[1.4.2]: https://github.com/trafficbyintent/kysely-bigquery/compare/v1.4.1...v1.4.2
[1.4.1]: https://github.com/trafficbyintent/kysely-bigquery/compare/v1.4.0...v1.4.1
[1.4.0]: https://github.com/trafficbyintent/kysely-bigquery/compare/v1.3.1...v1.4.0
[1.3.1]: https://github.com/trafficbyintent/kysely-bigquery/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/trafficbyintent/kysely-bigquery/releases/tag/v1.3.0