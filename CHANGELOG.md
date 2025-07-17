# Changelog

All notable changes to this project will be documented in this file.

## [1.3.1] - 2025-07-15

### Added
- **BigQuery Query Compiler** - Custom query compiler that handles BigQuery-specific SQL translations:
  - Translates `UNION` to `UNION DISTINCT` for BigQuery compatibility
  - Converts MySQL-style functions to BigQuery equivalents (e.g., `NOW()` → `CURRENT_TIMESTAMP()`, `DATE_FORMAT()` → `FORMAT_TIMESTAMP()`)
- **Enhanced JSON Support** - Automatic JSON column detection and handling improvements
- **Streaming Query Support** - Proper implementation and testing of streaming queries
- **GitHub Actions Workflows** - CI/CD pipeline setup with local testing via Act
- **Security Documentation** - Added SECURITY.md for vulnerability reporting

### Changed
- **Test Suite Refactoring** - Combined and reorganized test files for better maintainability
- **Code Style Compliance** - Applied Google TypeScript style guide across the codebase
- **File Naming Convention** - Renamed files to follow consistent camelCase naming

### Fixed
- **JSON Error Handling** - Resolved issues with null values and malformed JSON data
- **Google SQL Divergence** - Fixed compatibility issues between Google SQL and MySQL syntax
- **Test Reliability** - Various test fixes to improve consistency

### Documentation
- Updated README and setup documentation
- Added comprehensive examples for JSON handling
- Improved configuration documentation

## [1.2.0] - 2025-01-10

### Added
- Automatic handling of null parameter types for BigQuery queries
- Automatic JSON serialization/deserialization for object parameters
- Improved error messages for BigQuery-specific errors
- Comprehensive unit and integration tests for null and JSON handling

### Fixed
- Fixed "Parameter types must be provided for null values" error when using null in queries
- Fixed JSON field handling - objects are now automatically stringified for BigQuery
- Fixed JSON parsing in query results - JSON strings are automatically parsed back to objects

### Technical Details
- `BigQueryConnection` now automatically detects null parameters and provides type hints
- JSON objects (excluding Date and Buffer) are automatically serialized to strings
- Query results containing JSON strings are automatically parsed back to objects
- Both `executeQuery` and `streamQuery` methods have been enhanced

## [1.1.0] - 2025-01-09

### Added
- Option to pass in an existing BigQuery, Database or Table instance
- Support for BigQuery unenforced constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE)

### Fixed
- Column default value handling in introspection
- Introspection bug with duplicate tables