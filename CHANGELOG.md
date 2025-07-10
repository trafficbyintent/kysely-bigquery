# Changelog

All notable changes to this project will be documented in this file.

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