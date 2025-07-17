# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| Latest  | :white_check_mark: |
| < Latest| :x:                |

## Reporting a Vulnerability

We take the security of kysely-bigquery seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### How to Report

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please use GitHub's private vulnerability reporting feature:

1. Navigate to the Security tab of this repository
2. Click on "Report a vulnerability"
3. Fill out the vulnerability report form with the details of your findings

This ensures your report remains private while we work on a fix.

### What to Include

Please include the following information in your report:

- Type of issue (e.g., SQL injection, authentication bypass, data exposure, etc.)
- Full paths of source file(s) related to the manifestation of the issue
- The location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

### Response Timeline

We will make every effort to acknowledge receipt of your vulnerability report and provide updates on the progress towards a fix. Response times may vary depending on the complexity and severity of the issue.

## Security Best Practices

When using kysely-bigquery, please follow these security best practices:

1. **Input Validation**: Always validate and sanitize user inputs before using them in queries
2. **Query Building**: Use Kysely's built-in query builder methods rather than raw SQL when possible
3. **Authentication**: Ensure proper authentication with BigQuery using secure credential management
4. **Access Control**: Follow the principle of least privilege when configuring BigQuery permissions
5. **Dependencies**: Keep all dependencies up to date to ensure you have the latest security patches

## Disclosure Policy

When we receive a security vulnerability report, we will:

1. Confirm the problem and determine the affected versions
2. Audit code to find any similar problems
3. Prepare fixes for all supported versions
4. Release patches as soon as possible

We appreciate your efforts to responsibly disclose your findings and will make every effort to acknowledge your contributions.