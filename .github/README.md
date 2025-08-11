# GitHub Actions Configuration

This directory contains GitHub Actions workflows and related configuration for continuous integration, testing, and releases.

## Directory Structure

```
.github/
├── workflows/
│   ├── ci.yml              # Continuous Integration (runs on push/PR)
│   ├── release.yml         # Automated npm publishing (on tag/manual)
│   └── manual-release.yml  # Create release PRs with changelog
├── act-events/             # Event payloads for local testing
│   ├── push.json          # Simulates push to main branch
│   ├── pull_request.json  # Simulates PR event
│   ├── tag.json           # Simulates tag push event
│   └── workflow_dispatch.json # Simulates manual trigger
├── test-actions.sh        # Test workflows locally with act
└── validate-workflows.sh  # Validate workflow syntax without Docker
```

## Workflows Overview

### 1. CI (ci.yml)
**Trigger**: On push to main/master/develop branches and on pull requests

**Purpose**: Run tests and build checks to guard against regressions

**Jobs**:
- **Test**: Runs on Ubuntu with Node.js 18.x, 20.x, and 22.x
  - Installs dependencies
  - Builds the project
  - Runs unit tests
  - Optionally runs integration tests (only on push events if credentials are configured)
  - Validates package can be packed

### 2. Release (release.yml)
**Trigger**: 
- On push of version tags (e.g., v1.0.0)
- On GitHub release creation
- Manual workflow dispatch with version input

**Purpose**: Automated releases to npm

**Jobs**:
- **Publish**: 
  - Builds and tests the package
  - Updates version if manually triggered
  - Publishes to npm with public access
  - Creates GitHub release for tag pushes

### 3. Manual Release (manual-release.yml)
**Trigger**: Manual workflow dispatch with release type and message

**Purpose**: Create a release pull request with version bump and changelog update

**Jobs**:
- **Create Release PR**:
  - Bumps version based on release type (patch/minor/major)
  - Updates CHANGELOG.md with release notes
  - Creates a release branch
  - Opens a pull request for review

## Setup Instructions

### Required GitHub Secrets

To enable full CI functionality, configure these secrets in your repository settings:

#### Note on GitHub Packages Authentication

This project uses `@trafficbyintent/style-guide` from GitHub Packages. The CI workflows are configured to use `GITHUB_TOKEN` automatically for authentication, so no additional setup is needed for basic CI/CD.

#### 1. NPM_TOKEN (Required for Publishing)

To publish packages to npm, you need to create an NPM access token:

1. Log in to [npmjs.com](https://www.npmjs.com/)
2. Click on your profile icon → Access Tokens
3. Click "Generate New Token" → Select **"Classic Token"**
4. Select **"Automation"** type (this is important for CI/CD)
5. Name it something like "kysely-bigquery-github-actions"
6. Copy the token immediately (you won't see it again!)
7. In your GitHub repository:
   - Go to Settings → Secrets and variables → Actions
   - Click "New repository secret"
   - Name: `NPM_TOKEN`
   - Value: Paste your npm token
   - Click "Add secret"

#### 2. BigQuery Credentials (Optional, for Integration Tests)

Integration tests only run on push events to protected branches when these secrets are configured:

- `BIGQUERY_CREDENTIALS`: Service account JSON credentials for BigQuery access
- `BIGQUERY_PROJECT_ID`: Google Cloud project ID
- `BIGQUERY_DATASET`: BigQuery dataset name for tests (default: test_dataset)

**BigQuery Service Account Setup:**

1. Go to Google Cloud Console
2. Create a new service account or use existing
3. Grant the following roles:
   - BigQuery Data Editor
   - BigQuery Job User
4. Create and download a JSON key
5. Copy the entire JSON content as the `BIGQUERY_CREDENTIALS` secret

## Local Testing with Act

You can test GitHub Actions workflows locally using `act` before pushing changes.

### Configuration Files

- **`.secrets`** - All configuration for both local development and `act` testing (BigQuery settings, NPM token)
- **`.serviceAccount.json`** - Google Cloud service account credentials (referenced in `.secrets`)

### Setup for Local Testing

1. **Install act**:
   ```bash
   # macOS
   brew install act
   
   # Linux/WSL
   curl https://raw.githubusercontent.com/nektos/act/master/install.sh | bash
   ```

2. **Configure `.secrets`**:
   ```bash
   cp .secrets.example .secrets
   ```
   Edit `.secrets` with:
   - `NPM_TOKEN` - Your npm automation token (get from npmjs.com)
   - `GOOGLE_APPLICATION_CREDENTIALS` - Path to service account JSON
   - `GCP_PROJECT_ID` - Your Google Cloud project ID
   - `BIGQUERY_DATASET` - Dataset name for tests
   - `BIGQUERY_CREDENTIALS` - Full service account JSON (for act)
   - `BIGQUERY_PROJECT_ID` - Same as GCP_PROJECT_ID (for act)

### Running Tests

**Using the test script** (recommended):
```bash
# Test all workflows
./.github/test-actions.sh

# Test specific workflow
./.github/test-actions.sh ci
./.github/test-actions.sh release
./.github/test-actions.sh manual-release
```

**Using act directly**:
```bash
# Test CI on push
act push -W .github/workflows/ci.yml --secret-file .secrets

# Test CI on pull request
act pull_request -W .github/workflows/ci.yml --secret-file .secrets

# Test release (dry run)
act push -W .github/workflows/release.yml --eventpath .github/act-events/tag.json --secret-file .secrets --dry-run
```

**Without Docker** (quick validation):
```bash
# Validate workflow syntax and configuration
./.github/validate-workflows.sh
```

## Release Process

### Automated Release (Recommended)

1. Use the "Manual Release" workflow from Actions tab:
   - Go to Actions → Manual Release → Run workflow
   - Select release type: patch/minor/major
   - Enter release message
   
2. Review and merge the created PR

3. Create and push the tag:
   ```bash
   git checkout main
   git pull
   git tag v1.3.2
   git push origin v1.3.2
   ```

4. The Release workflow will automatically publish to npm

### Quick Release

For a quick release without PR:

```bash
# Update version in package.json
npm version patch  # or minor/major

# Push changes and tag
git push origin main --tags
```

## Troubleshooting

### "Context access might be invalid" warnings

These warnings in your IDE are expected - they indicate that the workflow is referencing GitHub secrets. They disappear when:
1. The `.secrets` file is properly configured for local testing
2. The repository secrets are configured in GitHub for production

### NPM Publishing Fails
- Check that NPM_TOKEN is set correctly
- Ensure you have publish permissions for the package scope
- Verify the token hasn't expired
- Token must be "Automation" type, not "Publish" type

### Integration Tests Skipped/Failing
- Integration tests only run on push events (not PRs from forks)
- Check that BigQuery secrets are set if you want them to run
- Verify the service account has proper permissions
- Check that the project ID matches your GCP project

### Act Issues
- If act fails to pull images, check your Docker installation
- For M1/M2 Macs, ensure Docker Desktop is configured for ARM64
- Use `--verbose` flag for detailed debugging output
- Ensure `.secrets` file exists and contains all required values

### Build Failures
- Ensure all dependencies are listed in package.json
- Check that TypeScript compilation succeeds locally
- Verify Node.js version compatibility
- Check that the build script produces output in `dist/`

## Best Practices

1. **Test locally first**: Use `act` to test workflows before pushing
2. **Keep secrets secure**: Never commit `.secrets` or `.env` files
3. **Use semantic versioning**: Follow semver for releases (major.minor.patch)
4. **Document changes**: Update CHANGELOG.md with each release
5. **Review PR workflows**: Ensure CI passes before merging any PR
6. **Monitor workflow runs**: Check Actions tab regularly for failures

## Additional Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [act Documentation](https://github.com/nektos/act)
- [npm Publishing Best Practices](https://docs.npmjs.com/packages-and-modules/publishing-packages)