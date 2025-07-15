# GitHub Actions Workflows

This directory contains GitHub Actions workflows for continuous integration, testing, and releases.

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

### Branch Protection

It's recommended to enable branch protection rules:

1. Go to Settings > Branches
2. Add rule for main/master branch
3. Enable "Require status checks to pass before merging"
4. Select the "test" status check
5. Enable "Require branches to be up to date before merging"

## Testing Workflows Locally

You can test GitHub Actions workflows locally before pushing using `act`:

### Setup for Local Testing

1. **Install act** (if not already installed):
   ```bash
   brew install act
   ```

2. **Copy secrets template** (if exists):
   ```bash
   cp .secrets.example .secrets
   ```

3. **Edit `.secrets`** with your actual values (NPM_TOKEN, etc.)

### Running Tests with Scripts

Test all workflows:
```bash
./.github/test-actions.sh
```

Test specific workflows:
```bash
./.github/test-actions.sh ci              # Test only CI workflow
./.github/test-actions.sh release         # Test only release workflow
./.github/test-actions.sh manual-release  # Test only manual release workflow
```

### Manual Testing with act

Run specific events:
```bash
# Test CI on push
act push -W .github/workflows/ci.yml

# Test CI on pull request
act pull_request -W .github/workflows/ci.yml

# Test release (dry run)
act push -W .github/workflows/release.yml --eventpath .github/act-events/tag.json --dry-run
```

### Running Tests Locally (without act)

```bash
# Run unit tests
npm test

# Run integration tests (requires .env file with BigQuery credentials)
npm run test:integration

# Run all tests
npm run test:all
```

## Usage Examples

### Automated Release (Recommended)

1. Use the manual release workflow:
   ```
   Go to Actions → Manual Release → Run workflow
   Select release type: patch/minor/major
   Enter release message
   ```

2. Review and merge the created PR

3. Create and push the tag:
   ```bash
   git checkout main
   git pull
   git tag v1.3.2
   git push origin v1.3.2
   ```

### Quick Release

For a quick release without PR:

```bash
# Update version in package.json
npm version patch  # or minor/major

# Push changes and tag
git push origin main --tags
```

### Manual Workflow Trigger

From the Actions tab, you can manually trigger a release:
1. Go to Actions → Release
2. Click "Run workflow"
3. Enter the version (e.g., "1.3.2")
4. Click "Run workflow"

## Troubleshooting

### NPM Publishing Fails
- Check that NPM_TOKEN is set correctly
- Ensure you have publish permissions for @trafficbyintent scope
- Verify the token hasn't expired
- Token must be "Automation" type, not "Publish" type

### Integration Tests Skipped/Failing
- Integration tests only run on push events (not PRs from forks)
- Check that BigQuery secrets are set if you want them to run
- Ensure BigQuery credentials are correctly set as secrets
- Verify the service account has proper permissions
- Check that the project ID matches your GCP project

### Build Failures
- Ensure all dependencies are listed in package.json
- Check that TypeScript compilation succeeds locally
- Verify Node.js version compatibility
- Check that the build script produces output in `dist/`

### Release Issues
- Ensure NPM_TOKEN is valid and has publish permissions
- Check that version tags follow semantic versioning (e.g., v1.2.3)
- Verify package.json version matches the tag

### act Issues
- If act fails to pull images, check your Docker installation
- For M1/M2 Macs, ensure Docker Desktop is configured for ARM64
- Use `--verbose` flag for detailed debugging output

## Release Process

### Automated Release (Recommended)
1. Use the "Manual Release" workflow from Actions tab
2. Select release type (patch/minor/major)
3. Enter release message for changelog
4. Review and merge the created PR
5. Create and push a tag: `git tag v1.2.3 && git push origin v1.2.3`
6. The Release workflow will automatically publish to npm

### Manual Release
1. Update version in package.json
2. Update CHANGELOG.md
3. Commit changes
4. Create and push tag: `git tag v1.2.3 && git push origin v1.2.3`
5. The Release workflow will automatically publish to npm