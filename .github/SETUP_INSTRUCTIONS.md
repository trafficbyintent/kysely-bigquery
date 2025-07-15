# GitHub Actions Setup Instructions

This document explains how to set up the GitHub Actions workflows for automated testing and npm publishing.

## Required GitHub Secrets

### 1. NPM_TOKEN (Required for Publishing)

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

### 2. BigQuery Credentials (Optional, for Integration Tests)

If you want to run integration tests in CI:

1. Create a service account in Google Cloud Console
2. Download the JSON key file
3. Add these secrets to your GitHub repository:
   - `BIGQUERY_CREDENTIALS`: The entire JSON key file contents
   - `BIGQUERY_PROJECT_ID`: Your Google Cloud project ID
   - `BIGQUERY_DATASET`: A test dataset name (e.g., "kysely_bigquery_test")

## Workflows Overview

### 1. CI Workflow (`ci.yml`)
- **Triggers**: On push to main branches and all pull requests
- **Actions**: 
  - Tests on Node.js 18.x, 20.x, and 22.x
  - Builds the package
  - Runs unit tests
  - Runs integration tests (if credentials are provided)
  - Validates package can be packed

### 2. Release Workflow (`release.yml`)
- **Triggers**: 
  - When you push a version tag (e.g., `git tag v1.3.2 && git push origin v1.3.2`)
  - When you create a GitHub release
  - Manual trigger with version input
- **Actions**:
  - Builds and tests the package
  - Publishes to npm
  - Creates GitHub release

### 3. Manual Release Workflow (`manual-release.yml`)
- **Triggers**: Manual only (from Actions tab)
- **Actions**:
  - Creates a release PR with version bump
  - Updates CHANGELOG.md
  - After PR merge, tag the release to trigger publishing

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

### Integration Tests Skipped
- Integration tests only run on push events (not PRs from forks)
- Check that BigQuery secrets are set if you want them to run

### Build Fails
- Ensure all dependencies are listed in package.json
- Check that TypeScript configuration is correct
- Verify that the build script produces output in `dist/`