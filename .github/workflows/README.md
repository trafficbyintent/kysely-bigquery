# GitHub Actions Configuration

This directory contains GitHub Actions workflows and related configuration for continuous integration, testing, and releases.

## Directory Structure

```
.github/
├── workflows/
│   ├── ci.yml                 # Continuous Integration (runs on push/PR to main)
│   ├── release-simplified.yml # Publish to npm and create GitHub release
│   └── manual-release.yml     # Create release PRs with version bump and changelog
├── act-events/                # Event payloads for local testing
│   ├── push.json              # Simulates push to main branch
│   ├── pull_request.json      # Simulates PR event
│   ├── tag.json               # Simulates tag push event
│   └── workflow_dispatch.json # Simulates manual trigger
├── test-actions.sh            # Test workflows locally with act
└── validate-workflows.sh      # Validate workflow syntax without Docker
```

## Workflows Overview

### 1. CI (ci.yml)
**Trigger**: On push to `main` and on pull requests targeting `main`

**Purpose**: Run linting, tests with coverage enforcement, and build checks

**Jobs**:
- **Lint Check (Required)**:
  - Runs ESLint
  - Checks Prettier formatting via `npm run format:check`
- **Test**: Runs on Ubuntu with Node.js 18.x, 20.x, and 22.x
  - Builds the project
  - Runs unit tests with coverage thresholds (`npm run test:coverage`)
  - Optionally runs integration tests (only on push if credentials are configured)
  - Validates package can be packed

### 2. Release Simplified (release-simplified.yml)
**Trigger**: Manual workflow dispatch only

**Purpose**: Publish the current version from package.json to npm and create a GitHub release

**Jobs**:
- **Publish**:
  - Builds and tests with coverage enforcement
  - Runs linting
  - Reads version from package.json (no version input needed)
  - Publishes to npm registry as public package
  - Creates GitHub release with tag

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

### npm Authentication

All workflows pass `NPM_TOKEN` as an environment variable to `npm ci`. The checked-in `.npmrc` reads `${NPM_TOKEN}` to authenticate with the npm registry for both installing private `@trafficbyintent` packages and publishing. All packages are on npmjs.org — GitHub Packages is not used.

**Do not overwrite `.npmrc` in CI** — the checked-in file already has the correct configuration. Do not use `NODE_AUTH_TOKEN` — our `.npmrc` reads `NPM_TOKEN`.

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
act -W .github/workflows/release-simplified.yml --eventpath .github/act-events/workflow_dispatch.json --secret-file .secrets
```

**Without Docker** (quick validation):
```bash
# Validate workflow syntax and configuration
./.github/validate-workflows.sh
```

## Release Process

### Overview

We use a **manual version management** approach where version updates are done locally and the CI/CD pipeline publishes the existing version from package.json. This approach:

- Gives full control over versioning
- Avoids git permission issues in CI
- Makes version history clear in git log
- Simplifies the CI/CD pipeline

### Step-by-Step Release Process

#### 1. Local Version Management

```bash
# 1. Ensure your main branch is up to date
git checkout main
git pull origin main

# 2. Update version in package.json
# Option A: Use npm version (creates commit but no push)
npm version patch --no-git-tag-version  # For bug fixes (1.4.4 -> 1.4.5)
npm version minor --no-git-tag-version  # For new features (1.4.4 -> 1.5.0)
npm version major --no-git-tag-version  # For breaking changes (1.4.4 -> 2.0.0)

# Option B: Manually edit package.json
# Edit the "version" field directly
```

#### 2. Update Documentation

Edit `CHANGELOG.md`:

```markdown
## [1.4.5] - 2024-01-20

### Added
- New features...

### Changed
- Updates...

### Fixed
- Bug fixes...
```

#### 3. Commit and Push

```bash
# Commit the version update
git add package.json CHANGELOG.md
git commit -m "chore: bump version to 1.4.5

- Add feature X
- Fix bug Y
- Update dependency Z"

# Push to main branch
git push origin main
```

#### 4. Trigger Release Workflow

1. Go to GitHub Actions tab in the repository
2. Select "Release (Simplified)" workflow
3. Click "Run workflow"
4. Select branch: `main`
5. Click "Run workflow" button

The workflow will:
- Read version from package.json
- Run tests with coverage enforcement and linting
- Build the package
- Publish to npm registry as public package
- Create a GitHub release with tag `v1.4.5`

#### 5. Verify Release

```bash
# Check npm registry
npm view @trafficbyintent/kysely-bigquery@1.4.5

# Check GitHub Releases
# Visit: https://github.com/trafficbyintent/kysely-bigquery/releases
```

### Alternative: Manual Release Workflow

For a more automated approach, use the Manual Release workflow:

1. Go to GitHub Actions tab
2. Select "Manual Release" workflow
3. Choose release type (patch/minor/major) and provide a release message
4. The workflow creates a PR with version bump and changelog updates
5. After merging the PR, run "Release (Simplified)" to publish

### Beta/Pre-release Versions

For testing new features before stable release:

```bash
# 1. Create beta version locally
npm version prerelease --preid=beta --no-git-tag-version
# Results in: 1.5.0-beta.0

# 2. Commit and push
git add package.json
git commit -m "chore: prepare beta release 1.5.0-beta.0"
git push origin main

# 3. Run Release workflow as normal
# The workflow will publish the beta version
```

### Rollback Process

If you need to rollback a release:

```bash
# 1. Revert the version in package.json
git revert HEAD  # If last commit was the version bump

# 2. Or manually fix and create new patch version
npm version patch --no-git-tag-version
# Update CHANGELOG.md with rollback notes

# 3. Push and release the fix
git push origin main
# Run Release workflow
```

### Security Considerations

1. **NPM Token**: Must be configured in GitHub Secrets for publishing
2. **Public Package**: This will be published as a public npm package
3. **No secrets in code**: Ensure no sensitive data before releasing
4. **Audit dependencies**: Run `npm audit` before each release

### Release Best Practices

1. **Always test locally first**
   ```bash
   npm test
   npm run lint
   npm run build
   ```

2. **Update CHANGELOG.md with every release**
   - Follow Keep a Changelog format
   - Be clear about breaking changes

3. **Use semantic versioning**
   - PATCH: Bug fixes (1.4.4 -> 1.4.5)
   - MINOR: New features, backwards compatible (1.4.0 -> 1.5.0)
   - MAJOR: Breaking changes (1.0.0 -> 2.0.0)

4. **Create detailed commit messages**
   - Explain what changed and why
   - Reference issues if applicable

5. **Monitor after release**
   - Check GitHub Actions for success
   - Verify package is accessible
   - Monitor for user issues

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
