#!/usr/bin/env bash

# Validate GitHub Actions workflows without Docker
# This is a lightweight alternative to test-actions.sh

echo "🔍 Validating GitHub Actions Workflows"
echo "====================================="

failed=0

# Check workflow files exist
echo ""
echo "📁 Checking workflow files..."
for workflow in ci.yml release-simplified.yml manual-release.yml; do
    if [ -f ".github/workflows/$workflow" ]; then
        echo "   ✅ $workflow exists"
    else
        echo "   ❌ $workflow missing"
        failed=1
    fi
done

# Validate YAML syntax
echo ""
echo "📝 Validating YAML syntax..."
for workflow in .github/workflows/*.yml; do
    if [ -f "$workflow" ]; then
        filename=$(basename "$workflow")
        # Basic YAML validation using Ruby (built-in on macOS)
        if ruby -ryaml -e "YAML.load_file('$workflow')" 2>/dev/null; then
            echo "   ✅ $filename - valid YAML"
        else
            echo "   ❌ $filename - invalid YAML"
            failed=1
        fi
    fi
done

# Check for required secrets in workflows
echo ""
echo "🔑 Checking for required secrets..."
required_secrets=("NPM_TOKEN")
optional_secrets=()

for secret in "${required_secrets[@]}"; do
    if grep -q "\${{ secrets.$secret }}" .github/workflows/*.yml 2>/dev/null; then
        echo "   ⚠️  $secret - required (used in workflows)"
    fi
done

for secret in "${optional_secrets[@]}"; do
    if grep -q "\${{ secrets.$secret }}" .github/workflows/*.yml 2>/dev/null; then
        echo "   ℹ️  $secret - optional (used in workflows)"
    fi
done

# Check event configurations
echo ""
echo "📅 Checking workflow triggers..."
echo "   CI workflow triggers on:"
grep -A5 "^on:" .github/workflows/ci.yml | grep -E "push:|pull_request:" | sed 's/^/      /'
echo "   Release (Simplified) workflow triggers on:"
grep -A5 "^on:" .github/workflows/release-simplified.yml | grep -E "workflow_dispatch:" | sed 's/^/      /'
echo "   Manual Release workflow triggers on:"
grep -A5 "^on:" .github/workflows/manual-release.yml | grep -E "workflow_dispatch:" | sed 's/^/      /'

# Summary
echo ""
echo "====================================="
if [ $failed -eq 0 ]; then
    echo "✅ All validations passed!"
    echo ""
    echo "Next steps:"
    echo "1. Add NPM_TOKEN to GitHub Secrets"
    echo "2. Commit and push these workflows"
    echo "3. Create a test PR to verify CI"
    echo "4. Tag a release to test publishing"
else
    echo "❌ Some validations failed"
    exit 1
fi