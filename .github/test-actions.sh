#!/usr/bin/env bash

# Test GitHub Actions workflows locally using act
# Usage: ./test-actions.sh [workflow-name]

set -e

echo "🧪 Testing GitHub Actions workflows locally with act"
echo "=================================================="

# Check if act is installed
if ! command -v act &> /dev/null; then
    echo "❌ act is not installed. Please install it with: brew install act"
    exit 1
fi

# Check if .secrets file exists
if [ ! -f ".secrets" ]; then
    echo "⚠️  No .secrets file found. Creating from template..."
    cp .secrets.example .secrets
    echo "📝 Please edit .secrets with your actual values before running release tests"
fi

# Function to run a workflow test
run_workflow_test() {
    local workflow=$1
    local event=$2
    local description=$3
    local extra_args="${4:-}"
    
    echo ""
    echo "🔄 Testing: $description"
    echo "   Workflow: $workflow"
    echo "   Event: $event"
    
    if [ -n "$extra_args" ]; then
        act -W ".github/workflows/$workflow" --eventpath ".github/act-events/$event" $extra_args
    else
        act -W ".github/workflows/$workflow" --eventpath ".github/act-events/$event" --secret-file .secrets
    fi
    
    if [ $? -eq 0 ]; then
        echo "✅ $description - PASSED"
    else
        echo "❌ $description - FAILED"
        return 1
    fi
}

# Function to validate workflow syntax
validate_syntax() {
    echo ""
    echo "🔍 Validating workflow syntax..."
    
    for workflow in .github/workflows/*.yml; do
        if [ -f "$workflow" ]; then
            echo -n "   Checking $(basename $workflow)... "
            if act -W "$workflow" -l > /dev/null 2>&1; then
                echo "✅"
            else
                echo "❌"
                return 1
            fi
        fi
    done
}

# Main test execution
main() {
    local workflow_filter="${1:-all}"
    local failed=0
    
    # First validate syntax
    validate_syntax || failed=1
    
    if [ "$workflow_filter" == "all" ] || [ "$workflow_filter" == "ci" ]; then
        echo ""
        echo "=== CI Workflow Tests ==="
        
        # Test CI on push
        run_workflow_test "ci.yml" "push.json" "CI on push to main" || failed=1
        
        # Test CI on pull request
        run_workflow_test "ci.yml" "pull_request.json" "CI on pull request" || failed=1
    fi
    
    if [ "$workflow_filter" == "all" ] || [ "$workflow_filter" == "release" ]; then
        echo ""
        echo "=== Release Workflow Tests ==="
        
        # Test release workflow (dry run to avoid actual publishing)
        run_workflow_test "release.yml" "tag.json" "Release on tag push" "--dry-run" || failed=1
        
        # Test manual release trigger
        run_workflow_test "release.yml" "workflow_dispatch.json" "Manual release trigger" "--dry-run" || failed=1
    fi
    
    if [ "$workflow_filter" == "all" ] || [ "$workflow_filter" == "manual-release" ]; then
        echo ""
        echo "=== Manual Release Workflow Tests ==="
        
        # Test manual release PR creation
        run_workflow_test "manual-release.yml" "workflow_dispatch.json" "Manual release PR creation" "--dry-run" || failed=1
    fi
    
    echo ""
    echo "=================================================="
    if [ $failed -eq 0 ]; then
        echo "✅ All workflow tests passed!"
    else
        echo "❌ Some workflow tests failed"
        exit 1
    fi
}

# Show help if requested
if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    echo "Usage: $0 [workflow-name]"
    echo ""
    echo "Test GitHub Actions workflows locally using act"
    echo ""
    echo "Options:"
    echo "  all              Test all workflows (default)"
    echo "  ci               Test only CI workflow"
    echo "  release          Test only release workflow"
    echo "  manual-release   Test only manual release workflow"
    echo ""
    echo "Examples:"
    echo "  $0               # Test all workflows"
    echo "  $0 ci            # Test only CI workflow"
    echo "  $0 release       # Test only release workflow"
    exit 0
fi

# Run tests
main "$@"