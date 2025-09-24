#!/bin/bash

echo "🚀 DuckDB FQE JDBC Driver Test Output Viewer"
echo "=============================================="

# Function to decode HTML entities and show clean output
show_test_output() {
    local file="$1"
    echo "📋 Showing output from: $file"
    echo ""

    if [[ -f "$file" ]]; then
        # Decode HTML entities and show the output
        sed 's/&#x1f4cb;/📋/g; s/&#x1f50d;/🔍/g; s/&#x1f3af;/🎯/g; s/&#x1f4ca;/📊/g; s/&#x1f3d7;/🏗️/g; s/&quot;/"/g; s/✓/✓/g; s/&lt;/</g; s/&gt;/>/g; s/&amp;/\&/g' "$file" | \
        grep -E "(📋|🔍|🎯|📊|✓|⚠|❌|Discovered|Found|Selected|Summary|Structure|Successfully)" | \
        head -50
    else
        echo "❌ Test output file not found: $file"
        echo "   Run tests first: ./gradlew integrationTest"
    fi
    echo ""
}

# Function to run tests with output
run_tests_with_output() {
    echo "🚀 Running integration tests with live output..."
    echo ""

    ./gradlew integrationTest --console=plain 2>&1 | \
    grep -E "(📋|🔍|🎯|📊|✅|⚠|✓|Running|SUCCESS|FAIL|Found|Selected|Summary|Structure|Successfully|Querying|Discovered)"

    echo ""
    echo "✅ Tests completed! View detailed reports:"
    echo "   Browser: file://$(pwd)/app/build/reports/tests/integrationTest/index.html"
}

# Main menu
case "${1:-menu}" in
    "unit")
        echo "📋 Unit Test Results:"
        show_test_output "./app/build/reports/tests/test/classes/com.duckdb.fqe.jdbc.DuckDBFQEDriverTest.html"
        ;;
    "integration")
        echo "📋 Integration Test Results:"
        show_test_output "./app/build/reports/tests/integrationTest/classes/com.duckdb.fqe.jdbc.DuckDBFQEIntegrationTest.html"
        ;;
    "live")
        run_tests_with_output
        ;;
    "discovery")
        echo "📋 Table Discovery Test Output:"
        show_test_output "./app/build/reports/tests/integrationTest/classes/com.duckdb.fqe.jdbc.DuckDBFQEIntegrationTest.html" | \
        grep -A 50 -B 5 "table discovery"
        ;;
    "all")
        echo "📋 ALL Test Results:"
        echo ""
        echo "=== Unit Tests ==="
        show_test_output "./app/build/reports/tests/test/index.html"
        echo "=== Integration Tests ==="
        show_test_output "./app/build/reports/tests/integrationTest/classes/com.duckdb.fqe.jdbc.DuckDBFQEIntegrationTest.html"
        ;;
    *)
        echo "Usage: $0 [option]"
        echo ""
        echo "Options:"
        echo "  unit         - Show unit test results"
        echo "  integration  - Show integration test results"
        echo "  discovery    - Show table discovery test output"
        echo "  live         - Run tests with live output"
        echo "  all          - Show all test results"
        echo ""
        echo "Examples:"
        echo "  $0 discovery    # See table discovery output"
        echo "  $0 live         # Run tests and see live output"
        echo "  $0 all          # See all test results"
        ;;
esac