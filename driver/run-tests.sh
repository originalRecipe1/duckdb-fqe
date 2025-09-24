#!/bin/bash

echo "🚀 Running JDBC Driver Tests"
echo "============================="

# Run the tests and capture output
echo "📋 Running integration tests..."
./gradlew integrationTest --tests "*table discovery*" --console=plain 2>&1 | \
grep -E "(🚀|📋|🔍|🎯|📊|✅|PASSED|FAILED|BUILD)" | \
grep -v "gradle" | \
head -20

# Check if tests passed
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Tests completed successfully!"
else
    echo ""
    echo "❌ Some tests failed. Check details above."
fi