#!/bin/bash

# Script to run the temperature alerts E2E test
# This script sets up prerequisites and runs the test with the right parameters

# Check if Timeplus is running
echo "Checking if Timeplus is running..."
if ! docker ps | grep -q timeplus; then
  echo "ERROR: Timeplus container not found. Please make sure it's running."
  echo "Run: docker ps | grep timeplus"
  exit 1
fi

# Make sure we can connect to Timeplus
echo "Testing Timeplus connection..."
docker exec timeplus timeplusd client --user test --password test123 --query="SELECT 1" > /dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "ERROR: Failed to connect to Timeplus. Check credentials and connection."
  exit 1
fi

# Print information about existing views
echo "Current materialized views:"
docker exec timeplus timeplusd client --user test --password test123 --query="SELECT name FROM system.tables WHERE engine = 'MaterializedView'"

# Run the test
echo "Running temperature alert E2E test..."
cd "$(dirname "$0")/.."
go test -v ./pkg/e2e -run TestTemperatureAlertsE2E -skip=false

# Check exit status
if [ $? -eq 0 ]; then
  echo "✅ Temperature alerts E2E test completed successfully!"
else
  echo "❌ Temperature alerts E2E test failed."
  exit 1
fi

# Show current views after test
echo "Materialized views after test (should be cleaned up):"
docker exec timeplus timeplusd client --user test --password test123 --query="SELECT name FROM system.tables WHERE engine = 'MaterializedView'"

exit 0 