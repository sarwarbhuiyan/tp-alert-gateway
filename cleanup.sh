#!/bin/bash

echo "Cleaning up Timeplus streams and views"

# Drop test streams
STREAMS=$(docker exec timeplus timeplusd client --user test --password test123 --query="SELECT name FROM system.tables WHERE engine = 'Stream' AND name NOT IN ('license_validation_log', 'stream_metric_log', 'stream_state_log')" | grep -v "^COLUMNS\|^SCHEMATA\|^TABLES\|^VIEWS")

for stream in $STREAMS; do
    # Need to handle names with dashes by quoting them
    echo "Dropping stream: $stream"
    docker exec timeplus timeplusd client --user test --password test123 --query="DROP STREAM IF EXISTS \`$stream\`"
done

# Drop views (if any)
VIEWS=$(docker exec timeplus timeplusd client --user test --password test123 --query="SELECT name FROM system.tables WHERE engine = 'View' AND name NOT IN ('COLUMNS', 'SCHEMATA', 'TABLES', 'VIEWS', 'columns', 'schemata', 'tables', 'views')")

for view in $VIEWS; do
    echo "Dropping view: $view"
    docker exec timeplus timeplusd client --user test --password test123 --query="DROP VIEW IF EXISTS \`$view\`"
done

# Drop materialized views (if any)
MATERIALIZED_VIEWS=$(docker exec timeplus timeplusd client --user test --password test123 --query="SELECT name FROM system.tables WHERE engine = 'MaterializedView'")

for mview in $MATERIALIZED_VIEWS; do
    echo "Dropping materialized view: $mview"
    docker exec timeplus timeplusd client --user test --password test123 --query="DROP MATERIALIZED VIEW IF EXISTS \`$mview\`"
done

echo "Creating required streams for tests"
# Create device_temperatures stream for testing - use lowercase data types
docker exec timeplus timeplusd client --user test --password test123 --query="CREATE STREAM IF NOT EXISTS device_temperatures (device_id string, temperature float64, timestamp datetime64(3))"

echo "Cleanup complete - Ready to run tests" 