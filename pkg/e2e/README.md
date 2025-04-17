# Alert Gateway End-to-End Tests

This directory contains end-to-end tests for the Timeplus Alert Gateway. These tests verify the functionality of the alert acknowledgment system by interacting with a running Timeplus instance.

## Test Components

- `alertack_simple_test.go`: A simplified test focusing on alert acknowledgment functionality
- `connection_test.go`: A diagnostic test for verifying Timeplus connectivity
- `temperature_alert_test.go`: A comprehensive test demonstrating temperature monitoring with alerts, throttling, and acknowledgment
- `utils.go`: Helper functions for setting up test streams, views, etc.

## Prerequisites

- Docker running with Timeplus container (named "timeplus")
- The container should have a user "test" with password "test123"
- The Timeplus service should be accessible on localhost:8464

## Running the Tests

By default, the tests are skipped to avoid interrupting CI/CD pipelines with long-running tests. To run them explicitly:

```bash
# Run the connection test to diagnose Timeplus connectivity issues
go test -v ./pkg/e2e -run TestTimeplusConnectionDiagnostics -skip=false

# Run the alert acknowledgment test
go test -v ./pkg/e2e -run TestAlertAcknowledgmentSimple -skip=false

# Run the temperature monitoring test
go test -v ./pkg/e2e -run TestTemperatureAlertsE2E -skip=false

# Run all tests
go test -v ./pkg/e2e -skip=false
```

For convenience, you can also use the script to run the temperature test:

```bash
./scripts/run_temperature_test.sh
```

## Temperature Monitoring Test

The temperature monitoring test (`TestTemperatureAlertsE2E`) demonstrates a complete end-to-end workflow:

1. **Setup Phase**:
   - Creates a temperature stream with device_id, temperature, humidity, and battery fields
   - Sets up an alert rule to trigger when temperature exceeds 30°C
   - Configures alert throttling to prevent duplicate alerts for 1 minute

2. **Normal Operation Phase**:
   - Inserts normal temperature readings (20-25°C) for multiple devices
   - Verifies no alerts are generated for normal conditions

3. **Alert Generation Phase**:
   - Inserts abnormal high temperature (35°C) for one device
   - Verifies an alert is properly generated

4. **Alert Throttling Phase**:
   - Inserts another high temperature reading (38°C) for the same device
   - Verifies that throttling prevents duplicate alerts within the time window

5. **Multiple Device Alerts Phase**:
   - Inserts high temperature for a second device
   - Verifies that separate alerts are generated for different devices

6. **Alert Acknowledgment Phase**:
   - Acknowledges the alert for the first device
   - Verifies the acknowledgment state is properly recorded

7. **Post-Acknowledgment Phase**:
   - Inserts another high temperature reading after acknowledgment
   - Verifies that throttling continues to work after acknowledgment

This test demonstrates the complete workflow of the alert system and verifies that all components work together correctly.

## Troubleshooting

### EOF Errors

If you encounter EOF errors during tests, it often indicates connection stability issues with Timeplus. Some options to fix this:

1. Increase the test timeouts (in the Go tests)
2. Check that Timeplus is running and accessible (`docker ps | grep timeplus`)
3. Restart the Timeplus container: `docker restart timeplus`
4. Verify the port mapping: `docker port timeplus`

### Error When Creating Views

If you see errors about views already existing:

1. Clean up existing views: 
   ```
   docker exec timeplus timeplusd client --user test --password test123 --query="DROP VIEW IF EXISTS <view_name>"
   ```
2. Check for existing views:
   ```
   docker exec timeplus timeplusd client --user test --password test123 --query="SELECT name FROM system.tables WHERE engine = 'MaterializedView'"
   ```

## About the Tests

The tests validate the alert acknowledgment functionality by:

1. Creating a test stream
2. Setting up a rule to monitor the stream
3. Generating test data that triggers an alert
4. Acknowledging the alert
5. Verifying the acknowledgment was stored and processed correctly

The implementation uses materialized views that write to a mutable stream for storing acknowledgments, which provides an elegant, reliable solution without requiring polling. 