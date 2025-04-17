# Timeplus Alert Gateway

A Go-based alerting system for Timeplus, allowing users to define SQL-based rules that trigger alerts when conditions are met.

## Features

- REST API for managing alert rules
- Rule lifecycle management (create, start, stop, delete)
- Alert acknowledgment and throttling
- Integration with Timeplus using the Proton native Go driver

## Getting Started

### Prerequisites

- Go 1.16 or higher
- Timeplus Proton instance (or Timeplus Cloud account) with API access

### Configuration

Create a configuration file `config.yaml` with your Timeplus credentials:

```yaml
server:
  port: "8080"
  allowedOrigins: "*"
  shutdownTimeout: 15

timeplus:
  address: "localhost" # Host address for Timeplus/Proton, without port or protocol
  apiKey: "your-api-key-here" # For Proton, this can be empty if no authentication
  workspace: "default"
```

### Building and Running

1. Build the application:

```
go build -o tp-alert-gateway ./cmd/server
```

2. Run the server:

```
./tp-alert-gateway --config config.yaml
```

## End-to-End Testing

The project includes comprehensive end-to-end tests that demonstrate the complete alert workflow.

### Temperature Monitoring Test

The `TestTemperatureAlertsE2E` test demonstrates a real-world temperature monitoring system. It:

1. Creates a temperature stream with device ID, temperature readings, and related metrics
2. Sets up an alert rule that triggers when temperature exceeds a threshold
3. Demonstrates alert generation, throttling, and acknowledgment
4. Shows how multiple devices can be monitored with the same rule

To run this test:

```bash
# Use the provided script for easy execution
./scripts/run_temperature_test.sh

# Or run it manually with Go test
go test -v ./pkg/e2e -run TestTemperatureAlertsE2E -skip=false
```

### Connection Diagnostics

To diagnose connection issues with Timeplus:

```bash
go test -v ./pkg/e2e -run TestTimeplusConnectionDiagnostics -skip=false
```

For more information about the tests, see the [E2E Test README](./pkg/e2e/README.md).

## API Reference

### Rules API

- `GET /api/rules` - Get all rules
- `POST /api/rules` - Create a new rule
- `GET /api/rules/{id}` - Get a specific rule
- `PUT /api/rules/{id}` - Update a rule
- `DELETE /api/rules/{id}` - Delete a rule
- `POST /api/rules/{id}/start` - Start a rule
- `POST /api/rules/{id}/stop` - Stop a rule
- `GET /api/rules/{ruleId}/alerts` - Get alerts for a specific rule

### Alerts API

- `GET /api/alerts` - Get all alerts
- `GET /api/alerts/{id}` - Get a specific alert
- `POST /api/alerts/{id}/acknowledge` - Acknowledge an alert

## Example Rule Creation

```json
{
  "name": "High CPU Usage",
  "description": "Alert when CPU usage exceeds 90%",
  "query": "SELECT * FROM cpu_metrics WHERE usage > 90",
  "severity": "critical",
  "throttleMinutes": 15,
  "sourceStream": "cpu_metrics"
}
```

## Connection to Timeplus

The application connects to Timeplus using the Proton Go driver via the native protocol on port 8464. This provides high-performance access to both streaming and historical data in Timeplus.

Alert states are maintained using materialized views that update in real-time when new data arrives. This eliminates the need for polling and provides a more efficient, event-driven architecture.

## License

MIT 