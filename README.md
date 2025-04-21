# Timeplus Alert Gateway

A Go-based alerting system for Timeplus, allowing users to define SQL-based rules that trigger alerts when conditions are met in streaming or historical data.

## Features

- REST API for managing alert rules
- Rule lifecycle management (create, start, stop, delete)
- Alert acknowledgment and throttling
- Support for entity-based alerting with throttling
- Integration with Timeplus using the Proton native Go driver

## Getting Started

### Prerequisites

- Go 1.16 or higher
- Timeplus Proton instance (or Timeplus Cloud account) with API access

### Configuration

Create a configuration file `config.yaml` with your Timeplus credentials:

```yaml
server:
  port: 8080  # Port for the alert gateway server
  allowedOrigins: "*"
  shutdownTimeout: 15  # Shutdown timeout in seconds

timeplus:
  address: "localhost:8464"  # Timeplus native protocol address with port
  username: "your-username"  # Username for Timeplus authentication
  password: "your-password"  # Password for Timeplus authentication
  workspace: "default"       # Timeplus workspace name
```

For local development, you can create a `config.local.yaml` file with test credentials.

### Building and Running

1. Build the application:

```
go build -o tp-alert-gateway ./cmd/server
```

2. Run the server:

```
./tp-alert-gateway --config config.yaml
```

## Creating Alert Rules

Alert rules define when and how alerts are triggered. Each rule consists of:

- A SQL query that defines the conditions for the alert
- Configuration for alert behavior (severity, throttling, etc.)
- Entity ID columns for grouping alerts by specific entities

### Rule Creation API

Rules are created by sending a POST request to the `/api/rules` endpoint:

```bash
curl -X POST http://localhost:8080/api/rules \
  -H "Content-Type: application/json" \
  -d '{
    "name": "High CPU Usage",
    "description": "Alert when CPU usage exceeds 90%",
    "query": "SELECT * FROM cpu_metrics WHERE usage > 90",
    "severity": "critical",
    "throttleMinutes": 15,
    "entityIdColumns": "host_id"
  }'
```

### Important Rule Properties

| Property | Description |
|----------|-------------|
| `name` | Human-readable name for the rule |
| `description` | Detailed description of the rule's purpose |
| `query` | SQL query that defines when alerts are triggered |
| `severity` | Alert severity ("info", "warning", or "critical") |
| `throttleMinutes` | Time in minutes before a new alert can be triggered for the same entity |
| `entityIdColumns` | Column(s) used to identify unique entities (comma-separated) |
| `dedicatedAlertAcksStream` | (Optional) Whether to use a dedicated stream for storing alert acknowledgments |

### SQL Query Guidelines

When writing queries for alert rules, follow these best practices:

1. Use direct stream references rather than the `table()` wrapper for streaming queries:
   ```sql
   SELECT * FROM network_logs WHERE status_code >= 500
   ```

2. Include all necessary fields in your SELECT statement to provide context for alerts.

3. Use simple conditions for better performance.

4. For stream-to-table joins, make sure the stream is on the left side of the join (stream to table). Table-to-stream joins are not supported.

### Example Alert Rules

#### Network Error Monitoring

```json
{
  "name": "HTTP 5xx Errors",
  "description": "Alert when HTTP 500 errors are detected",
  "query": "SELECT * FROM network_logs WHERE status_code >= 500",
  "severity": "critical",
  "throttleMinutes": 5,
  "entityIdColumns": "client_ip"
}
```

#### Temperature Monitoring

```json
{
  "name": "High Temperature Alert",
  "description": "Alert when any device temperature exceeds 30°C",
  "query": "SELECT * FROM device_temperatures WHERE temperature > 30",
  "severity": "warning",
  "throttleMinutes": 10,
  "entityIdColumns": "device_id"
}
```

#### Low Disk Space

```json
{
  "name": "Low Disk Space",
  "description": "Alert when disk space falls below 10%",
  "query": "SELECT * FROM disk_metrics WHERE free_percent < 10",
  "severity": "critical",
  "throttleMinutes": 60,
  "entityIdColumns": "host_id,mount_point"
}
```

## Alert Lifecycle Management

After creating a rule, it's automatically started. You can also:

- Start a rule: `POST /api/rules/{id}/start`
- Stop a rule: `POST /api/rules/{id}/stop`
- Get alerts: `GET /api/rules/{ruleId}/alerts`
- Acknowledge an alert: `POST /api/alerts/{id}/acknowledge`

## Common Limitations and Troubleshooting

- **Stream to Table Joins**: Table to stream joins are not currently supported. Use stream to table joins instead.
- **View Names**: If you encounter errors about view creation, check for name conflicts.
- **Alert Throttling**: If alerts are not triggering as expected, verify the `throttleMinutes` setting.

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

## Connection to Timeplus

The application connects to Timeplus using the Proton Go driver via the native protocol on port 8464. This provides high-performance access to both streaming and historical data in Timeplus.

Alert states are maintained using materialized views that update in real-time when new data arrives. This eliminates the need for polling and provides a more efficient, event-driven architecture.

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

## Recent Changes

### Simplified Rule Creation

The `sourceStream` field has been removed from rule definitions as it was redundant with the SQL query which already specifies the stream(s) being monitored. This simplifies the API and provides better support for complex queries that may reference multiple streams or use joins.

## License

MIT 