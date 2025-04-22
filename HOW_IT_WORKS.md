# Timeplus Alert Gateway - How It Works

This document explains the key components and processes of the Timeplus Alert Gateway, including rule creation, generated SQL, APIs, and stream management.

## Rule Creation Process

### Rule Definition

Rules are defined using a JSON structure that includes:

- **name**: The name of the alert rule
- **description**: Description of what the rule monitors
- **query**: The SQL query that defines conditions for triggering alerts
- **severity**: The severity level (info, warning, critical)
- **throttleMinutes**: Time period to throttle repeated alerts
- **entityIdColumns**: Column(s) used as entity identifiers (comma-separated for multiple columns)

Example rule definition:
```json
{
  "name": "High Temperature Alert",
  "description": "Alert when device temperature exceeds 30°C",
  "query": "SELECT device_id, temperature FROM device_temperatures WHERE temperature > 30",
  "severity": "warning",
  "throttleMinutes": 5,
  "entityIdColumns": "device_id"
}
```

### Rule Creation Flow

1. The rule is submitted via the API
2. A unique rule ID is generated
3. The rule is persisted to the `tp_rules` mutable stream in Timeplus
4. The rule is automatically started (asynchronously)

## Generated Timeplus SQL

When a rule is started, the system generates and executes several SQL queries:

### 1. Plain View Creation

```sql
CREATE VIEW rule_{sanitized_rule_id}_view AS {user_provided_query}
```

This view contains the results of the user's query condition.

### 2. Entity ID Handling

If multiple columns are specified for `entityIdColumns`, a concatenation is performed:

```sql
CREATE VIEW rule_{sanitized_rule_id}_view AS 
SELECT *, concat({column1}, ':', {column2}, ':', ...) AS entity_id 
FROM ({original_query})
```

### 3. Materialized View Creation

The materialized view is complex and contains logic for alert management, throttling, and acknowledgments:

```sql
CREATE MATERIALIZED VIEW default.rule_{sanitized_rule_id}_mv INTO default.tp_alert_acks_mutable
(
  `rule_id` string,
  `entity_id` string,
  `state` string,
  `created_at` datetime64(3),
  `updated_at` datetime,
  `updated_by` string,
  `comment` string,
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
  `_tp_sn` int64
) AS
WITH filtered_events AS
  (
    SELECT
      view.*, ack.state AS ack_state, ack.created_at AS ack_created_at
    FROM
      default.rule_{sanitized_rule_id}_view AS view
    LEFT JOIN default.tp_alert_acks_mutable AS ack ON view.{entity_id_column} = ack.entity_id
    WHERE
      (ack.rule_id = '') OR ((ack.rule_id = '{rule_id}') AND ((ack_state = '') OR (ack_state = 'acknowledged') OR ((now() - {throttle}m) > ack.created_at)))
  )
SELECT
  '{rule_id}' AS rule_id, 
  fe.{entity_id_column} AS entity_id, 
  'active' AS state, 
  coalesce(fe.ack_created_at, now()) AS created_at, 
  now() AS updated_at, 
  '' AS updated_by, 
  concat('{', array_string_concat([{json_fields}], ', '), '}') AS comment
FROM
  filtered_events AS fe
```

This materialized view:
- Writes results to the alert acknowledgment mutable stream
- Performs a LEFT JOIN to check for existing acknowledgments
- Filters based on acknowledgment state and throttling period
- Generates a JSON comment with relevant event data
- Includes state tracking for alert lifecycle

## Stream Architecture

The system uses several Timeplus streams:

### Core Streams

1. **tp_rules**: Mutable stream that stores rule definitions
   ```sql
   CREATE MUTABLE STREAM tp_rules (
     id string,
     name string,
     description string,
     query string,
     status string,
     severity string,
     throttle_minutes int32,
     entity_id_columns string,
     created_at datetime64,
     updated_at datetime64,
     last_triggered_at datetime64 NULL,
     result_stream string,
     view_name string,
     last_error string NULL,
     dedicated_alert_acks_stream bool NULL,
     alert_acks_stream_name string NULL,
     _tp_time datetime64,
     active bool
   ) PRIMARY KEY (id)
   ```

2. **tp_alerts**: Stream that stores triggered alerts
   ```sql
   CREATE STREAM tp_alerts (
     id string,
     rule_id string,
     rule_name string,
     severity string,
     triggered_at datetime64,
     data string,
     acknowledged bool,
     acknowledged_at datetime64 NULL,
     acknowledged_by string NULL,
     _tp_time datetime64
   )
   ```

3. **tp_alert_acks_mutable**: Global mutable stream for alert acknowledgments
   ```sql
   CREATE MUTABLE STREAM tp_alert_acks_mutable (
     rule_id string,
     entity_id string,
     state string,
     created_at datetime64,
     updated_at datetime,
     updated_by string,
     comment string,
     _tp_time datetime64,
     _tp_sn int64
   ) PRIMARY KEY (rule_id, entity_id)
   ```

### Per-Rule Streams

For each rule, the system can create:

1. **rule_{rule_id}_results**: Results of each rule's query
2. **rule_{rule_id}_alert_acks**: Dedicated acknowledgment stream (optional)

## Alert Flow

1. The materialized view detects new data matching the rule conditions
2. The system checks if alerts should be throttled based on the throttleMinutes setting
3. Existing acknowledgments are checked to prevent duplicate alerts
4. If conditions are met, a new alert record is inserted into the `tp_alert_acks_mutable` stream with state="active"

## Acknowledgment System

The system supports acknowledgments to suppress alerts for specific entities:

1. Global acknowledgments stored in `tp_alert_acks_mutable`
2. Per-rule acknowledgments in dedicated streams (if enabled)
3. Acknowledgments include:
   - State (active, acknowledged)
   - Who acknowledged
   - When acknowledged
   - Optional comment with event data

## API Endpoints

The alert gateway provides the following APIs:

- **GET /rules**: List all rules
- **GET /rules/{id}**: Get a specific rule
- **POST /rules**: Create a new rule
- **PUT /rules/{id}**: Update a rule
- **DELETE /rules/{id}**: Delete a rule
- **POST /rules/{id}/start**: Start a rule
- **POST /rules/{id}/stop**: Stop a rule
- **GET /rules/{id}/alerts**: Get alerts for a rule
- **GET /alerts/{id}**: Get a specific alert
- **POST /alerts/{id}/acknowledge**: Acknowledge an alert
- **POST /rules/{id}/acks**: Acknowledge all alerts for an entity ID

## Timeplus Integration

The system uses the Timeplus Proton client to interact with Timeplus:

```go
conn, err := proton.Open(&proton.Options{
    Address:  fmt.Sprintf("%s:%d", host, port),
    Username: username,
    Password: password,
    Database: database,
})
```

The client then uses direct methods like `conn.Exec()` and `conn.Query()` for all Timeplus operations, following best practices for working with materialized views and streaming data.
