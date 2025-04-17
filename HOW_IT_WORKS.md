# How Timeplus Alert Gateway Works

## Alert Creation and Storage

### How Alerts are Created and Written to `tp_alerts`

1. **Rule Creation and Monitoring**:
   - When a rule is created, a dedicated materialized view is set up in Timeplus (e.g., `rule_{id}_view`)
   - This materialized view contains the query that monitors data streams for the alert condition
   - The materialized view is set up with throttling to prevent excessive alerts (see `CreateRuleView` function which uses `EMIT CHANGES EVERY {throttle} SECONDS`)
   - A second materialized view (`rule_{id}_acks_view`) is created to write to the `tp_alert_acks_mutable` stream

2. **Alert Generation Process**:
   - There are two ways alerts are managed in the system:
     
     a) **Mutable Stream Method (Current Implementation)**:
     - A materialized view is created for each rule that writes directly to `tp_alert_acks_mutable`
     - This view automatically inserts records with device_id and rule_id keys when alerts are triggered
     - This creates a state tracking system that can be queried and updated without polling
     - No Go routines or polling are used in this approach - it's fully event-driven

     b) **Manual Alert Creation (API-Driven)**:
     - Alerts can also be manually created through the API using the `CreateAlertFromData` method
     - This is useful for integrating with external systems or for testing
   
3. **Materialized View for Alert State**:
   - When a rule is started, a materialized view is created that inserts into `tp_alert_acks_mutable`
   - The view is created with this query (from `timeplus.GetAlertAcksViewQuery`):
   ```sql
   CREATE MATERIALIZED VIEW rule_{rule_id}_acks_view INTO tp_alert_acks_mutable AS 
   SELECT 
       '{rule_id}' AS rule_id,
       device_id,
       'active' AS state,
       now() AS created_at,
       now() AS updated_at,
       '' AS updated_by,
       '' AS comment
   FROM {result_stream}
   ```
   - This automatically maintains a state table with active alerts for each device/rule pair
   - When alerts need to be acknowledged, a simple INSERT is made to the mutable stream with the same keys but a new state

### Alert Storage

Alerts are stored in two streams:

1. **tp_alerts** (Historical record of all alerts):
   - `id`: A unique identifier for the alert
   - `rule_id`: The ID of the rule that triggered the alert
   - `rule_name`: The name of the rule
   - `severity`: The severity level (info/warning/critical)
   - `triggered_at`: When the alert was triggered
   - `data`: A JSON string containing the data that caused the alert
   - `acknowledged`: Boolean flag indicating if the alert has been acknowledged
   - `acknowledged_at`: Timestamp when the alert was acknowledged (nullable)
   - `acknowledged_by`: Who acknowledged the alert (nullable)

2. **tp_alert_acks_mutable** (Current state of alerts with primary keys):
   - `rule_id`: The ID of the rule (part of primary key)
   - `device_id`: The device identifier from the alert data (part of primary key)
   - `state`: The current state (active/acknowledged/silenced/resolved)
   - `created_at`: When the record was created
   - `updated_at`: When the record was last updated
   - `updated_by`: Who last updated the record (nullable)
   - `comment`: Optional comment for the update (nullable)

### Alert Acknowledgement

When an alert is acknowledged:
1. The `AcknowledgeDevice` function in `rule_service.go` is called
2. It checks for active alerts for the device/rule combination
3. It inserts a new record into the `tp_alert_acks_mutable` stream with state="acknowledged"
4. Due to the primary key constraint, this new record replaces the "active" state record

This approach leverages Timeplus's mutable streams feature to maintain state without requiring constant polling, making the system more efficient and scalable. The removal of polling Go routines simplifies the codebase and reduces resource usage. 