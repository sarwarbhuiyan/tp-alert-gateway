package timeplus

import (
	"fmt"
	"strings"
)

// Stream names
const (
	// AlertsStream is the name of the stream that stores alerts
	AlertsStream = "tp_alerts"

	// RulesStream is the name of the stream that stores rule definitions
	RulesStream = "tp_rules"

	// AlertAcksStream is the name of the stream that tracks alert acknowledgments
	AlertAcksStream = "tp_alert_acks"

	// AlertAcksMutableStream is the name of the mutable stream that stores alert acknowledgments
	AlertAcksMutableStream = "tp_alert_acks_mutable"
)

// GetAlertsSchema returns the schema for the alerts stream
func GetAlertsSchema() []Column {
	return []Column{
		{Name: "id", Type: "string"},
		{Name: "rule_id", Type: "string"},
		{Name: "rule_name", Type: "string"},
		{Name: "severity", Type: "string"},
		{Name: "triggered_at", Type: "datetime64"},
		{Name: "data", Type: "string"}, // JSON string of the event data
		{Name: "acknowledged", Type: "bool"},
		{Name: "acknowledged_at", Type: "datetime64", Nullable: true},
		{Name: "acknowledged_by", Type: "string"},
	}
}

// GetRulesSchema returns the schema for the rules stream
func GetRulesSchema() []Column {
	return []Column{
		{Name: "id", Type: "string"},
		{Name: "name", Type: "string"},
		{Name: "description", Type: "string"},
		{Name: "query", Type: "string"},
		{Name: "status", Type: "string"},
		{Name: "severity", Type: "string"},
		{Name: "throttle_minutes", Type: "int32"},
		{Name: "created_at", Type: "datetime64"},
		{Name: "updated_at", Type: "datetime64"},
		{Name: "last_triggered_at", Type: "datetime64", Nullable: true},
		{Name: "source_stream", Type: "string"},
		{Name: "result_stream", Type: "string"},
		{Name: "view_name", Type: "string"},
		{Name: "last_error", Type: "string", Nullable: true},
	}
}

// GetAlertAcksSchema returns the schema for the alert acknowledgments stream
// Note: entity_id is a generic identifier for the entity that triggered the alert
// This could be any meaningful identifier from your data sources (device IDs, IP addresses, user IDs, etc.)
func GetAlertAcksSchema() []Column {
	return []Column{
		{Name: "rule_id", Type: "string"},
		{Name: "entity_id", Type: "string"}, // Generic identifier for the entity that triggered the alert
		{Name: "state", Type: "string"},
		{Name: "created_at", Type: "datetime64"},
		{Name: "updated_at", Type: "datetime64"},
		{Name: "updated_by", Type: "string", Nullable: true},
		{Name: "comment", Type: "string", Nullable: true},
	}
}

// GetMutableAlertAcksSchema returns the schema for the mutable alert acknowledgments stream
// The schema is generic and can accommodate any type of alert source, not just device data
func GetMutableAlertAcksSchema() []Column {
	return []Column{
		{Name: "rule_id", Type: "string"},
		{Name: "entity_id", Type: "string"}, // Generic identifier for the entity that triggered the alert
		{Name: "state", Type: "string"},
		{Name: "created_at", Type: "datetime64"},
		{Name: "updated_at", Type: "datetime64"},
		{Name: "updated_by", Type: "string", Nullable: true},
		{Name: "comment", Type: "string", Nullable: true},
	}
}

// GetRuleAlertViewQuery returns a SQL query to create a materialized view that tracks alerts for a rule with throttling
func GetRuleAlertViewQuery(ruleID, ruleName, severity, sourceStream, whereClause string) string {
	return fmt.Sprintf(`SELECT 
        *,
        '%s' AS rule_id,
        '%s' AS rule_name, 
        '%s' AS severity,
        now() AS triggered_at
FROM %s 
WHERE %s`,
		ruleID, ruleName, severity, sourceStream, whereClause)
}

// GetCreateMutableStreamQuery returns a SQL query to create a mutable stream
func GetCreateMutableStreamQuery(streamName string, columns []Column, primaryKeys []string) string {
	columnsSQL := ""
	for i, col := range columns {
		if i > 0 {
			columnsSQL += ",\n"
		}
		nullableStr := ""
		if col.Nullable {
			nullableStr = " NULL"
		}
		columnsSQL += fmt.Sprintf("  `%s` %s%s", col.Name, col.Type, nullableStr)
	}

	primaryKeySQL := ""
	if len(primaryKeys) > 0 {
		primaryKeySQL = "PRIMARY KEY ("
		for i, key := range primaryKeys {
			if i > 0 {
				primaryKeySQL += ", "
			}
			primaryKeySQL += fmt.Sprintf("`%s`", key)
		}
		primaryKeySQL += ")"
	}

	return fmt.Sprintf(`CREATE MUTABLE STREAM `+"`"+`%s`+"`"+` (
%s
) %s`, streamName, columnsSQL, primaryKeySQL)
}

// GetAlertAcksViewQuery returns a SQL query to create a materialized view that updates the mutable alert_acks stream
// Note: This query expects the source stream to have a field that can be used as an entity_id
// The schema of the materialized view is dynamically extended based on the query results
// If there is no entity_id field in the results, it will create one from available fields
func GetAlertAcksViewQuery(ruleID, sourceStream string) string {
	// Sanitize the rule ID for view name
	sanitizedRuleID := strings.ReplaceAll(ruleID, "-", "_")

	// Import the AlertStateActive constant from alert_acks.go
	return fmt.Sprintf(`
CREATE MATERIALIZED VIEW rule_%s_acks_view INTO %s AS 
SELECT 
	'%s' AS rule_id,
	CASE 
		WHEN hasColumn('entity_id') AND isNotNull(entity_id) THEN entity_id
		WHEN hasColumn('device_id') AND isNotNull(device_id) THEN device_id
		WHEN hasColumn('ip') AND isNotNull(ip) THEN ip
		WHEN hasColumn('host') AND isNotNull(host) THEN host
		WHEN hasColumn('user_id') AND isNotNull(user_id) THEN user_id 
		WHEN hasColumn('id') AND isNotNull(id) THEN id
		ELSE toString(now64(3)) 
	END AS entity_id,
	'active' AS state,
	now() AS created_at,
	now() AS updated_at,
	'' AS updated_by,
	'' AS comment
FROM %s
`, sanitizedRuleID, AlertAcksMutableStream, ruleID, sourceStream)
}

// GetAlertSchema returns the schema for the alerts stream
func GetAlertSchema() []Column {
	return []Column{
		{Name: "id", Type: "string", Nullable: false},
		{Name: "rule_id", Type: "string", Nullable: false},
		{Name: "rule_name", Type: "string", Nullable: false},
		{Name: "severity", Type: "string", Nullable: false},
		{Name: "triggered_at", Type: "datetime", Nullable: false},
		{Name: "data", Type: "string", Nullable: false}, // JSON string with all event data
		{Name: "acknowledged", Type: "bool", Nullable: false},
		{Name: "acknowledged_at", Type: "datetime", Nullable: true},
		{Name: "acknowledged_by", Type: "string", Nullable: true},
	}
}

// GetRulePlainViewQuery returns a SQL query to create a regular view for a rule
// This view doesn't store any state and simply represents the rule query
func GetRulePlainViewQuery(ruleID, ruleQuery string) string {
	// Sanitize the rule ID for view name
	sanitizedRuleID := strings.ReplaceAll(ruleID, "-", "_")
	viewName := fmt.Sprintf("rule_%s_view", sanitizedRuleID)

	return fmt.Sprintf("CREATE VIEW %s AS %s", viewName, ruleQuery)
}

// GetRuleThrottledMaterializedViewQuery generates the SQL query for creating a materialized view
// that feeds into a specified rule-specific alert ack stream and includes throttling logic, using a CTE.
func GetRuleThrottledMaterializedViewQuery(
	ruleID string,
	ThrottleMinutes int,
	idColumnName string,
	triggeringDataExpr string, // SQL expression for the comment field (e.g., a JSON string)
	targetAlertStream string, // The rule-specific alert ack stream name
) string {
	sanitizedRuleID := strings.ReplaceAll(ruleID, "-", "_")
	viewName := fmt.Sprintf("rule_%s_view", sanitizedRuleID)
	mvName := fmt.Sprintf("rule_%s_mv", sanitizedRuleID)

	// Throttling condition using Timeplus interval syntax, referencing aliased ack columns
	throttleCondition := "ack_state = ''" // Always trigger if no previous state
	if ThrottleMinutes >= 0 {             // Apply user logic if throttle is enabled (>= 0)
		throttleCondition = fmt.Sprintf(`(
			ack_state IS NULL OR
			ack_state = '%s' OR
			now() - %dm > view._tp_time
		)`, AlertStateAcknowledged, ThrottleMinutes)
	} else {
		// If ThrottleMinutes is negative (e.g., -1), effectively disable throttling beyond the initial trigger
		throttleCondition = "ack_state = ''"
	}

	// Use CTE to resolve potential column name conflicts and clarify logic
	query := fmt.Sprintf(`
CREATE MATERIALIZED VIEW %s INTO %s AS
WITH filtered_events AS (
    SELECT
        view.*,
        ack.state AS ack_state,
        ack.created_at AS ack_created_at
    FROM %s AS view
    LEFT JOIN %s AS ack ON view.%s = ack.entity_id
    WHERE (ack.rule_id = '') OR (ack.rule_id = '%s' AND (%s))
)
SELECT
    '%s' AS rule_id,
    fe.%s AS entity_id,
    '%s' AS state,
    fe._tp_time AS created_at,
    fe._tp_time AS updated_at,
    '' AS updated_by,
    %s AS comment
FROM filtered_events AS fe`,
		mvName, targetAlertStream, // Use parameterized target stream
		viewName,           // Source view for CTE
		targetAlertStream,  // Join with parameterized target stream
		idColumnName,       // Join column entity_id
		ruleID,             // Rule ID for WHERE clause
		throttleCondition,  // Throttle condition for WHERE clause
		ruleID,             // rule_id for final SELECT
		idColumnName,       // entity_id for final SELECT
		AlertStateActive,   // state for final SELECT
		triggeringDataExpr) // comment expression for final SELECT

	return query
}
