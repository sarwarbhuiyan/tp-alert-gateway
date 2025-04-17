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
		ELSE toString(now64(3)) -- Fallback: use timestamp as unique ID if no identifying field exists
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

// GetRuleThrottledMaterializedViewQuery returns a SQL query to create a materialized view
// that joins the rule view with the alert acks mutable stream for throttling.
func GetRuleThrottledMaterializedViewQuery(ruleID string, throttleMinutes int, idColumnName string) string {
	// Sanitize the rule ID for view name
	sanitizedRuleID := strings.ReplaceAll(ruleID, "-", "_")
	plainViewName := fmt.Sprintf("rule_%s_view", sanitizedRuleID)
	mvName := fmt.Sprintf("rule_%s_mv", sanitizedRuleID)

	// Throttling is handled by the mutable stream's primary key.
	// No need for throttleDuration variable here anymore.

	query := fmt.Sprintf(`
CREATE MATERIALIZED VIEW %s INTO %s AS
SELECT
    '%s' AS rule_id,
    entity_id, -- Already concatenated in the plain view
    '%s' AS state,
    now() AS created_at,
    now() AS updated_at,
    '' AS updated_by,
    '' AS comment
FROM %s
`,
		// Arguments for fmt.Sprintf
		mvName,                 // 1. MV name
		AlertAcksMutableStream, // 2. INTO target (mutable)
		ruleID,                 // 3. SELECT '%s' AS rule_id
		// entity_id comes directly from plainViewName now
		AlertStateActive, // 4. SELECT '%s' AS state
		plainViewName,    // 5. FROM %s (the plain rule view which includes entity_id)
	)

	return query
}
