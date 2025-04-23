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
			ack_state = '' OR
			ack_state = '%s' OR
			(now() - %dm > ack.created_at)
		)`, AlertStateAcknowledged, ThrottleMinutes)
	} else {
		// If ThrottleMinutes is negative (e.g., -1), effectively disable throttling beyond the initial trigger
		throttleCondition = "ack_state = ''"
	}

	// Use CTE to resolve potential column name conflicts and clarify logic
	query := fmt.Sprintf(`
CREATE MATERIALIZED VIEW `+"`%s`"+` INTO `+"`%s`"+` AS
WITH filtered_events AS (
    SELECT
        view.*,
        ack.state AS ack_state,
        ack.created_at AS ack_created_at
    FROM `+"`%s`"+` AS view
    LEFT JOIN `+"`%s`"+` AS ack ON view.`+"`%s`"+` = ack.entity_id
    WHERE (ack.rule_id = '') OR (ack.rule_id = '%s' AND (%s))
)
SELECT
    '%s' AS rule_id,
    fe.`+"`%s`"+` AS entity_id,
    '%s' AS state,
    coalesce(fe.ack_created_at, now()) AS created_at,
    now() AS updated_at,
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

// GetRuleResolveViewQuery generates a SQL query for creating a materialized view
// that will automatically acknowledge alerts when a resolve condition is met
func GetRuleResolveViewQuery(
	ruleID string,
	idColumnName string,
	targetAlertStream string, // The alert ack stream name
) string {
	sanitizedRuleID := strings.ReplaceAll(ruleID, "-", "_")
	viewName := fmt.Sprintf("rule_%s_view", sanitizedRuleID)
	mvName := fmt.Sprintf("rule_%s_resolve_mv", sanitizedRuleID)

	// Create a view that inserts records with 'acknowledged' state
	// based on the resolve query results
	query := fmt.Sprintf(`
CREATE MATERIALIZED VIEW `+"`%s`"+` INTO `+"`%s`"+` AS
SELECT
    '%s' AS rule_id,
    `+"`%s`"+` AS entity_id,
    '%s' AS state,
    now() AS created_at,
    now() AS updated_at,
    'auto-resolver' AS updated_by,
    '{"reason": "Auto-resolved by resolve query"}' AS comment
FROM `+"`%s`"+``,
		mvName, targetAlertStream, // View name and target stream
		ruleID,                 // rule_id for INSERT
		idColumnName,           // entity_id column from resolve query
		AlertStateAcknowledged, // Set state to acknowledged
		viewName)               // Source view with resolve query

	return query
}
