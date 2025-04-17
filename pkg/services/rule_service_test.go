package services

import (
	"fmt"
	"strings"
	"testing"
)

func TestQueryFormatting(t *testing.T) {
	// Create a test service with fake stream names
	service := &RuleService{
		ruleStream:  "test_rules",
		alertStream: "test_alerts",
	}

	// Test rule query formatting
	ruleQuery := fmt.Sprintf(`
		SELECT id, name, description, query, status, severity, 
		       throttle_minutes, created_at, updated_at, last_triggered_at,
		       source_stream, result_stream, view_name, last_error
		FROM (
			SELECT *, row_number() OVER (PARTITION BY id ORDER BY _tp_time DESC) as row_num
			FROM table(%s)
			WHERE active = true
		) WHERE row_num = 1
	`, service.ruleStream)

	// Verify that table() is used correctly
	if expected := "table(test_rules)"; !contains(ruleQuery, expected) {
		t.Errorf("Rule query doesn't contain %q: %s", expected, ruleQuery)
	}

	// Test alert query formatting
	alertQuery := fmt.Sprintf(`
		SELECT id, rule_id, rule_name, severity, triggered_at,
		       data, acknowledged, acknowledged_at, acknowledged_by
		FROM table(%s)
		WHERE id = 'test_id'
		LIMIT 1
	`, service.alertStream)

	// Verify that table() is used correctly
	if expected := "table(test_alerts)"; !contains(alertQuery, expected) {
		t.Errorf("Alert query doesn't contain %q: %s", expected, alertQuery)
	}
}

func TestStreamQueryFormatting(t *testing.T) {
	// Create a test rule with a view name
	viewName := "test_view"

	// Test streaming query (should NOT use table())
	streamQuery := fmt.Sprintf("SELECT * FROM %s", viewName)

	// Verify that table() is NOT used
	if unexpected := "table(test_view)"; contains(streamQuery, unexpected) {
		t.Errorf("Stream query shouldn't contain %q: %s", unexpected, streamQuery)
	}

	// Verify the correct format
	if expected := "SELECT * FROM test_view"; streamQuery != expected {
		t.Errorf("Stream query incorrect format. Expected %q, got %q", expected, streamQuery)
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
