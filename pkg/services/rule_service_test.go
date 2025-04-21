package services

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	"github.com/timeplus-io/tp-alert-gateway/pkg/models"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

func setupTestClient(t *testing.T) timeplus.TimeplusClient {
	cfg := &config.TimeplusConfig{
		Address:   "localhost:8464",
		Username:  "test",
		Password:  "test123",
		Workspace: "default",
	}

	client, err := timeplus.NewClient(cfg)
	require.NoError(t, err)
	return client
}

// TestCreateRule tests rule creation
func TestCreateRule(t *testing.T) {
	client := setupTestClient(t)
	service, err := NewRuleService(client)
	require.NoError(t, err)

	ctx := context.Background()

	tests := []struct {
		name    string
		req     *models.CreateRuleRequest
		wantErr bool
	}{
		{
			name: "valid rule",
			req: &models.CreateRuleRequest{
				Name:        "Test Rule",
				Description: "Test Description",
				Query:       "SELECT * FROM network_logs WHERE status_code >= 500",
				Severity:    "critical",
			},
			wantErr: false,
		},
		{
			name: "invalid rule - missing name",
			req: &models.CreateRuleRequest{
				Description: "Test Description",
				Query:       "SELECT * FROM network_logs",
				Severity:    "critical",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule, err := service.CreateRule(ctx, tt.req)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, rule)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, rule)
				assert.NotEmpty(t, rule.ID)
				assert.Equal(t, tt.req.Name, rule.Name)
				assert.Equal(t, tt.req.Description, rule.Description)
				assert.Equal(t, tt.req.Query, rule.Query)
				assert.Equal(t, tt.req.Severity, rule.Severity)

				// Cleanup
				err = service.DeleteRule(ctx, rule.ID)
				assert.NoError(t, err)
			}
		})
	}
}

// TestStartStopRule tests starting and stopping a rule
func TestStartStopRule(t *testing.T) {
	client := setupTestClient(t)
	service, err := NewRuleService(client)
	require.NoError(t, err)

	ctx := context.Background()

	// Create a rule first
	rule, err := service.CreateRule(ctx, &models.CreateRuleRequest{
		Name:        "Test Rule",
		Description: "Test Description",
		Query:       "SELECT * FROM network_logs WHERE status_code >= 500",
		Severity:    "critical",
	})
	require.NoError(t, err)
	require.NotNil(t, rule)

	defer func() {
		// Cleanup
		err = service.DeleteRule(ctx, rule.ID)
		assert.NoError(t, err)
	}()

	// Test starting the rule
	err = service.StartRule(ctx, rule.ID)
	assert.NoError(t, err)

	// Verify the rule is running
	runningRule, err := service.GetRule(rule.ID)
	assert.NoError(t, err)
	assert.Equal(t, models.RuleStatusRunning, runningRule.Status)

	// Test stopping the rule
	err = service.StopRule(ctx, rule.ID)
	assert.NoError(t, err)

	// Verify the rule is stopped
	stoppedRule, err := service.GetRule(rule.ID)
	assert.NoError(t, err)
	assert.Equal(t, models.RuleStatusStopped, stoppedRule.Status)
}

// TestAcknowledgeAlert tests alert acknowledgment
func TestAcknowledgeAlert(t *testing.T) {
	client := setupTestClient(t)
	service, err := NewRuleService(client)
	require.NoError(t, err)

	ctx := context.Background()

	// Create a rule first
	rule, err := service.CreateRule(ctx, &models.CreateRuleRequest{
		Name:        "Test Rule",
		Description: "Test Description",
		Query:       "SELECT * FROM network_logs WHERE status_code >= 500",
		Severity:    "critical",
	})
	require.NoError(t, err)
	require.NotNil(t, rule)

	defer func() {
		// Cleanup
		err = service.DeleteRule(ctx, rule.ID)
		assert.NoError(t, err)
	}()

	// Create an alert
	alertID, err := service.CreateAlertFromData(ctx, rule, "test-entity", map[string]interface{}{
		"status_code": 500,
		"path":        "/test",
	})
	require.NoError(t, err)
	require.NotEmpty(t, alertID)

	// Test acknowledging the alert
	err = service.AcknowledgeAlert(alertID, "test-user")
	assert.NoError(t, err)

	// Verify the alert is acknowledged
	alert, err := service.GetAlert(alertID)
	assert.NoError(t, err)
	assert.True(t, alert.Acknowledged)
	assert.Equal(t, "test-user", alert.AcknowledgedBy)
	assert.NotNil(t, alert.AcknowledgedAt)
}

func TestQueryFormatting(t *testing.T) {
	// Create a test service with fake stream names
	service := &RuleService{
		ruleStream:  "test_rules",
		alertStream: "test_alerts",
	}

	// Test rule query formatting - removed source_stream field
	ruleQuery := fmt.Sprintf(`
		SELECT id, name, description, query, status, severity, 
		       throttle_minutes, created_at, updated_at, last_triggered_at,
		       result_stream, view_name, last_error
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

func TestAcknowledgeDeviceWithMock(t *testing.T) {
	// Create a mock client
	mockClient := new(MockClient)

	// 1. Mock for GetActiveAlertAcks
	mockActiveAcks := []map[string]interface{}{
		{
			"rule_id":   "rule1",
			"entity_id": "device_123",
			"state":     timeplus.AlertStateActive,
		},
	}

	// Match any query containing SELECT and state = 'active'
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "SELECT") &&
			strings.Contains(query, "state = '"+timeplus.AlertStateActive+"'")
	})).Return(mockActiveAcks, nil)

	// 2. Mock for the acknowledgment INSERT
	// Match any query containing INSERT
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "INSERT")
	})).Return([]map[string]interface{}{}, nil)

	// Create a rule service with the mock client
	service := &RuleService{
		tpClient:    mockClient,
		ruleStream:  "tp_rules",
		alertStream: "tp_alerts",
	}

	// Test acknowledging a device directly
	ctx := context.Background()
	err := service.AcknowledgeDevice(ctx, "rule1", "device_123", "test-user", "Test comment")
	assert.NoError(t, err)

	// Verify that all expected mock calls were made
	mockClient.AssertExpectations(t)
}

func TestAlertAcknowledgmentWithMock(t *testing.T) {
	// Create a mock client
	mockClient := new(MockClient)

	// 1. Mock for rule query
	mockRuleData := []map[string]interface{}{
		{
			"id":       "rule1",
			"name":     "Test Rule",
			"severity": "critical",
		},
	}

	// Match rule queries - used by GetRule
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "id = 'rule1'")
	})).Return(mockRuleData, nil)

	// 2. Mock for GetActiveAlertAcks
	mockActiveAcks := []map[string]interface{}{
		{
			"rule_id":   "rule1",
			"entity_id": "device_123",
			"state":     timeplus.AlertStateActive,
		},
	}

	// Match active alerts query
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "state = '"+timeplus.AlertStateActive+"'")
	})).Return(mockActiveAcks, nil)

	// 3. Mock for the acknowledgment INSERT
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return strings.Contains(query, "INSERT")
	})).Return([]map[string]interface{}{}, nil)

	// Create a rule service with the mock client
	service := &RuleService{
		tpClient:    mockClient,
		ruleStream:  "tp_rules",
		alertStream: "tp_alerts",
	}

	// Test acknowledging an alert
	err := service.AcknowledgeAlert("rule1:device_123", "test-user")
	assert.NoError(t, err)

	// Verify that all expected mock calls were made
	mockClient.AssertExpectations(t)
}
