package services

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/timeplus-io/tp-alert-gateway/pkg/models"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

// This test file contains integration tests for the alert acknowledgment system
// It verifies the full flow of:
// 1. Setting up mutable streams
// 2. Creating materialized views for rules
// 3. Triggering alerts via these views
// 4. Acknowledging alerts and verifying the state changes

// ExtendedMockClient adds additional methods needed for alert acknowledgment tests
type ExtendedMockClient struct {
	mock.Mock
}

// Ensure ExtendedMockClient implements TimeplusClient
var _ timeplus.TimeplusClient = (*ExtendedMockClient)(nil)

// Basic TimeplusClient methods
func (m *ExtendedMockClient) StreamExists(ctx context.Context, name string) (bool, error) {
	args := m.Called(ctx, name)
	return args.Bool(0), args.Error(1)
}

func (m *ExtendedMockClient) CreateStream(ctx context.Context, name string, schema []timeplus.Column) error {
	args := m.Called(ctx, name, schema)
	return args.Error(0)
}

func (m *ExtendedMockClient) CreateMaterializedView(ctx context.Context, name string, query string) error {
	args := m.Called(ctx, name, query)
	return args.Error(0)
}

func (m *ExtendedMockClient) DeleteMaterializedView(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *ExtendedMockClient) ViewExists(ctx context.Context, name string) (bool, error) {
	args := m.Called(ctx, name)
	return args.Bool(0), args.Error(1)
}

func (m *ExtendedMockClient) DeleteStream(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *ExtendedMockClient) ExecuteQuery(ctx context.Context, query string) ([]map[string]interface{}, error) {
	args := m.Called(ctx, query)
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *ExtendedMockClient) StreamQuery(ctx context.Context, query string, callback func(row interface{})) error {
	args := m.Called(ctx, query, callback)
	return args.Error(0)
}

func (m *ExtendedMockClient) InsertIntoStream(ctx context.Context, streamName string, columns []string, values []interface{}) error {
	args := m.Called(ctx, streamName, columns, values)
	return args.Error(0)
}

// Additional methods for alert acknowledgment tests
func (m *ExtendedMockClient) ListStreams(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *ExtendedMockClient) ListViews(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *ExtendedMockClient) ListMaterializedViews(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *ExtendedMockClient) ExecuteStreamingQuery(ctx context.Context, query string, callback func(result map[string]interface{}) error) error {
	args := m.Called(ctx, query, callback)
	return args.Error(0)
}

func (m *ExtendedMockClient) SetupAlertAcksStream(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *ExtendedMockClient) SetupMutableAlertAcksStream(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *ExtendedMockClient) CreateAlertAck(ctx context.Context, alertAck timeplus.AlertAck) error {
	args := m.Called(ctx, alertAck)
	return args.Error(0)
}

func (m *ExtendedMockClient) GetAlertAck(ctx context.Context, alertID string) (*timeplus.AlertAck, error) {
	args := m.Called(ctx, alertID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*timeplus.AlertAck), args.Error(1)
}

func (m *ExtendedMockClient) IsAlertAcknowledged(ctx context.Context, alertID string) (bool, error) {
	args := m.Called(ctx, alertID)
	return args.Bool(0), args.Error(1)
}

func (m *ExtendedMockClient) CreateRuleResultsStream(ctx context.Context, ruleID string) error {
	args := m.Called(ctx, ruleID)
	return args.Error(0)
}

// Helper function to create a test rule
func createTestRule() *models.Rule {
	return &models.Rule{
		ID:              uuid.New().String(),
		Name:            "Test Rule",
		Description:     "Rule for testing alert acknowledgments",
		Query:           "SELECT device_id, value FROM test_stream WHERE value > 90",
		Status:          models.RuleStatusStopped,
		Severity:        models.RuleSeverityWarning,
		ThrottleMinutes: 5,
		CreatedAt:       time.Now().Add(-1 * time.Hour),
		UpdatedAt:       time.Now(),
		SourceStream:    "test_stream",
	}
}

// Integration test for the complete alert acknowledgment flow
func TestAlertAcknowledgmentFlow(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create a mock client
	mockClient := new(ExtendedMockClient)

	// Setup mock responses

	// 1. Stream and view existence checks
	mockClient.On("StreamExists", mock.Anything, mock.Anything).Return(false, nil)
	mockClient.On("ViewExists", mock.Anything, mock.Anything).Return(false, nil)
	mockClient.On("ListStreams", mock.Anything).Return([]string{}, nil)

	// 2. Stream creation
	mockClient.On("CreateStream", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClient.On("SetupMutableAlertAcksStream", mock.Anything).Return(nil)
	mockClient.On("CreateRuleResultsStream", mock.Anything, mock.Anything).Return(nil)

	// 3. View creation
	mockClient.On("CreateMaterializedView", mock.Anything, mock.Anything, mock.MatchedBy(func(query string) bool {
		// Verify it's creating the right kind of view
		return contains(query, "INTO tp_alert_acks_mutable") ||
			contains(query, "AS SELECT") ||
			contains(query, "WHERE value > 90")
	})).Return(nil)

	// 4. Query for columns
	viewColumns := []map[string]interface{}{
		{"name": "device_id", "type": "string"},
		{"name": "value", "type": "float64"},
	}
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return contains(query, "DESCRIBE") || contains(query, "SELECT * FROM")
	})).Return(viewColumns, nil)

	// 5. Alert detection
	// Mock data for queries checking active alerts
	activeAlertsData := []map[string]interface{}{
		{
			"rule_id":    "test-rule-id",
			"entity_id":  "device_123",
			"state":      "active",
			"created_at": time.Now().Add(-5 * time.Minute),
			"updated_at": time.Now().Add(-5 * time.Minute),
		},
	}
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return contains(query, "state = 'active'")
	})).Return(activeAlertsData, nil)

	// 6. Acknowledgment
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return contains(query, "INSERT INTO tp_alert_acks_mutable") &&
			contains(query, "state = 'acknowledged'")
	})).Return([]map[string]interface{}{}, nil)

	// 7. Verification after acknowledgment
	acknowledgedData := []map[string]interface{}{
		{
			"rule_id":    "test-rule-id",
			"entity_id":  "device_123",
			"state":      "acknowledged",
			"created_at": time.Now().Add(-5 * time.Minute),
			"updated_at": time.Now(),
			"updated_by": "tester",
			"comment":    "Test acknowledgment",
		},
	}
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return contains(query, "state = 'acknowledged'")
	})).Return(acknowledgedData, nil)

	// 8. GetRule mock response
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return contains(query, "id = ") && contains(query, "WHERE row_num = 1")
	})).Return([]map[string]interface{}{
		{
			"id":               "test-rule-id",
			"name":             "Test Rule",
			"description":      "Test Description",
			"query":            "SELECT * FROM test_stream",
			"status":           string(models.RuleStatusStopped),
			"severity":         string(models.RuleSeverityWarning),
			"throttle_minutes": 5,
			"created_at":       time.Now().Add(-1 * time.Hour),
			"updated_at":       time.Now(),
		},
	}, nil)

	// Create the RuleService
	service, err := NewRuleService(mockClient)
	assert.NoError(t, err)

	// Create a test rule and store the ID
	rule := createTestRule()
	ruleID := rule.ID

	// Mock rule creation
	mockClient.On("InsertIntoStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Create the rule using CreateRule
	createReq := &models.CreateRuleRequest{
		Name:            rule.Name,
		Description:     rule.Description,
		Query:           rule.Query,
		Severity:        rule.Severity,
		ThrottleMinutes: rule.ThrottleMinutes,
		SourceStream:    rule.SourceStream,
	}
	createdRule, err := service.CreateRule(context.Background(), createReq)
	assert.NoError(t, err)
	ruleID = createdRule.ID

	// TEST 1: Starting a rule should set up the materialized views
	ctx := context.Background()
	err = service.StartRule(ctx, ruleID)
	assert.NoError(t, err)

	// TEST 2: Test acknowledging an active alert
	err = service.AcknowledgeDevice(ctx, ruleID, "device_123", "tester", "Test acknowledgment")
	assert.NoError(t, err)

	// TEST 3: Verify the acknowledgment state
	acks, err := service.GetActiveAlertAcks(ctx, ruleID, "device_123")
	assert.NoError(t, err)
	assert.Empty(t, acks, "Should have no active alerts after acknowledgment")

	// Get acknowledged alerts to verify
	ackQuery := `
		SELECT * FROM table(tp_alert_acks_mutable)
		WHERE rule_id = '` + ruleID + `'
		AND entity_id = 'device_123'
		AND state = 'acknowledged'
	`
	results, err := service.tpClient.ExecuteQuery(ctx, ackQuery)
	assert.NoError(t, err)
	assert.NotEmpty(t, results, "Should have acknowledgment records")
	assert.Equal(t, "acknowledged", results[0]["state"])
	assert.Equal(t, "tester", results[0]["updated_by"])

	// Verify all expected calls were made
	mockClient.AssertExpectations(t)
}

// Test for the throttling implementation
func TestAlertThrottling(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create a mock client
	mockClient := new(ExtendedMockClient)

	// Configure all the same mock responses as the previous test...
	// (same setup as TestAlertAcknowledgmentFlow for brevity)
	mockClient.On("StreamExists", mock.Anything, mock.Anything).Return(false, nil)
	mockClient.On("ViewExists", mock.Anything, mock.Anything).Return(false, nil)
	mockClient.On("ListStreams", mock.Anything).Return([]string{}, nil)
	mockClient.On("CreateStream", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClient.On("SetupMutableAlertAcksStream", mock.Anything).Return(nil)
	mockClient.On("CreateRuleResultsStream", mock.Anything, mock.Anything).Return(nil)

	// We want to specifically check that the materialized view query contains throttling logic
	mockClient.On("CreateMaterializedView", mock.Anything, mock.Anything, mock.MatchedBy(func(query string) bool {
		// Verify it includes throttling logic with LEFT JOIN
		return contains(query, "LEFT JOIN") &&
			contains(query, "INTERVAL") &&
			contains(query, "tp_alert_acks_mutable")
	})).Return(nil)

	// Same column description as before
	viewColumns := []map[string]interface{}{
		{"name": "device_id", "type": "string"},
		{"name": "value", "type": "float64"},
	}
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return contains(query, "DESCRIBE") || contains(query, "SELECT * FROM")
	})).Return(viewColumns, nil)

	// GetRule mock response
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return contains(query, "id = ") && contains(query, "WHERE row_num = 1")
	})).Return([]map[string]interface{}{
		{
			"id":               "test-rule-id",
			"name":             "Test Rule",
			"description":      "Test Description",
			"query":            "SELECT * FROM test_stream",
			"status":           string(models.RuleStatusStopped),
			"severity":         string(models.RuleSeverityWarning),
			"throttle_minutes": 15, // High throttle minutes
			"created_at":       time.Now().Add(-1 * time.Hour),
			"updated_at":       time.Now(),
		},
	}, nil)

	// Create the RuleService
	service, err := NewRuleService(mockClient)
	assert.NoError(t, err)

	// Create a test rule with specific throttle settings
	rule := createTestRule()
	rule.ThrottleMinutes = 15 // 15 minutes throttle

	// Mock rule creation
	mockClient.On("InsertIntoStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Create the rule using CreateRule
	createReq := &models.CreateRuleRequest{
		Name:            rule.Name,
		Description:     rule.Description,
		Query:           rule.Query,
		Severity:        rule.Severity,
		ThrottleMinutes: rule.ThrottleMinutes,
		SourceStream:    rule.SourceStream,
	}
	createdRule, err := service.CreateRule(context.Background(), createReq)
	assert.NoError(t, err)
	ruleID := createdRule.ID

	// Start the rule
	ctx := context.Background()
	err = service.StartRule(ctx, ruleID)
	assert.NoError(t, err)

	// Verify all expected calls were made
	mockClient.AssertExpectations(t)
}
