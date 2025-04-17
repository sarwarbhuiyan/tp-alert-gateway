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

// MockClient is a mock implementation of the TimeplusClient interface
type MockClient struct {
	mock.Mock
}

// Ensure MockClient implements TimeplusClient
var _ timeplus.TimeplusClient = (*MockClient)(nil)

func (m *MockClient) StreamExists(ctx context.Context, name string) (bool, error) {
	args := m.Called(ctx, name)
	return args.Bool(0), args.Error(1)
}

func (m *MockClient) CreateStream(ctx context.Context, name string, schema []timeplus.Column) error {
	args := m.Called(ctx, name, schema)
	return args.Error(0)
}

func (m *MockClient) CreateMaterializedView(ctx context.Context, name string, query string) error {
	args := m.Called(ctx, name, query)
	return args.Error(0)
}

func (m *MockClient) DeleteMaterializedView(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *MockClient) ViewExists(ctx context.Context, name string) (bool, error) {
	args := m.Called(ctx, name)
	return args.Bool(0), args.Error(1)
}

func (m *MockClient) DeleteStream(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

func (m *MockClient) ExecuteQuery(ctx context.Context, query string) ([]map[string]interface{}, error) {
	args := m.Called(ctx, query)
	return args.Get(0).([]map[string]interface{}), args.Error(1)
}

func (m *MockClient) StreamQuery(ctx context.Context, query string, callback func(row interface{})) error {
	args := m.Called(ctx, query, callback)
	return args.Error(0)
}

func (m *MockClient) InsertIntoStream(ctx context.Context, streamName string, columns []string, values []interface{}) error {
	args := m.Called(ctx, streamName, columns, values)
	return args.Error(0)
}

// Additional methods to implement TimeplusClient interface
func (m *MockClient) ListStreams(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockClient) ListViews(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockClient) ListMaterializedViews(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockClient) ExecuteStreamingQuery(ctx context.Context, query string, callback func(result map[string]interface{}) error) error {
	args := m.Called(ctx, query, callback)
	return args.Error(0)
}

func (m *MockClient) SetupAlertAcksStream(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockClient) SetupMutableAlertAcksStream(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockClient) CreateAlertAck(ctx context.Context, alertAck timeplus.AlertAck) error {
	args := m.Called(ctx, alertAck)
	return args.Error(0)
}

func (m *MockClient) GetAlertAck(ctx context.Context, alertID string) (*timeplus.AlertAck, error) {
	args := m.Called(ctx, alertID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*timeplus.AlertAck), args.Error(1)
}

func (m *MockClient) IsAlertAcknowledged(ctx context.Context, alertID string) (bool, error) {
	args := m.Called(ctx, alertID)
	return args.Bool(0), args.Error(1)
}

func (m *MockClient) CreateRuleResultsStream(ctx context.Context, ruleID string) error {
	args := m.Called(ctx, ruleID)
	return args.Error(0)
}

func TestGetRulesWithMock(t *testing.T) {
	// Skip the test if testing.Short() is true - useful for CI/CD
	if testing.Short() {
		t.Skip("Skipping mock test in short mode")
	}

	// Create a mock client
	mockClient := new(MockClient)

	// Setup mock response for the ExecuteQuery call
	mockRuleData := []map[string]interface{}{
		{
			"id":               "rule1",
			"name":             "Test Rule",
			"description":      "Test Description",
			"query":            "SELECT * FROM test_stream",
			"status":           "running",
			"severity":         "warning",
			"throttle_minutes": int32(5),
			"created_at":       time.Now().Add(-1 * time.Hour),
			"updated_at":       time.Now(),
			"source_stream":    "test_stream",
			"result_stream":    "rule_rule1_results",
			"view_name":        "rule_rule1_view",
		},
	}

	// Verify that the query includes table() for one-off queries
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return contains(query, "FROM table(tp_rules)")
	})).Return(mockRuleData, nil)

	// Create a rule service with the mock client
	service := &RuleService{
		tpClient:    mockClient,
		ruleStream:  "tp_rules",
		alertStream: "tp_alerts",
	}

	// Call GetRules
	rules, err := service.GetRules()

	// Assert expectations
	assert.NoError(t, err)
	assert.Len(t, rules, 1)
	assert.Equal(t, "rule1", rules[0].ID)
	assert.Equal(t, "Test Rule", rules[0].Name)
	assert.Equal(t, models.RuleStatus("running"), rules[0].Status)

	// Verify that all expected mock calls were made
	mockClient.AssertExpectations(t)
}

func TestGetAlertWithMock(t *testing.T) {
	// Skip the test if testing.Short() is true
	if testing.Short() {
		t.Skip("Skipping mock test in short mode")
	}

	// Create a mock client
	mockClient := new(MockClient)

	// Setup mock response for the ExecuteQuery call
	mockAlertData := []map[string]interface{}{
		{
			"id":              uuid.New().String(),
			"rule_id":         "rule1",
			"rule_name":       "Test Rule",
			"severity":        "warning",
			"triggered_at":    time.Now().Add(-30 * time.Minute),
			"data":            "{\"value\": 100}",
			"acknowledged":    false,
			"acknowledged_at": nil,
			"acknowledged_by": "",
		},
	}

	// Verify that the query includes table() for one-off queries
	mockClient.On("ExecuteQuery", mock.Anything, mock.MatchedBy(func(query string) bool {
		return contains(query, "FROM table(tp_alerts)")
	})).Return(mockAlertData, nil)

	// Create a rule service with the mock client
	service := &RuleService{
		tpClient:    mockClient,
		ruleStream:  "tp_rules",
		alertStream: "tp_alerts",
	}

	// Call GetAlert
	alert, err := service.GetAlert("any-id")

	// Assert expectations
	assert.NoError(t, err)
	assert.Equal(t, "rule1", alert.RuleID)
	assert.Equal(t, "Test Rule", alert.RuleName)
	assert.Equal(t, models.RuleSeverity("warning"), alert.Severity)
	assert.False(t, alert.Acknowledged)

	// Verify that all expected mock calls were made
	mockClient.AssertExpectations(t)
}
