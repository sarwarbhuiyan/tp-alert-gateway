package services

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

// TestSimpleConnection tests the basic connection to Timeplus
func TestSimpleConnection(t *testing.T) {
	// Skip this test unless explicitly specified with environment variable
	if os.Getenv("RUN_INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=true to run.")
	}

	// Load configuration from file
	projectRoot, err := filepath.Abs("../../")
	require.NoError(t, err)

	configPath := filepath.Join(projectRoot, "config.local.yaml")
	cfg, err := config.LoadConfig(configPath)
	require.NoError(t, err, "Failed to load config from %s", configPath)

	// Create Timeplus client
	tpClient, err := timeplus.NewClient(&cfg.Timeplus)
	require.NoError(t, err, "Failed to create Timeplus client")

	// Create a unique test stream name
	testStreamName := fmt.Sprintf("test_stream_%d", time.Now().UnixNano())
	ctx := context.Background()

	// Cleanup function to delete the test stream after test
	defer func() {
		// Try to delete the stream
		exists, err := tpClient.StreamExists(ctx, testStreamName)
		if err == nil && exists {
			tpClient.DeleteStream(ctx, testStreamName)
		}
	}()

	// Define a simple schema for the test stream
	schema := []timeplus.Column{
		{Name: "id", Type: "string"},
		{Name: "value", Type: "float"},
		{Name: "timestamp", Type: "datetime64"},
	}

	// Step 1: Create a stream
	err = tpClient.CreateStream(ctx, testStreamName, schema)
	require.NoError(t, err, "Failed to create test stream")

	// Step 2: Verify the stream exists
	exists, err := tpClient.StreamExists(ctx, testStreamName)
	require.NoError(t, err, "Failed to check if stream exists")
	assert.True(t, exists, "Stream should exist after creation")

	// Step 3: Insert data into the stream
	columns := []string{"id", "value", "timestamp"}
	values := []interface{}{"test1", 100.5, time.Now()}

	err = tpClient.InsertIntoStream(ctx, testStreamName, columns, values)
	require.NoError(t, err, "Failed to insert data into stream")

	// Step 4: Query the data (with a longer delay to allow processing)
	time.Sleep(3 * time.Second)

	query := fmt.Sprintf("SELECT count(*) as count FROM table(`%s`)", testStreamName)
	results, err := tpClient.ExecuteQuery(ctx, query)
	require.NoError(t, err, "Failed to query data from stream")

	// Check if we have any records at all
	require.NotEmpty(t, results, "Should have at least one result row")

	// Try extracting count as uint64 (based on our debugging)
	countVal, ok := results[0]["count"].(uint64)
	require.True(t, ok, "Failed to get count from result")
	assert.GreaterOrEqual(t, countVal, uint64(1), "Should have at least one record in the stream")

	// Success! We've verified basic connectivity, stream operations, and data insertion/retrieval
}

// TestSimpleAlertAcknowledgment tests a simplified alert acknowledgment flow
func TestSimpleAlertAcknowledgment(t *testing.T) {
	// Skip this test unless explicitly specified with environment variable
	if os.Getenv("RUN_INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=true to run.")
	}

	// Load configuration from file
	projectRoot, err := filepath.Abs("../../")
	require.NoError(t, err)

	configPath := filepath.Join(projectRoot, "config.local.yaml")
	cfg, err := config.LoadConfig(configPath)
	require.NoError(t, err, "Failed to load config from %s", configPath)

	// Create Timeplus client
	tpClient, err := timeplus.NewClient(&cfg.Timeplus)
	require.NoError(t, err, "Failed to create Timeplus client")

	// Generate unique test identifiers
	testID := fmt.Sprintf("%d", time.Now().UnixNano())
	testStreamName := fmt.Sprintf("tp_alert_acks_test_%s", testID)
	entityID := "test_entity_123"
	ruleID := fmt.Sprintf("rule_%s", testID)
	ctx := context.Background()

	// Cleanup function to delete the test stream after test
	defer func() {
		exists, err := tpClient.StreamExists(ctx, testStreamName)
		if err == nil && exists {
			tpClient.DeleteStream(ctx, testStreamName)
		}
	}()

	// Step 1: Create alert acknowledgment stream
	// Use the standard schema for alert acks
	acksSchema := timeplus.GetMutableAlertAcksSchema()
	err = tpClient.CreateStream(ctx, testStreamName, acksSchema)
	require.NoError(t, err, "Failed to create test alert acks stream")

	// Step 2: Insert an "active" alert record
	now := time.Now()
	columns := []string{
		"rule_id",
		"entity_id",
		"state",
		"created_at",
		"updated_at",
		"updated_by",
		"comment",
	}
	values := []interface{}{
		ruleID,
		entityID,
		"active",
		now,
		now,
		"",
		"test alert",
	}

	err = tpClient.InsertIntoStream(ctx, testStreamName, columns, values)
	require.NoError(t, err, "Failed to insert active alert record")

	// Step 3: Verify the active alert was inserted (wait for processing)
	time.Sleep(3 * time.Second)
	queryActive := fmt.Sprintf("SELECT count(*) as count FROM table(`%s`) WHERE rule_id = '%s' AND entity_id = '%s' AND state = 'active'",
		testStreamName, ruleID, entityID)
	results, err := tpClient.ExecuteQuery(ctx, queryActive)
	require.NoError(t, err, "Failed to query for active alert")
	require.NotEmpty(t, results, "Should have results for active alert query")

	countVal, ok := results[0]["count"].(uint64)
	require.True(t, ok, "Count field should be of type uint64")
	assert.GreaterOrEqual(t, countVal, uint64(1), "Should have at least one active alert")

	// Step 4: Insert an "acknowledged" alert record (simulate acknowledgment)
	ackTime := time.Now()
	ackValues := []interface{}{
		ruleID,
		entityID,
		"acknowledged",
		now, // Keep original creation time
		ackTime,
		"test_user",
		"test acknowledgment",
	}

	err = tpClient.InsertIntoStream(ctx, testStreamName, columns, ackValues)
	require.NoError(t, err, "Failed to insert acknowledgment record")

	// Step 5: Verify the acknowledgment (wait for processing)
	time.Sleep(3 * time.Second)
	queryAck := fmt.Sprintf("SELECT state, updated_by FROM table(`%s`) WHERE rule_id = '%s' AND entity_id = '%s' ORDER BY updated_at DESC LIMIT 1",
		testStreamName, ruleID, entityID)
	results, err = tpClient.ExecuteQuery(ctx, queryAck)
	require.NoError(t, err, "Failed to query for acknowledgment")
	require.NotEmpty(t, results, "Should have results for acknowledgment query")

	// Debug - print result details
	t.Logf("Acknowledgment result: %+v", results)
	for k, v := range results[0] {
		t.Logf("Field '%s' has type %T with value: %v", k, v, v)
	}

	// Verify the state and updated_by fields
	state, ok := results[0]["state"].(string)
	require.True(t, ok, "State field should be a string")
	assert.Equal(t, "acknowledged", state, "Latest state should be 'acknowledged'")

	// Handle the updated_by field which might be returned differently
	var updatedBy string
	updatedByVal := results[0]["updated_by"]

	switch v := updatedByVal.(type) {
	case string:
		updatedBy = v
	case *string:
		if v != nil {
			updatedBy = *v // Dereference the pointer
		} else {
			updatedBy = ""
		}
	case nil:
		updatedBy = "" // Default to empty string if nil
	default:
		t.Logf("updated_by field has unexpected type: %T", v)
		updatedBy = fmt.Sprintf("%v", v) // Convert to string representation
	}

	assert.Equal(t, "test_user", updatedBy, "Updated_by should match")

	// Success! We've verified the simplified alert acknowledgment flow.
}
