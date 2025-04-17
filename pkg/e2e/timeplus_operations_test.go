package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

// TestTimeplusOperations tests all basic Timeplus operations
func TestTimeplusOperations(t *testing.T) {
	// Configure logging
	logrus.SetLevel(logrus.InfoLevel)
	logrus.Info("Starting Timeplus operations test")

	// Setup test configuration
	tpConfig := &config.TimeplusConfig{
		Address:   "localhost:8464",
		Username:  "test",
		Password:  "test123",
		Workspace: "default",
	}

	// Create a client
	client, err := timeplus.NewClient(tpConfig)
	require.NoError(t, err, "Failed to create Timeplus client")

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Generate unique names
	uniqueID := "t" + uuid.New().String()[:8]
	testStreamName := fmt.Sprintf("test_stream_%s", uniqueID)
	testViewName := fmt.Sprintf("test_view_%s", uniqueID)
	testMatViewName := fmt.Sprintf("test_matview_%s", uniqueID)

	// Cleanup function to remove all resources
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()

		logrus.Info("Cleanup: Removing test resources")

		// Drop materialized view
		_, err := client.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP VIEW IF EXISTS `%s`", testMatViewName))
		if err != nil {
			logrus.Warnf("Failed to drop materialized view: %v", err)
		}

		// Drop view
		_, err = client.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP VIEW IF EXISTS `%s`", testViewName))
		if err != nil {
			logrus.Warnf("Failed to drop view: %v", err)
		}

		// Drop stream
		_, err = client.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP STREAM IF EXISTS `%s`", testStreamName))
		if err != nil {
			logrus.Warnf("Failed to drop stream: %v", err)
		}
	}()

	// Test 1: List Streams
	logrus.Info("Test 1: Listing streams")
	streams, err := client.ListStreams(ctx)
	require.NoError(t, err, "Failed to list streams")
	logrus.Infof("Found %d existing streams", len(streams))

	// Test 2: Create Stream
	logrus.Info("Test 2: Creating a test stream")
	createStreamQuery := fmt.Sprintf(
		"CREATE STREAM IF NOT EXISTS `%s` (id string, value float64, event_time datetime64(3))",
		testStreamName,
	)
	logrus.Infof("Execute: %s", createStreamQuery)
	_, err = client.ExecuteQuery(ctx, createStreamQuery)
	require.NoError(t, err, "Failed to create stream")

	// Test 3: Verify Stream Exists
	logrus.Info("Test 3: Verifying stream exists")
	exists, err := client.StreamExists(ctx, testStreamName)
	require.NoError(t, err, "Failed to check if stream exists")
	require.True(t, exists, "Created stream should exist")

	// Test 4: Insert Data
	logrus.Info("Test 4: Inserting data into stream")
	insertSuccessful := false
	for i := 0; i < 3; i++ {
		timestamp := time.Now().Format("2006-01-02 15:04:05.000")
		insertQuery := fmt.Sprintf(
			"INSERT INTO `%s` (id, value, event_time) VALUES ('test_%d', %f, '%s')",
			testStreamName, i, float64(i*10), timestamp,
		)
		logrus.Infof("Execute: %s", insertQuery)
		_, err = client.ExecuteQuery(ctx, insertQuery)
		if err != nil {
			logrus.Warnf("Insert attempt %d failed: %v", i+1, err)
			time.Sleep(1 * time.Second)
		} else {
			logrus.Infof("Successfully inserted data (row %d)", i)
			insertSuccessful = true
		}
	}

	// Verify at least one insert succeeded
	require.True(t, insertSuccessful, "At least one insert should succeed")

	// Test 5: Create Plain View
	logrus.Info("Test 5: Creating a plain view")
	createViewQuery := fmt.Sprintf(
		"CREATE VIEW IF NOT EXISTS `%s` AS SELECT id, value FROM `%s` WHERE value > 5",
		testViewName, testStreamName,
	)
	logrus.Infof("Execute: %s", createViewQuery)
	_, err = client.ExecuteQuery(ctx, createViewQuery)
	require.NoError(t, err, "Failed to create plain view")

	// Test 6: Verify View Exists
	logrus.Info("Test 6: Verifying view exists")
	viewsQuery := "SELECT name FROM system.tables WHERE engine = 'View'"
	logrus.Infof("Execute: %s", viewsQuery)
	viewsResult, err := client.ExecuteQuery(ctx, viewsQuery)
	require.NoError(t, err, "Failed to list views")

	viewFound := false
	for _, row := range viewsResult {
		if name, ok := row["name"].(string); ok && name == testViewName {
			viewFound = true
			break
		}
	}
	require.True(t, viewFound, "Plain view should exist")

	// Test 7: Create Materialized View
	logrus.Info("Test 7: Creating a materialized view")
	createMatViewQuery := fmt.Sprintf(
		"CREATE MATERIALIZED VIEW IF NOT EXISTS `%s` AS SELECT id, value, event_time FROM `%s` WHERE value > 0",
		testMatViewName, testStreamName,
	)
	logrus.Infof("Execute: %s", createMatViewQuery)
	_, err = client.ExecuteQuery(ctx, createMatViewQuery)
	require.NoError(t, err, "Failed to create materialized view")

	// Test 8: Verify Materialized View Exists
	logrus.Info("Test 8: Verifying materialized view exists")
	matViewsQuery := "SELECT name FROM system.tables WHERE engine = 'MaterializedView'"
	logrus.Infof("Execute: %s", matViewsQuery)
	matViewsResult, err := client.ExecuteQuery(ctx, matViewsQuery)
	require.NoError(t, err, "Failed to list materialized views")

	matViewFound := false
	logrus.Info("Materialized views found:")
	for _, row := range matViewsResult {
		if name, ok := row["name"].(string); ok {
			logrus.Infof("  - %s", name)
			if name == testMatViewName {
				matViewFound = true
			}
		}
	}
	require.True(t, matViewFound, "Materialized view should exist")

	// Test 9: Query Data from Stream
	logrus.Info("Test 9: Query data from stream")
	time.Sleep(2 * time.Second) // Wait a bit for data propagation
	streamQuery := fmt.Sprintf("SELECT * FROM table(`%s`) LIMIT 10", testStreamName)
	logrus.Infof("Execute: %s", streamQuery)
	streamResult, err := client.ExecuteQuery(ctx, streamQuery)
	require.NoError(t, err, "Failed to query stream data")
	logrus.Infof("Stream query returned %d rows", len(streamResult))

	// Test 10: Query Data from View
	logrus.Info("Test 10: Query data from view")
	viewQuery := fmt.Sprintf("SELECT * FROM table(`%s`) LIMIT 10", testViewName)
	logrus.Infof("Execute: %s", viewQuery)
	viewResult, err := client.ExecuteQuery(ctx, viewQuery)
	require.NoError(t, err, "Failed to query view data")
	logrus.Infof("View query returned %d rows", len(viewResult))

	// Test 11: List all tables
	logrus.Info("Test 11: List all tables")
	tablesQuery := "SELECT name, engine FROM system.tables WHERE database = 'default'"
	logrus.Infof("Execute: %s", tablesQuery)
	tablesResult, err := client.ExecuteQuery(ctx, tablesQuery)
	require.NoError(t, err, "Failed to query tables")
	logrus.Infof("Found %d tables", len(tablesResult))

	for i, row := range tablesResult {
		if i < 10 { // Show first 10 tables
			logrus.Infof("Table: %v, Engine: %v", row["name"], row["engine"])
		}
	}

	logrus.Info("All Timeplus operations tested successfully")
}
