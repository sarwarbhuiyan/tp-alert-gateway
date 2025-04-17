package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

// WaitForRule waits for a rule to be available
func WaitForRule(ctx context.Context, client timeplus.TimeplusClient, ruleID string) error {
	// Wait for materialized view to be available
	viewName := fmt.Sprintf("rule_%s_view", ruleID)
	for i := 0; i < 10; i++ {
		exists, err := client.ViewExists(ctx, viewName)
		if err == nil && exists {
			logrus.Infof("Rule view %s is running", viewName)
			return nil
		}
		logrus.Infof("Rule view %s not ready yet, retrying in 1s", viewName)
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timed out waiting for rule view %s to be ready", viewName)
}

// DropExistingViews drops any existing views for the given rule ID
func DropExistingViews(ctx context.Context, client timeplus.TimeplusClient, formattedRuleID string) error {
	viewNames := []string{
		fmt.Sprintf("rule_%s_view", formattedRuleID),
		fmt.Sprintf("rule_%s_acks_view", formattedRuleID),
	}

	for _, viewName := range viewNames {
		logrus.Infof("Dropping view %s if exists", viewName)
		if _, err := client.ExecuteQuery(ctx, fmt.Sprintf("DROP VIEW IF EXISTS `%s`", viewName)); err != nil {
			return fmt.Errorf("failed to drop view %s: %w", viewName, err)
		}
	}
	return nil
}

// SetupTestStream creates a test stream with the specified columns
func SetupTestStream(ctx context.Context, client timeplus.TimeplusClient, streamName string) error {
	// Drop stream if it exists
	_, err := client.ExecuteQuery(ctx, fmt.Sprintf("DROP STREAM IF EXISTS `%s`", streamName))
	if err != nil {
		return fmt.Errorf("failed to drop stream: %w", err)
	}

	// Create stream
	columns := []timeplus.Column{
		{Name: "device_id", Type: "string"},
		{Name: "value", Type: "float"},
		{Name: "_tp_time", Type: "datetime64(3)"},
	}
	err = client.CreateStream(ctx, streamName, columns)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	return nil
}

// GenerateTestData inserts test data into the specified stream
func GenerateTestData(ctx context.Context, client timeplus.TimeplusClient, streamName string, deviceID string, value float64) error {
	columns := []string{"device_id", "value", "_tp_time"}
	values := []interface{}{deviceID, value, time.Now()}

	err := client.InsertIntoStream(ctx, streamName, columns, values)
	if err != nil {
		return fmt.Errorf("failed to insert test data: %w", err)
	}
	return nil
}

// TestTimeplusConnection tests the connection to Timeplus and reports diagnostic information
func TestTimeplusConnection(ctx context.Context, client timeplus.TimeplusClient) error {
	logrus.Info("Testing Timeplus connection...")

	// Test 1: Check if we can execute a simple query
	logrus.Info("Test 1: Simple query execution")
	result, err := client.ExecuteQuery(ctx, "SELECT 1")
	if err != nil {
		return fmt.Errorf("failed to execute simple query: %w", err)
	}
	logrus.Infof("Simple query result: %v", result)

	// Test 2: List streams to test metadata operations
	logrus.Info("Test 2: Listing streams")
	streams, err := client.ListStreams(ctx)
	if err != nil {
		return fmt.Errorf("failed to list streams: %w", err)
	}
	logrus.Infof("Found %d streams", len(streams))

	// Test 3: Check if alert acks mutable stream exists
	logrus.Info("Test 3: Checking alert acks stream")
	exists, err := client.StreamExists(ctx, "tp_alert_acks_mutable")
	if err != nil {
		return fmt.Errorf("failed to check if alert acks stream exists: %w", err)
	}
	logrus.Infof("Alert acks stream exists: %v", exists)

	// Test 4: List materialized views
	logrus.Info("Test 4: Listing materialized views")
	views, err := client.ListMaterializedViews(ctx)
	if err != nil {
		return fmt.Errorf("failed to list materialized views: %w", err)
	}
	logrus.Infof("Found %d materialized views", len(views))

	logrus.Info("Timeplus connection tests completed successfully")
	return nil
}
