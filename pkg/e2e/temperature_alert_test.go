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
	"github.com/timeplus-io/tp-alert-gateway/pkg/models"
	"github.com/timeplus-io/tp-alert-gateway/pkg/services"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

// TestTemperatureAlertsE2E performs an end-to-end test of the temperature monitoring system
// with alerts, throttling and acknowledgment
func TestTemperatureAlertsE2E(t *testing.T) {
	// Skip by default for CI/CD pipelines
	// Temporarily commenting out to run the test
	// t.Skip("Skipping E2E test - run with -skip=false when needed")

	// Configure logging
	logrus.SetLevel(logrus.InfoLevel)
	logrus.Info("Starting temperature alerts E2E test")

	// Setup test configuration with longer timeouts
	tpConfig := &config.TimeplusConfig{
		Address:   "localhost:8464",
		Username:  "test",
		Password:  "test123",
		Workspace: "default",
	}

	// Create a client with improved retry mechanisms
	client, err := timeplus.NewClient(tpConfig)
	require.NoError(t, err, "Failed to create Timeplus client")

	// Create a context with a longer timeout for the entire test
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Setup test resources with unique IDs
	streamName := fmt.Sprintf("temperature_stream_%s", uuid.New().String()[:8])
	deviceIDs := []string{
		"thermostat_living_room",
		"thermostat_kitchen",
		"thermostat_bedroom",
	}
	ruleID := uuid.New().String()

	// Test cleanup handler
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanupTemperatureTest(cleanupCtx, client, streamName, ruleID)
	})

	// ---- Phase 1: Setup Test Environment ----

	// Setup temperature stream
	err = setupTemperatureStream(ctx, client, streamName)
	require.NoError(t, err, "Failed to setup temperature stream")
	logrus.Infof("Created temperature stream: %s", streamName)

	// Initialize rule service
	ruleService, err := services.NewRuleService(client)
	require.NoError(t, err, "Failed to create rule service")

	// Setup alert acks stream
	err = client.SetupMutableAlertAcksStream(ctx)
	require.NoError(t, err, "Failed to setup alert acks stream")

	// Create a rule to alert when temperature exceeds 30°C with 1 minute throttling
	rule, err := createTemperatureRule(ctx, ruleService, streamName, ruleID, 30.0, 1)
	require.NoError(t, err, "Failed to create temperature rule")
	logrus.Infof("Created temperature rule: %s (ID: %s)", rule.Name, rule.ID)

	// Wait for the rule to be available
	err = WaitForRule(ctx, client, ruleID)
	require.NoError(t, err, "Rule was not created in time")

	// ---- Phase 2: Generate Normal Data (No Alerts) ----

	logrus.Info("Generating normal temperature data (below threshold)")
	// Insert normal temperatures (below threshold) for all devices
	for _, deviceID := range deviceIDs {
		// Generate temperatures between 20-25°C (normal range)
		for i := 0; i < 3; i++ {
			temperature := 20.0 + float64(i)
			err = insertTemperature(ctx, client, streamName, deviceID, temperature)
			require.NoError(t, err, "Failed to insert normal temperature data")
			time.Sleep(500 * time.Millisecond) // Space out the inserts
		}
	}

	// Check that no alerts were generated
	time.Sleep(5 * time.Second) // Wait for processing
	alerts, err := getAlertsForRule(ctx, client, ruleID)
	require.NoError(t, err, "Failed to query alerts")
	require.Empty(t, alerts, "No alerts should be generated for normal temperatures")
	logrus.Info("Verified no alerts for normal temperatures")

	// ---- Phase 3: Generate Alert Condition ----

	logrus.Info("Generating high temperature data (above threshold)")
	// Insert high temperature for one device to trigger alert
	highTempDeviceID := deviceIDs[0]
	err = insertTemperature(ctx, client, streamName, highTempDeviceID, 35.0)
	require.NoError(t, err, "Failed to insert high temperature data")

	// Wait for the alert to be generated
	time.Sleep(5 * time.Second)

	// Verify alert was created
	alerts, err = getAlertsForRule(ctx, client, ruleID)
	require.NoError(t, err, "Failed to query alerts")
	require.NotEmpty(t, alerts, "Alert should be generated for high temperature")
	logrus.Infof("Alert generated for device %s with temperature 35.0°C", highTempDeviceID)

	// ---- Phase 4: Test Throttling ----

	logrus.Info("Testing alert throttling")
	// Insert another high temperature for the same device - should be throttled
	err = insertTemperature(ctx, client, streamName, highTempDeviceID, 38.0)
	require.NoError(t, err, "Failed to insert second high temperature data")

	// Wait for potential processing
	time.Sleep(2 * time.Second)

	// Count alerts for this device - should still be just one due to throttling
	initialAlertCount := len(alerts)
	alerts, err = getAlertsForRule(ctx, client, ruleID)
	require.NoError(t, err, "Failed to query alerts after throttling test")
	require.Equal(t, initialAlertCount, len(alerts), "Alerts should be throttled")
	logrus.Info("Verified alert throttling is working")

	// ---- Phase 5: Test Multiple Device Alerts ----

	logrus.Info("Testing alerts for multiple devices")
	// Generate high temperature for a different device
	secondDeviceID := deviceIDs[1]
	err = insertTemperature(ctx, client, streamName, secondDeviceID, 36.0)
	require.NoError(t, err, "Failed to insert high temperature for second device")

	// Wait for alert processing
	time.Sleep(5 * time.Second)

	// Verify we have alerts for both devices
	alerts, err = getAlertsForRule(ctx, client, ruleID)
	require.NoError(t, err, "Failed to query alerts for multiple devices")
	require.Equal(t, initialAlertCount+1, len(alerts), "Should have one more alert for the second device")
	logrus.Infof("Alert generated for second device %s with temperature 36.0°C", secondDeviceID)

	// ---- Phase 6: Test Alert Acknowledgment ----

	logrus.Info("Testing alert acknowledgment")
	// Acknowledge alert for the first device
	err = ruleService.AcknowledgeDevice(ctx, ruleID, highTempDeviceID, "test-user", "Temperature issue acknowledged")
	require.NoError(t, err, "Failed to acknowledge temperature alert")

	// Wait for acknowledgment processing
	time.Sleep(5 * time.Second)

	// Verify the acknowledgment was recorded in the acks stream
	query := fmt.Sprintf("SELECT state FROM table(`tp_alert_acks_mutable`) WHERE rule_id = '%s' AND device_id = '%s' LIMIT 1", ruleID, highTempDeviceID)
	result, err := client.ExecuteQuery(ctx, query)
	require.NoError(t, err, "Failed to query alert acks")
	require.NotEmpty(t, result, "Alert acknowledgment should exist in the stream")

	if len(result) > 0 && result[0]["state"] != nil {
		state := result[0]["state"].(string)
		require.Equal(t, "acknowledged", state, "Alert should be in acknowledged state")
		logrus.Infof("Successfully acknowledged alert for device %s", highTempDeviceID)
	}

	// ---- Phase 7: Test Continued Throttling After Acknowledgment ----

	logrus.Info("Testing continued throttling after acknowledgment")
	// Insert another high temperature after acknowledgment - should still throttle
	err = insertTemperature(ctx, client, streamName, highTempDeviceID, 40.0)
	require.NoError(t, err, "Failed to insert high temperature after acknowledgment")

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify alert count hasn't changed (still throttled)
	alerts, err = getAlertsForRule(ctx, client, ruleID)
	require.NoError(t, err, "Failed to query alerts after acknowledgment")
	require.Equal(t, initialAlertCount+1, len(alerts), "Alerts should still be throttled after acknowledgment")
	logrus.Info("Verified continued throttling after acknowledgment")

	// Test completed successfully
	logrus.Info("Temperature alerts E2E test completed successfully")
}

// setupTemperatureStream creates a stream for temperature data
func setupTemperatureStream(ctx context.Context, client timeplus.TimeplusClient, streamName string) error {
	// Drop stream if it exists
	_, err := client.ExecuteQuery(ctx, fmt.Sprintf("DROP STREAM IF EXISTS `%s`", streamName))
	if err != nil {
		return fmt.Errorf("failed to drop existing stream: %w", err)
	}

	// Create stream with temperature schema
	columns := []timeplus.Column{
		{Name: "device_id", Type: "string"},
		{Name: "temperature", Type: "float"},
		{Name: "humidity", Type: "float", Nullable: true},
		{Name: "battery", Type: "float", Nullable: true},
		{Name: "timestamp", Type: "datetime64(3)"},
	}

	return client.CreateStream(ctx, streamName, columns)
}

// createTemperatureRule creates a rule for monitoring high temperatures
func createTemperatureRule(
	ctx context.Context,
	ruleService *services.RuleService,
	streamName string,
	ruleID string,
	threshold float64,
	throttleMinutes int,
) (*models.Rule, error) {
	req := &models.CreateRuleRequest{
		Name:            "Temperature Alert",
		Description:     fmt.Sprintf("Alert when temperature exceeds %.1f°C", threshold),
		Severity:        models.RuleSeverityWarning,
		Query:           fmt.Sprintf("SELECT device_id, temperature, timestamp FROM `%s` WHERE temperature > %.1f", streamName, threshold),
		ThrottleMinutes: throttleMinutes,
		EntityIDColumns: "device_id",
		SourceStream:    streamName,
	}

	return ruleService.CreateRule(ctx, req)
}

// insertTemperature inserts a temperature reading into the stream
func insertTemperature(ctx context.Context, client timeplus.TimeplusClient, streamName, deviceID string, temperature float64) error {
	columns := []string{"device_id", "temperature", "humidity", "battery", "timestamp"}
	values := []interface{}{
		deviceID,
		temperature,
		float64(30 + (temperature - 20)), // Humidity correlates with temperature
		float64(95 - (temperature / 2)),  // Battery decreases a bit with higher temperature
		time.Now(),
	}

	return client.InsertIntoStream(ctx, streamName, columns, values)
}

// getAlertsForRule retrieves alerts for a specific rule
func getAlertsForRule(ctx context.Context, client timeplus.TimeplusClient, ruleID string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM table(tp_alerts) WHERE rule_id = '%s' ORDER BY triggered_at DESC", ruleID)
	return client.ExecuteQuery(ctx, query)
}

// cleanupTemperatureTest cleans up all resources created during the test
func cleanupTemperatureTest(ctx context.Context, client timeplus.TimeplusClient, streamName, ruleID string) {
	// Clean up rule views
	viewNames := []string{
		fmt.Sprintf("rule_%s_view", ruleID),
		fmt.Sprintf("rule_%s_acks_view", ruleID),
	}

	for _, viewName := range viewNames {
		_, err := client.ExecuteQuery(ctx, fmt.Sprintf("DROP VIEW IF EXISTS `%s`", viewName))
		if err != nil {
			logrus.Warnf("Failed to drop view %s: %v", viewName, err)
		}
	}

	// Clean up result stream
	resultStream := fmt.Sprintf("rule_%s_results", ruleID)
	_, err := client.ExecuteQuery(ctx, fmt.Sprintf("DROP STREAM IF EXISTS `%s`", resultStream))
	if err != nil {
		logrus.Warnf("Failed to drop result stream: %v", err)
	}

	// Clean up temperature stream
	_, err = client.ExecuteQuery(ctx, fmt.Sprintf("DROP STREAM IF EXISTS `%s`", streamName))
	if err != nil {
		logrus.Warnf("Failed to drop temperature stream: %v", err)
	}

	logrus.Info("Cleaned up all test resources")
}
