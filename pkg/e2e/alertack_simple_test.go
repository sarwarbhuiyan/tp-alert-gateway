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

func TestAlertAcknowledgmentSimple(t *testing.T) {
	// Skip the test for now until we fix the connection issues
	t.Skip("Skipping test until connection stability issues are resolved")

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

	// Create a context with a longer timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Setup test resources
	testStreamName := fmt.Sprintf("test_stream_%s", uuid.New().String())
	testDeviceID := "device_test_simple"
	testRuleID := uuid.New().String()
	formattedRuleID := testRuleID

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanupCancel()
		cleanupTestResources(cleanupCtx, client, testStreamName, formattedRuleID)
	})

	// Add a small delay before starting to ensure Timeplus connection is stable
	logrus.Info("Waiting for Timeplus connection to stabilize...")
	time.Sleep(5 * time.Second)

	// Drop any existing views for the rule to ensure clean state
	logrus.Info("Dropping any existing views...")
	err = DropExistingViews(ctx, client, formattedRuleID)
	require.NoError(t, err, "Failed to drop existing views")

	// Add a small delay between operations
	time.Sleep(2 * time.Second)

	// Setup test stream
	logrus.Info("Setting up test stream...")
	err = SetupTestStream(ctx, client, testStreamName)
	require.NoError(t, err, "Failed to setup test stream")

	// Add a small delay between operations
	time.Sleep(2 * time.Second)

	// Setup rule service
	ruleService, err := services.NewRuleService(client)
	require.NoError(t, err, "Failed to create rule service")

	// Setup alert acks stream with retry
	logrus.Info("Setting up alert acks stream...")
	err = retryWithBackoff(ctx, 5, func() error {
		return client.SetupMutableAlertAcksStream(ctx)
	})
	require.NoError(t, err, "Failed to setup alert acks stream after retries")

	// Add a small delay between operations
	time.Sleep(2 * time.Second)

	// Create simple test rule with retry
	logrus.Info("Creating test rule...")
	var rule *models.Rule
	err = retryWithBackoff(ctx, 5, func() error {
		var createErr error
		rule, createErr = createSimpleTestRule(ctx, ruleService, testStreamName, testRuleID)
		return createErr
	})
	require.NoError(t, err, "Failed to create test rule after retries")
	require.NotNil(t, rule, "Rule should not be nil")

	// Wait for the rule to be available with improved waiting mechanism
	logrus.Info("Waiting for rule to be available...")
	err = WaitForRule(ctx, client, formattedRuleID)
	require.NoError(t, err, "Failed to wait for rule")

	// Wait a bit to ensure the views are fully operational
	logrus.Info("Waiting for views to become fully operational...")
	time.Sleep(10 * time.Second)

	// Generate test data that will trigger an alert
	logrus.Info("Generating test data...")
	err = retryWithBackoff(ctx, 5, func() error {
		return GenerateTestData(ctx, client, testStreamName, testDeviceID, 100.0)
	})
	require.NoError(t, err, "Failed to generate test data")

	// Wait for alert to be generated with improved retry mechanism
	logrus.Info("Waiting for alert to be generated...")
	time.Sleep(15 * time.Second)

	// Acknowledge the alert
	logrus.Info("Acknowledging alert...")
	err = retryWithBackoff(ctx, 5, func() error {
		return ruleService.AcknowledgeDevice(ctx, formattedRuleID, testDeviceID, "test-user", "test comment")
	})
	require.NoError(t, err, "Failed to acknowledge alert")

	// Verify alert has been acknowledged by checking the acks stream
	logrus.Info("Verifying alert acknowledgment...")
	time.Sleep(10 * time.Second)

	query := fmt.Sprintf("SELECT state FROM table(`tp_alert_acks_mutable`) WHERE rule_id = '%s' AND device_id = '%s' LIMIT 1", formattedRuleID, testDeviceID)

	var state string
	var found bool

	// Retry query a few times with backoff
	err = retryWithBackoff(ctx, 10, func() error {
		result, err := client.ExecuteQuery(ctx, query)
		if err != nil {
			return err
		}

		if len(result) > 0 {
			if stateVal, ok := result[0]["state"]; ok {
				if strVal, ok := stateVal.(string); ok {
					state = strVal
					found = true
					return nil
				}
			}
		}

		return fmt.Errorf("alert ack not found yet")
	})

	require.True(t, found, "Alert acknowledgment should exist in the stream")
	require.Equal(t, "acknowledged", state, "Alert should be in acknowledged state")

	logrus.Info("Test completed successfully")
}

// retryWithBackoff retries a function with exponential backoff
func retryWithBackoff(ctx context.Context, maxRetries int, fn func() error) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = fn()
		if err == nil {
			return nil
		}

		waitTime := time.Duration(1<<uint(i)) * time.Second
		logrus.Warnf("Operation failed, retrying in %v (%d/%d): %v", waitTime, i+1, maxRetries, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
			// Continue with retry
		}
	}
	return fmt.Errorf("operation failed after %d retries: %w", maxRetries, err)
}

// createSimpleTestRule creates a simple test rule
func createSimpleTestRule(ctx context.Context, ruleService *services.RuleService, streamName, ruleID string) (*models.Rule, error) {
	req := &models.CreateRuleRequest{
		Name:            "Simple Test Rule",
		Description:     "A simple test rule for acknowledgment tests",
		Severity:        models.RuleSeverityCritical,
		Query:           fmt.Sprintf("SELECT device_id, value FROM `%s` WHERE value > 50", streamName),
		ThrottleMinutes: 0,
		EntityIDColumns: "device_id",
	}

	return ruleService.CreateRule(ctx, req)
}

// cleanupTestResources cleans up test resources
func cleanupTestResources(ctx context.Context, client timeplus.TimeplusClient, streamName, ruleID string) {
	// Clean up rule views
	if err := DropExistingViews(ctx, client, ruleID); err != nil {
		logrus.Warnf("Failed to drop views: %v", err)
	}

	// Clean up test stream
	_, err := client.ExecuteQuery(ctx, fmt.Sprintf("DROP STREAM IF EXISTS `%s`", streamName))
	if err != nil {
		logrus.Warnf("Failed to drop stream: %v", err)
	}

	logrus.Info("Cleaned up test resources")
}
