package timeplus

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/timeplus-io/tp-alert-gateway/pkg/models"
)

// Alert acknowledgment states
const (
	AlertStateActive       = "active"
	AlertStateAcknowledged = "acknowledged"
	AlertStateSilenced     = "silenced"
	AlertStateResolved     = "resolved"
)

// AlertAck represents an alert acknowledgment in Timeplus
type AlertAck struct {
	AlertID    string    `json:"alert_id"`
	RuleID     string    `json:"rule_id"`
	State      string    `json:"state"`
	UpdatedBy  string    `json:"updated_by"`
	UpdatedAt  time.Time `json:"updated_at"`
	Comment    string    `json:"comment,omitempty"`
	ValidUntil time.Time `json:"valid_until,omitempty"` // Used for silencing
}

// Helper function to safely get string values from query results
func getString(result map[string]interface{}, key string) string {
	if val, ok := result[key].(string); ok {
		return val
	}
	return ""
}

// SetupAlertAcksStream ensures the alert acknowledgments stream exists
func (c *Client) SetupAlertAcksStream(ctx context.Context) error {
	logrus.Info("Setting up alert acknowledgments stream")

	// Define schema
	schema := []Column{
		{Name: "alert_id", Type: "string"},
		{Name: "rule_id", Type: "string"},
		{Name: "state", Type: "string"},
		{Name: "updated_by", Type: "string"},
		{Name: "updated_at", Type: "datetime64"},
		{Name: "comment", Type: "string", Nullable: true},
		{Name: "valid_until", Type: "datetime64", Nullable: true},
	}

	// Create stream if it doesn't exist
	return c.CreateStream(ctx, AlertAcksStream, schema)
}

// CreateAlertAck adds a new alert acknowledgment record
func (c *Client) CreateAlertAck(ctx context.Context, alertAck AlertAck) error {
	logrus.Infof("Creating alert acknowledgment for alert %s", alertAck.AlertID)

	// Construct SQL to insert data
	sql := fmt.Sprintf(
		"INSERT INTO %s (alert_id, rule_id, state, updated_by, updated_at, comment, valid_until) VALUES ('%s', '%s', '%s', '%s', parseDateTimeBestEffort('%s'), '%s', parseDateTimeBestEffort('%s'))",
		AlertAcksStream,
		alertAck.AlertID,
		alertAck.RuleID,
		alertAck.State,
		alertAck.UpdatedBy,
		alertAck.UpdatedAt.Format(time.RFC3339),
		alertAck.Comment,
		alertAck.ValidUntil.Format(time.RFC3339),
	)

	_, err := c.ExecuteQuery(ctx, sql)
	return err
}

// AcknowledgeAlert updates an alert's state to acknowledged
func (c *Client) AcknowledgeAlert(ctx context.Context, alertID string, ruleID string, updatedBy string, comment string) error {
	alertAck := AlertAck{
		AlertID:   alertID,
		RuleID:    ruleID,
		State:     AlertStateAcknowledged,
		UpdatedBy: updatedBy,
		UpdatedAt: time.Now(),
		Comment:   comment,
	}

	return c.CreateAlertAck(ctx, alertAck)
}

// IsAlertAcknowledged checks if an alert is acknowledged or silenced
func (c *Client) IsAlertAcknowledged(ctx context.Context, alertID string) (bool, error) {
	sql := fmt.Sprintf(
		"SELECT state FROM %s WHERE alert_id = '%s' ORDER BY updated_at DESC LIMIT 1",
		AlertAcksStream,
		alertID,
	)

	results, err := c.ExecuteQuery(ctx, sql)
	if err != nil {
		return false, err
	}

	if len(results) == 0 {
		return false, nil
	}

	state := getString(results[0], "state")
	return state == AlertStateAcknowledged || state == AlertStateSilenced, nil
}

// CreateRuleResultsStream creates a stream for storing the results of a rule query
func (c *Client) CreateRuleResultsStream(ctx context.Context, ruleID string) error {
	resultsStreamName := fmt.Sprintf("rule_%s_results", ruleID)

	// Check if stream already exists
	streams, err := c.ListStreams(ctx)
	if err != nil {
		return fmt.Errorf("failed to list streams: %w", err)
	}

	for _, stream := range streams {
		if stream == resultsStreamName {
			logrus.Infof("Results stream %s already exists", resultsStreamName)
			return nil
		}
	}

	// Create a generic schema that can accept any columns from the query
	// We'll use the flexible approach since the rule can select any columns
	schema := []Column{
		{Name: "_tp_time", Type: "datetime64"},
		// The actual columns will be added dynamically based on the query
	}

	logrus.Infof("Creating results stream: %s", resultsStreamName)
	err = c.CreateStream(ctx, resultsStreamName, schema)
	if err != nil {
		return fmt.Errorf("failed to create results stream %s: %w", resultsStreamName, err)
	}

	logrus.Infof("Successfully created results stream: %s", resultsStreamName)
	return nil
}

// CreateRuleView creates a materialized view for a rule with throttling
func (c *Client) CreateRuleView(ctx context.Context, rule models.Rule) error {
	viewName := fmt.Sprintf("rule_%s_view", rule.ID)
	resultsStreamName := fmt.Sprintf("rule_%s_results", rule.ID)

	// First ensure the results stream exists
	if err := c.CreateRuleResultsStream(ctx, rule.ID); err != nil {
		return fmt.Errorf("failed to create results stream: %w", err)
	}

	// Drop view if it exists
	err := c.DropRuleView(ctx, rule.ID)
	if err != nil {
		logrus.Warnf("Failed to drop existing view: %v", err)
		// Continue anyway
	}

	// Construct query with throttling and INTO clause
	throttleSeconds := rule.ThrottleMinutes * 60
	viewQuery := fmt.Sprintf(
		"CREATE MATERIALIZED VIEW %s INTO %s AS %s EMIT CHANGES EVERY %d SECONDS",
		viewName,
		resultsStreamName,
		rule.Query,
		throttleSeconds,
	)

	logrus.Infof("Creating materialized view with query: %s", viewQuery)

	// Execute with retries due to eventual consistency
	maxAttempts := 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		_, err = c.ExecuteQuery(ctx, viewQuery)
		if err == nil {
			break
		}
		logrus.Warnf("Attempt %d to create view failed: %v", attempt, err)
		if attempt < maxAttempts {
			time.Sleep(1 * time.Second)
		}
	}

	if err != nil {
		return fmt.Errorf("failed to create materialized view after multiple attempts: %w", err)
	}

	return nil
}

// DropRuleView drops a materialized view for a rule
func (c *Client) DropRuleView(ctx context.Context, ruleID string) error {
	viewName := fmt.Sprintf("rule_%s_view", ruleID)
	dropQuery := fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %s", viewName)

	_, err := c.ExecuteQuery(ctx, dropQuery)
	return err
}

// GetAlertAcks retrieves alert acknowledgments with filtering options
func (c *Client) GetAlertAcks(ctx context.Context, options map[string]string) ([]AlertAck, error) {
	whereClause := ""

	// Build where clause based on options
	if alertID, ok := options["alert_id"]; ok {
		whereClause += fmt.Sprintf("alert_id = '%s'", alertID)
	}
	if ruleID, ok := options["rule_id"]; ok {
		if whereClause != "" {
			whereClause += " AND "
		}
		whereClause += fmt.Sprintf("rule_id = '%s'", ruleID)
	}
	if state, ok := options["state"]; ok {
		if whereClause != "" {
			whereClause += " AND "
		}
		whereClause += fmt.Sprintf("state = '%s'", state)
	}

	// Add where clause if any filters were applied
	query := fmt.Sprintf("SELECT * FROM %s", AlertAcksStream)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}
	query += " ORDER BY updated_at DESC"

	results, err := c.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	var alertAcks []AlertAck
	for _, result := range results {
		alertAck := AlertAck{
			AlertID:   getString(result, "alert_id"),
			RuleID:    getString(result, "rule_id"),
			State:     getString(result, "state"),
			UpdatedBy: getString(result, "updated_by"),
			// Handle datetime conversion
			Comment: getString(result, "comment"),
			// Handle valid_until
		}
		alertAcks = append(alertAcks, alertAck)
	}

	return alertAcks, nil
}

// GetAlertAck gets an alert acknowledgment by alert ID
func (c *Client) GetAlertAck(ctx context.Context, alertID string) (*AlertAck, error) {
	logrus.Infof("Getting alert acknowledgment for alert: %s", alertID)

	// Query the alert acknowledgments stream
	query := fmt.Sprintf("SELECT * FROM table(%s) WHERE alert_id = '%s' LIMIT 1", AlertAcksStream, alertID)
	results, err := c.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// If no results, return nil
	if len(results) == 0 {
		return nil, nil
	}

	// Parse the result into an AlertAck
	result := results[0]
	ack := &AlertAck{
		AlertID:   getString(result, "alert_id"),
		RuleID:    getString(result, "rule_id"),
		State:     getString(result, "state"),
		UpdatedBy: getString(result, "updated_by"),
		Comment:   getString(result, "comment"),
	}

	// Parse timestamps
	if updatedAt, ok := result["updated_at"].(time.Time); ok {
		ack.UpdatedAt = updatedAt
	}

	if validUntil, ok := result["valid_until"].(time.Time); ok {
		ack.ValidUntil = validUntil
	}

	return ack, nil
}
