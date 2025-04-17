package services

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/timeplus-io/tp-alert-gateway/pkg/models"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

// Stream names for persistent storage
const (
	RuleStreamName  = "tp_rules"
	AlertStreamName = "tp_alerts"
)

// RuleService manages the lifecycle of rules and their corresponding Timeplus resources
type RuleService struct {
	tpClient    timeplus.TimeplusClient
	ruleStream  string
	alertStream string
	// Map of rule ID to cancellation function for streaming queries
	ruleContexts     map[string]context.CancelFunc
	ruleContextMutex sync.RWMutex
	// Map of rule ID to cancel function for rule monitors
	ruleMonitors     map[string]context.CancelFunc
	ruleMonitorMutex sync.RWMutex
}

// NewRuleService creates a new rule service
func NewRuleService(tpClient timeplus.TimeplusClient) (*RuleService, error) {
	ctx := context.Background()

	// Ensure rule stream exists
	if err := ensureRuleStream(ctx, tpClient); err != nil {
		return nil, fmt.Errorf("failed to ensure rule stream exists: %w", err)
	}

	// Ensure alert stream exists
	if err := ensureAlertStream(ctx, tpClient); err != nil {
		return nil, fmt.Errorf("failed to ensure alert stream exists: %w", err)
	}

	service := &RuleService{
		tpClient:     tpClient,
		ruleStream:   RuleStreamName,
		alertStream:  AlertStreamName,
		ruleContexts: make(map[string]context.CancelFunc),
		ruleMonitors: make(map[string]context.CancelFunc),
	}

	// Start all rules that were previously in running state
	if err := service.resumeRunningRules(ctx); err != nil {
		logrus.Warnf("Error resuming running rules: %v", err)
	}

	return service, nil
}

// ensureRuleStream ensures that the rule stream exists
func ensureRuleStream(ctx context.Context, tpClient timeplus.TimeplusClient) error {
	exists, err := tpClient.StreamExists(ctx, RuleStreamName)
	if err != nil {
		return err
	}

	if !exists {
		logrus.Infof("Creating rule stream: %s", RuleStreamName)
		ruleSchema := []timeplus.Column{
			{Name: "id", Type: "string"},
			{Name: "name", Type: "string"},
			{Name: "description", Type: "string"},
			{Name: "query", Type: "string"},
			{Name: "status", Type: "string"},
			{Name: "severity", Type: "string"},
			{Name: "throttle_minutes", Type: "int32"},
			{Name: "entity_id_columns", Type: "string"},
			{Name: "created_at", Type: "datetime64"},
			{Name: "updated_at", Type: "datetime64"},
			{Name: "last_triggered_at", Type: "datetime64 NULL"},
			{Name: "source_stream", Type: "string"},
			{Name: "result_stream", Type: "string"},
			{Name: "view_name", Type: "string"},
			{Name: "last_error", Type: "string"},
			{Name: "_tp_time", Type: "datetime64"},
			{Name: "active", Type: "bool"}, // Flag to mark if a rule is active or deleted
		}
		if err := tpClient.CreateStream(ctx, RuleStreamName, ruleSchema); err != nil {
			return fmt.Errorf("failed to create rule stream: %w", err)
		}
	}
	return nil
}

// ensureAlertStream ensures that the alert stream exists
func ensureAlertStream(ctx context.Context, tpClient timeplus.TimeplusClient) error {
	exists, err := tpClient.StreamExists(ctx, AlertStreamName)
	if err != nil {
		return err
	}

	if !exists {
		logrus.Infof("Creating alert stream: %s", AlertStreamName)
		// Use the schema from timeplus package
		alertSchema := timeplus.GetAlertSchema()
		// Add the Timeplus timestamp column
		alertSchema = append(alertSchema, timeplus.Column{Name: "_tp_time", Type: "datetime64"})

		if err := tpClient.CreateStream(ctx, AlertStreamName, alertSchema); err != nil {
			return fmt.Errorf("failed to create alert stream: %w", err)
		}
	}
	return nil
}

// resumeRunningRules starts all rules that were in running state
func (s *RuleService) resumeRunningRules(ctx context.Context) error {
	rules, err := s.GetRules()
	if err != nil {
		return err
	}

	for _, rule := range rules {
		if rule.Status == models.RuleStatusRunning {
			logrus.Infof("Resuming rule: %s", rule.Name)
			if err := s.StartRule(ctx, rule.ID); err != nil {
				logrus.Errorf("Failed to resume rule %s: %v", rule.ID, err)
			}
		}
	}
	return nil
}

// GetRules returns all rules
func (s *RuleService) GetRules() ([]*models.Rule, error) {
	ctx := context.Background()

	// Query to get the latest version of each active rule
	query := fmt.Sprintf(`
		SELECT id, name, description, query, status, severity, 
		       throttle_minutes, entity_id_columns, created_at, updated_at, last_triggered_at,
		       source_stream, result_stream, view_name, last_error
		FROM (
			SELECT *, row_number() OVER (PARTITION BY id ORDER BY _tp_time DESC) as row_num
			FROM table(%s)
			WHERE active = true
		) WHERE row_num = 1
	`, s.ruleStream)

	results, err := s.tpClient.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query rules: %w", err)
	}

	rules := make([]*models.Rule, 0, len(results))
	for _, result := range results {
		rule := mapToRule(result)
		rules = append(rules, rule)
	}

	return rules, nil
}

// mapToRule converts a map of query results to a Rule struct
func mapToRule(data map[string]interface{}) *models.Rule {
	rule := &models.Rule{
		ID:              getString(data, "id"),
		Name:            getString(data, "name"),
		Description:     getString(data, "description"),
		Query:           getString(data, "query"),
		Status:          models.RuleStatus(getString(data, "status")),
		Severity:        models.RuleSeverity(getString(data, "severity")),
		ThrottleMinutes: getInt(data, "throttle_minutes"),
		EntityIDColumns: getString(data, "entity_id_columns"),
		SourceStream:    getString(data, "source_stream"),
		ResultStream:    getString(data, "result_stream"),
		ViewName:        getString(data, "view_name"),
		LastError:       getString(data, "last_error"),
	}

	// Handle dates
	if createdAt, ok := data["created_at"].(time.Time); ok {
		rule.CreatedAt = createdAt
	}
	if updatedAt, ok := data["updated_at"].(time.Time); ok {
		rule.UpdatedAt = updatedAt
	}
	if lastTriggered, ok := data["last_triggered_at"].(time.Time); ok {
		rule.LastTriggeredAt = &lastTriggered
	}

	return rule
}

// Helper functions to safely get values from map
func getString(data map[string]interface{}, key string) string {
	if val, ok := data[key].(string); ok {
		return val
	}
	return ""
}

func getInt(data map[string]interface{}, key string) int {
	switch v := data[key].(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

// GetRule returns a rule by ID
func (s *RuleService) GetRule(id string) (*models.Rule, error) {
	ctx := context.Background()

	// Query to get the latest version of the specified rule
	query := fmt.Sprintf(`
		SELECT id, name, description, query, status, severity, 
		       throttle_minutes, entity_id_columns, created_at, updated_at, last_triggered_at,
		       source_stream, result_stream, view_name, last_error
		FROM (
			SELECT *, row_number() OVER (PARTITION BY id ORDER BY _tp_time DESC) as row_num
			FROM table(%s)
			WHERE id = '%s' AND active = true
		) WHERE row_num = 1
	`, s.ruleStream, id)

	results, err := s.tpClient.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query rule: %w", err)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("rule with ID %s not found", id)
	}

	return mapToRule(results[0]), nil
}

// CreateRule creates a new rule
func (s *RuleService) CreateRule(ctx context.Context, req *models.CreateRuleRequest) (*models.Rule, error) {
	ruleID := uuid.New().String()
	now := time.Now()

	// Sanitize the rule ID for stream and view names by replacing hyphens with underscores
	sanitizedRuleID := strings.ReplaceAll(ruleID, "-", "_")

	// Create the rule
	rule := &models.Rule{
		ID:              ruleID,
		Name:            req.Name,
		Description:     req.Description,
		Query:           req.Query,
		Status:          models.RuleStatusCreated,
		Severity:        req.Severity,
		ThrottleMinutes: req.ThrottleMinutes,
		EntityIDColumns: req.EntityIDColumns,
		CreatedAt:       now,
		UpdatedAt:       now,
		SourceStream:    req.SourceStream,
		ResultStream:    fmt.Sprintf("rule_%s_results", sanitizedRuleID),
		ViewName:        fmt.Sprintf("rule_%s_view", sanitizedRuleID),
	}

	// Persist the rule to Timeplus
	if err := s.persistRule(ctx, rule, true); err != nil {
		return nil, fmt.Errorf("failed to persist rule: %w", err)
	}

	// Automatically start the rule after creation
	logrus.Infof("Auto-starting newly created rule: %s", rule.Name)
	go func() {
		// Use background context to avoid cancellation if the original request context is canceled
		startCtx := context.Background()
		if err := s.StartRule(startCtx, rule.ID); err != nil {
			logrus.Errorf("Failed to auto-start rule %s: %v", rule.ID, err)
		} else {
			logrus.Infof("Successfully auto-started rule %s", rule.ID)
		}
	}()

	return rule, nil
}

// persistRule persists a rule to the rule stream
func (s *RuleService) persistRule(ctx context.Context, rule *models.Rule, active bool) error {
	// Use time.Time objects directly for timestamps
	var lastTriggeredAt interface{}
	if rule.LastTriggeredAt != nil {
		lastTriggeredAt = *rule.LastTriggeredAt
	} else {
		lastTriggeredAt = nil // Use a nil value
	}

	// Define columns for insertion
	columns := []string{
		"id", "name", "description", "query", "status", "severity", "throttle_minutes",
		"entity_id_columns", "created_at", "updated_at", "last_triggered_at",
		"source_stream", "result_stream", "view_name", "last_error", "active",
	}

	// Prepare values for insertion
	values := []interface{}{
		rule.ID,
		rule.Name,
		rule.Description,
		rule.Query,
		string(rule.Status),
		string(rule.Severity),
		rule.ThrottleMinutes,
		rule.EntityIDColumns,
		rule.CreatedAt,
		rule.UpdatedAt,
		lastTriggeredAt, // Pass directly, InsertIntoStream handles formatting
		rule.SourceStream,
		rule.ResultStream,
		rule.ViewName,
		rule.LastError,
		active,
	}

	// Use the client's InsertIntoStream method
	err := s.tpClient.InsertIntoStream(ctx, s.ruleStream, columns, values)
	return err
}

// UpdateRule updates an existing rule
func (s *RuleService) UpdateRule(ctx context.Context, id string, req *models.UpdateRuleRequest) (*models.Rule, error) {
	// Get current rule
	rule, err := s.GetRule(id)
	if err != nil {
		return nil, err
	}

	// Can only update if rule is in created or stopped state
	if rule.Status != models.RuleStatusCreated && rule.Status != models.RuleStatusStopped {
		return nil, fmt.Errorf("cannot update rule in %s state", rule.Status)
	}

	// Update fields if provided
	if req.Name != nil {
		rule.Name = *req.Name
	}
	if req.Description != nil {
		rule.Description = *req.Description
	}
	if req.Query != nil {
		rule.Query = *req.Query
	}
	if req.Severity != nil {
		rule.Severity = *req.Severity
	}
	if req.ThrottleMinutes != nil {
		rule.ThrottleMinutes = *req.ThrottleMinutes
	}
	if req.EntityIDColumns != nil {
		rule.EntityIDColumns = *req.EntityIDColumns
	}

	rule.UpdatedAt = time.Now()

	// Persist the updated rule
	if err := s.persistRule(ctx, rule, true); err != nil {
		return nil, fmt.Errorf("failed to persist updated rule: %w", err)
	}

	return rule, nil
}

// DeleteRule deletes a rule
func (s *RuleService) DeleteRule(ctx context.Context, id string) error {
	// First, stop the rule if it's running
	if err := s.StopRule(ctx, id); err != nil {
		logrus.Warnf("Error stopping rule %s before deletion: %v", id, err)
	}

	// Get the rule
	rule, err := s.GetRule(id)
	if err != nil {
		return err
	}

	// Cleanup Timeplus resources
	if err := s.tpClient.DeleteMaterializedView(ctx, rule.ViewName); err != nil {
		logrus.Warnf("Error deleting materialized view %s: %v", rule.ViewName, err)
	}

	// Delete the alert acks view as well
	acksViewName := fmt.Sprintf("rule_%s_acks_view", rule.ID)
	if err := s.tpClient.DeleteMaterializedView(ctx, acksViewName); err != nil {
		logrus.Warnf("Error deleting alert acks view %s: %v", acksViewName, err)
	}

	if err := s.tpClient.DeleteStream(ctx, rule.ResultStream); err != nil {
		logrus.Warnf("Error deleting result stream %s: %v", rule.ResultStream, err)
	}

	// Mark the rule as inactive rather than physically deleting it
	// This is a soft delete approach
	rule.Status = models.RuleStatusStopped
	rule.UpdatedAt = time.Now()

	if err := s.persistRule(ctx, rule, false); err != nil {
		return fmt.Errorf("failed to mark rule as deleted: %w", err)
	}

	return nil
}

// setupAlertAcksStream ensures the alert acknowledgements stream exists
func (s *RuleService) setupAlertAcksStream(ctx context.Context) error {
	logrus.Info("Setting up mutable alert acknowledgments stream")
	return s.tpClient.SetupMutableAlertAcksStream(ctx)
}

// StartRule starts a rule by setting up a materialized view
func (s *RuleService) StartRule(ctx context.Context, ruleID string) error {
	// Add a timeout to the context
	timeoutCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	// Add a small delay to allow the rule persistence to become consistent
	time.Sleep(500 * time.Millisecond) // Wait 500ms

	rule, err := s.GetRule(ruleID)
	if err != nil {
		return err
	}

	// Already running
	if rule.Status == models.RuleStatusRunning {
		return nil
	}

	// First, ensure the alert acknowledgments stream is set up
	if err := s.setupAlertAcksStream(timeoutCtx); err != nil {
		logrus.Errorf("Failed to setup alert acknowledgments stream: %v", err)
		rule.Status = models.RuleStatusFailed
		rule.LastError = fmt.Sprintf("Failed to setup alert acknowledgments stream: %v", err)
		s.persistRule(timeoutCtx, rule, true)
		return fmt.Errorf("failed to setup alert acknowledgments stream: %w", err)
	}

	// We need to create a view name for both regular view and materialized view
	sanitizedRuleID := GetFormattedRuleID(rule.ID)
	plainViewName := fmt.Sprintf("rule_%s_view", sanitizedRuleID)
	materializedViewName := fmt.Sprintf("rule_%s_mv", sanitizedRuleID)

	// Force drop existing views with retries to ensure we're starting clean
	dropViews := []string{plainViewName, materializedViewName}
	for _, viewName := range dropViews {
		// Try up to 3 times to drop each view
		for i := 0; i < 3; i++ {
			// First try DROP VIEW IF EXISTS (works for plain views)
			dropQuery := fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName)
			// Use ExecuteDDL for DROP VIEW
			err := s.tpClient.ExecuteDDL(timeoutCtx, dropQuery)
			if err != nil {
				logrus.Warnf("Attempt %d to drop view %s failed: %v", i+1, viewName, err)
				// If it failed, try DROP MATERIALIZED VIEW directly (already uses Exec internally)
				err = s.tpClient.DeleteMaterializedView(timeoutCtx, viewName)
				if err != nil {
					logrus.Warnf("Attempt %d to drop materialized view %s failed: %v", i+1, viewName, err)
				} else {
					logrus.Infof("Successfully dropped materialized view: %s", viewName)
					break
				}
			} else {
				logrus.Infof("Successfully dropped view: %s", viewName)
				break
			}

			if i < 2 {
				// Wait before retrying
				time.Sleep(3 * time.Second)
			}
		}
	}

	// Give the system some time to properly release the views
	time.Sleep(2 * time.Second)

	// Step 2: Create a plain VIEW for the rule query
	plainViewQuery := timeplus.GetRulePlainViewQuery(rule.ID, rule.Query)
	logrus.Infof("Creating plain view with query: %s", plainViewQuery)

	// Create the plain view with retries
	var plainViewErr error
	for attempt := 1; attempt <= 3; attempt++ {
		// Use ExecuteDDL for CREATE VIEW
		plainViewErr = s.tpClient.ExecuteDDL(timeoutCtx, plainViewQuery)
		if plainViewErr == nil {
			break
		}

		// If view already exists (which might happen if DROP failed), try dropping again
		if strings.Contains(plainViewErr.Error(), "already exists") {
			logrus.Warnf("View already exists, trying to forcefully drop it again")
			dropQuery := fmt.Sprintf("DROP VIEW IF EXISTS %s", plainViewName)
			// Use ExecuteDDL here too
			s.tpClient.ExecuteDDL(timeoutCtx, dropQuery)
			time.Sleep(2 * time.Second)
		} else {
			logrus.Warnf("Attempt %d to create plain view failed: %v", attempt, plainViewErr)
		}

		if attempt < 3 {
			time.Sleep(3 * time.Second)
		}
	}

	if plainViewErr != nil {
		logrus.Errorf("Failed to create plain view: %v", plainViewErr)
		rule.Status = models.RuleStatusFailed
		rule.LastError = fmt.Sprintf("Failed to create plain view: %v", plainViewErr)
		s.persistRule(timeoutCtx, rule, true)
		return fmt.Errorf("failed to create plain view: %w", plainViewErr)
	}

	// Step 3: Determine which column to use as the entity_id from the plain view
	// First, we need to inspect the columns available in the view
	columnsQuery := fmt.Sprintf("DESCRIBE %s", plainViewName)
	columnResults, err := s.tpClient.ExecuteQuery(timeoutCtx, columnsQuery)
	if err != nil {
		logrus.Errorf("Failed to get view columns: %v", err)
		rule.Status = models.RuleStatusFailed
		rule.LastError = fmt.Sprintf("Failed to get view columns: %v", err)
		s.persistRule(timeoutCtx, rule, true)
		return fmt.Errorf("failed to get view columns: %w", err)
	}

	// Find a suitable ID column (prioritize common ID field names)
	idColumnName := ""
	var foundColumns []string

	// Check if the rule has EntityIDColumns defined
	if rule.EntityIDColumns != "" {
		// Split the comma-separated list
		userSpecifiedColumns := strings.Split(rule.EntityIDColumns, ",")

		// Trim whitespace from each column name
		for i := range userSpecifiedColumns {
			userSpecifiedColumns[i] = strings.TrimSpace(userSpecifiedColumns[i])
		}

		// Find all specified columns that exist in the results
		for _, column := range columnResults {
			colName := ""
			if name, ok := column["name"].(string); ok {
				colName = name
			}

			for _, userCol := range userSpecifiedColumns {
				if colName == userCol {
					foundColumns = append(foundColumns, colName)
					break
				}
			}
		}

		// If we found any matching columns
		if len(foundColumns) > 0 {
			if len(foundColumns) == 1 {
				// If only one column, use it directly
				idColumnName = foundColumns[0]
			} else {
				// If multiple columns, we need to create a modified view with concatenation

				// Drop the original view first
				// Use ExecuteDDL
				err = s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", plainViewName))
				if err != nil {
					logrus.Warnf("Error dropping plain view for concatenation: %v", err)
				}

				// Create a concatenation expression
				// Build the concatenation expression with separators
				var concatParts []string
				for i, col := range foundColumns {
					if i > 0 {
						concatParts = append(concatParts, "'_'") // Add underscore separator
					}
					concatParts = append(concatParts, col)
				}
				concatenationExpr := fmt.Sprintf("concat(%s)", strings.Join(concatParts, ", "))

				// Recreate the view with the concatenated entity_id
				modifiedQuery := fmt.Sprintf("CREATE VIEW %s AS SELECT *, %s AS entity_id FROM (%s)",
					plainViewName, concatenationExpr, rule.Query)
				// Use ExecuteDDL
				err = s.tpClient.ExecuteDDL(timeoutCtx, modifiedQuery)
				if err != nil {
					logrus.Errorf("Failed to create modified plain view with concatenation: %v", err)
					rule.Status = models.RuleStatusFailed
					rule.LastError = fmt.Sprintf("Failed to create modified plain view with concatenation: %v", err)
					s.persistRule(timeoutCtx, rule, true)
					return fmt.Errorf("failed to create modified plain view with concatenation: %w", err)
				}

				// Use the generated entity_id
				idColumnName = "entity_id"
				logrus.Infof("Created concatenated entity_id from columns: %v", foundColumns)
			}
		} else {
			// If specified column not found, log a warning
			logrus.Warnf("None of the specified entity ID columns %v found in the results", userSpecifiedColumns)
		}
	}

	// Fall back to the default priority columns if no user columns matched
	if idColumnName == "" {
		priorityColumns := []string{"entity_id", "device_id", "id", "host", "ip", "user_id"}

		// First check for priority columns
		for _, column := range columnResults {
			colName := ""
			if name, ok := column["name"].(string); ok {
				colName = name
			}

			for _, priority := range priorityColumns {
				if colName == priority {
					idColumnName = colName
					break
				}
			}

			if idColumnName != "" {
				break
			}
		}
	}

	// If no priority column found, use the first string column
	if idColumnName == "" {
		for _, column := range columnResults {
			colName := ""
			colType := ""
			if name, ok := column["name"].(string); ok {
				colName = name
			}
			if typ, ok := column["type"].(string); ok {
				colType = typ
			}

			if strings.Contains(colType, "string") {
				idColumnName = colName
				break
			}
		}
	}

	// If still no suitable column found, create a hash of _tp_time as the entity_id
	if idColumnName == "" {
		// Add a hashed _tp_time column to the view
		// Drop the original view first
		// Use ExecuteDDL
		err = s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", plainViewName))
		if err != nil {
			logrus.Warnf("Error dropping plain view for modification: %v", err)
		}

		// Recreate with a hashed _tp_time field
		modifiedQuery := fmt.Sprintf("CREATE VIEW %s AS SELECT *, lower(hex(md5(toString(_tp_time)))) AS entity_id FROM (%s)",
			plainViewName, rule.Query)
		// Use ExecuteDDL
		err = s.tpClient.ExecuteDDL(timeoutCtx, modifiedQuery)
		if err != nil {
			logrus.Errorf("Failed to create modified plain view: %v", err)
			rule.Status = models.RuleStatusFailed
			rule.LastError = fmt.Sprintf("Failed to create modified plain view: %v", err)
			s.persistRule(timeoutCtx, rule, true)
			return fmt.Errorf("failed to create modified plain view: %w", err)
		}

		idColumnName = "entity_id"
	}

	logrus.Infof("Using column '%s' as the entity_id for rule %s", idColumnName, rule.ID)

	// Construct the expression to capture triggering data for the comment field as JSON
	var dataCaptureParts []string
	for _, column := range columnResults {
		colName := ""
		if name, ok := column["name"].(string); ok {
			colName = name
		}
		// Skip internal columns and the potentially generated entity_id column
		if colName == "" || colName == "_tp_time" || colName == "_tp_sn" || colName == idColumnName {
			continue
		}
		// Format as '"key": "' || to_string(value) || '"'
		part := fmt.Sprintf("concat('\"%s\": \"', to_string(`%s`), '\"')", colName, colName)
		dataCaptureParts = append(dataCaptureParts, part)
	}

	triggeringDataExpr := "'{}'" // Default to empty JSON object
	if len(dataCaptureParts) > 0 {
		// Construct the SQL array content string: [part1_expr, part2_expr, ...]
		// Each part is already a valid SQL expression producing a string.
		sqlArrayContent := strings.Join(dataCaptureParts, ", ") // Join the SQL expressions with commas

		// Use this array content in array_string_concat
		joinedPartsExpr := fmt.Sprintf("array_string_concat([%s], ', ')", sqlArrayContent)

		// Wrap with {} using SQL concat
		triggeringDataExpr = fmt.Sprintf("concat('{', %s, '}')", joinedPartsExpr)
	}
	logrus.Infof("Built triggering JSON expression: %s", triggeringDataExpr)

	// Step 4: Create a materialized view that joins with tp_alert_acks_mutable for throttling
	materializedViewQuery := timeplus.GetRuleThrottledMaterializedViewQuery(
		rule.ID,
		rule.ThrottleMinutes,
		idColumnName,
		triggeringDataExpr, // Pass the constructed JSON expression
	)

	logrus.Infof("Creating materialized view with query: %s", materializedViewQuery)

	// Create the materialized view with retries
	maxAttempts := 3
	var createErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Use ExecuteDDL for CREATE MATERIALIZED VIEW
		createErr = s.tpClient.ExecuteDDL(timeoutCtx, materializedViewQuery)
		if createErr == nil {
			break
		}
		logrus.Warnf("Attempt %d to create materialized view failed: %v", attempt, createErr)
		if attempt < maxAttempts {
			time.Sleep(2 * time.Second)
		}
	}

	if createErr != nil {
		logrus.Errorf("Failed to create materialized view: %v", createErr)
		rule.Status = models.RuleStatusFailed
		rule.LastError = fmt.Sprintf("Failed to create materialized view: %v", createErr)
		s.persistRule(timeoutCtx, rule, true)
		return fmt.Errorf("failed to create throttled materialized view: %w", createErr)
	}

	// Update rule status to running
	rule.Status = models.RuleStatusRunning
	rule.UpdatedAt = time.Now()

	if err := s.persistRule(timeoutCtx, rule, true); err != nil {
		return fmt.Errorf("failed to update rule status: %w", err)
	}

	return nil
}

// GetAlerts returns all alerts, optionally filtered by rule ID
func (s *RuleService) GetAlerts(ruleID string) ([]*models.Alert, error) {
	ctx := context.Background()

	// Query from tp_alert_acks_mutable with table() function and proper field mapping
	var query string
	if ruleID == "" {
		query = fmt.Sprintf(`
			SELECT 
				uuid() as id, 
				rule_id, 
				entity_id,
				state,
				created_at,
				updated_at,
				updated_by,
				comment
			FROM table(%s)
			ORDER BY created_at DESC
			LIMIT 1000
		`, timeplus.AlertAcksMutableStream)
	} else {
		query = fmt.Sprintf(`
			SELECT 
				uuid() as id, 
				rule_id,
				entity_id,
				state,
				created_at,
				updated_at,
				updated_by,
				comment
			FROM table(%s)
			WHERE rule_id = '%s'
			ORDER BY created_at DESC
			LIMIT 1000
		`, timeplus.AlertAcksMutableStream, ruleID)
	}

	logrus.Infof("GetAlerts query: %s", query)
	results, err := s.tpClient.ExecuteQuery(ctx, query)
	if err != nil {
		logrus.Errorf("Error querying alerts: %v", err)
		return nil, fmt.Errorf("failed to query alerts: %w", err)
	}

	// Map to alerts, but first fetch rule details to get names and severities
	ruleDetails := make(map[string]*models.Rule)
	alerts := make([]*models.Alert, 0, len(results))

	// Collect all unique rule IDs
	ruleIDs := make(map[string]bool)
	for _, result := range results {
		if rID, ok := result["rule_id"].(string); ok {
			ruleIDs[rID] = true
		}
	}

	// Fetch rule details for all involved rules
	for rID := range ruleIDs {
		rule, err := s.GetRule(rID)
		if err == nil {
			ruleDetails[rID] = rule
		}
	}

	// Create alert objects with rule details
	for _, result := range results {
		alert := &models.Alert{
			ID:     getString(result, "id"),
			RuleID: getString(result, "rule_id"),
		}

		// Add rule details if available
		if rule, ok := ruleDetails[alert.RuleID]; ok {
			alert.RuleName = rule.Name
			alert.Severity = rule.Severity
		} else {
			alert.RuleName = "Unknown Rule"
			alert.Severity = models.RuleSeverity("info")
		}

		// Get entity ID and create data field
		entityID := getString(result, "entity_id")
		state := getString(result, "state")
		alert.Data = fmt.Sprintf(`{"entity_id":"%s","state":"%s"}`, entityID, state)

		// Set acknowledged status based on state
		alert.Acknowledged = state != timeplus.AlertStateActive
		alert.AcknowledgedBy = getString(result, "updated_by")

		// Handle dates
		if createdAt, ok := result["created_at"].(time.Time); ok {
			alert.TriggeredAt = createdAt
		}

		// For acknowledged alerts, updated_at represents acknowledged_at
		if alert.Acknowledged {
			if updatedAt, ok := result["updated_at"].(time.Time); ok {
				alert.AcknowledgedAt = &updatedAt
			}
		}

		alerts = append(alerts, alert)
	}

	return alerts, nil
}

// GetAlertsByTimeRange returns alerts within a specified time range
func (s *RuleService) GetAlertsByTimeRange(ruleID string, startTime, endTime time.Time) ([]*models.Alert, error) {
	ctx := context.Background()

	// Format timestamps
	startStr := startTime.Format(time.RFC3339)
	endStr := endTime.Format(time.RFC3339)

	// Build query based on whether a rule ID is provided, but using tp_alert_acks_mutable
	var query string
	if ruleID == "" {
		query = fmt.Sprintf(`
			SELECT 
				uuid() as id, 
				rule_id, 
				entity_id,
				state,
				created_at,
				updated_at,
				updated_by,
				comment
			FROM table(%s)
			WHERE created_at >= '%s' AND created_at <= '%s'
			ORDER BY created_at DESC
			LIMIT 1000
		`, timeplus.AlertAcksMutableStream, startStr, endStr)
	} else {
		query = fmt.Sprintf(`
			SELECT 
				uuid() as id, 
				rule_id,
				entity_id,
				state,
				created_at,
				updated_at,
				updated_by,
				comment
			FROM table(%s)
			WHERE rule_id = '%s' AND created_at >= '%s' AND created_at <= '%s'
			ORDER BY created_at DESC
			LIMIT 1000
		`, timeplus.AlertAcksMutableStream, ruleID, startStr, endStr)
	}

	logrus.Infof("GetAlertsByTimeRange query: %s", query)
	results, err := s.tpClient.ExecuteQuery(ctx, query)
	if err != nil {
		logrus.Errorf("Error querying alerts by time range: %v", err)
		return nil, fmt.Errorf("failed to query alerts by time range: %w", err)
	}

	// Map to alerts with rule details
	ruleDetails := make(map[string]*models.Rule)
	alerts := make([]*models.Alert, 0, len(results))

	// Collect all unique rule IDs
	ruleIDs := make(map[string]bool)
	for _, result := range results {
		if rID, ok := result["rule_id"].(string); ok {
			ruleIDs[rID] = true
		}
	}

	// Fetch rule details for all involved rules
	for rID := range ruleIDs {
		rule, err := s.GetRule(rID)
		if err == nil {
			ruleDetails[rID] = rule
		}
	}

	// Create alert objects
	for _, result := range results {
		alert := &models.Alert{
			ID:     getString(result, "id"),
			RuleID: getString(result, "rule_id"),
		}

		// Add rule details if available
		if rule, ok := ruleDetails[alert.RuleID]; ok {
			alert.RuleName = rule.Name
			alert.Severity = rule.Severity
		} else {
			alert.RuleName = "Unknown Rule"
			alert.Severity = models.RuleSeverity("info")
		}

		// Get entity ID and create data field
		entityID := getString(result, "entity_id")
		state := getString(result, "state")
		alert.Data = fmt.Sprintf(`{"entity_id":"%s","state":"%s"}`, entityID, state)

		// Set acknowledged status based on state
		alert.Acknowledged = state != timeplus.AlertStateActive
		alert.AcknowledgedBy = getString(result, "updated_by")

		// Handle dates
		if createdAt, ok := result["created_at"].(time.Time); ok {
			alert.TriggeredAt = createdAt
		}

		// For acknowledged alerts, updated_at represents acknowledged_at
		if alert.Acknowledged {
			if updatedAt, ok := result["updated_at"].(time.Time); ok {
				alert.AcknowledgedAt = &updatedAt
			}
		}

		alerts = append(alerts, alert)
	}

	return alerts, nil
}

// GetAlert returns a single alert by ID
func (s *RuleService) GetAlert(alertID string) (*models.Alert, error) {
	// Parse composite ID to get rule_id and entity_id
	parts := strings.Split(alertID, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid alert ID format, expected 'rule_id:entity_id'")
	}

	ruleID := parts[0]
	entityID := parts[1]

	// Get rule details first
	rule, err := s.GetRule(ruleID)
	if err != nil {
		logrus.Warnf("Failed to get rule details for alert %s: %v", alertID, err)
		// Continue anyway, we'll create an alert with minimal information
	}

	// Query the alert from the mutable stream
	ctx := context.Background()
	query := fmt.Sprintf(`
		SELECT 
			'%s' as id,
			rule_id,
			entity_id,
			state,
			created_at,
			updated_at,
			updated_by,
			comment
		FROM table(%s) 
		WHERE rule_id = '%s' AND entity_id = '%s'
		ORDER BY updated_at DESC 
		LIMIT 1
	`, alertID, timeplus.AlertAcksMutableStream, ruleID, entityID)

	logrus.Infof("GetAlert query: %s", query)
	results, err := s.tpClient.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query alert %s: %w", alertID, err)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("alert %s not found", alertID)
	}

	// Create the alert with available information
	result := results[0]
	alert := &models.Alert{
		ID:     alertID,
		RuleID: ruleID,
	}

	// Add rule details if available
	if rule != nil {
		alert.RuleName = rule.Name
		alert.Severity = rule.Severity
	} else {
		alert.RuleName = "Unknown Rule"
		alert.Severity = models.RuleSeverity("info")
	}

	// Get entity ID and create data field
	entityVal := getString(result, "entity_id")
	state := getString(result, "state")
	alert.Data = fmt.Sprintf(`{"entity_id":"%s","state":"%s"}`, entityVal, state)

	// Set acknowledged status based on state
	alert.Acknowledged = state != timeplus.AlertStateActive
	alert.AcknowledgedBy = getString(result, "updated_by")

	// Handle dates
	if createdAt, ok := result["created_at"].(time.Time); ok {
		alert.TriggeredAt = createdAt
	}

	// For acknowledged alerts, updated_at represents acknowledged_at
	if alert.Acknowledged {
		if updatedAt, ok := result["updated_at"].(time.Time); ok {
			alert.AcknowledgedAt = &updatedAt
		}
	}

	return alert, nil
}

// AcknowledgeAlert acknowledges an alert
func (s *RuleService) AcknowledgeAlert(id string, acknowledgedBy string) error {
	// Parse the id which should be in format rule_id:entity_id
	parts := strings.Split(id, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid alert ID format, expected 'rule_id:entity_id'")
	}

	ruleID := parts[0]
	entityID := parts[1]

	return s.AcknowledgeDevice(context.Background(), ruleID, entityID, acknowledgedBy, "Acknowledged via API")
}

// StopRule stops a rule in the new implementation
func (s *RuleService) StopRule(ctx context.Context, ruleID string) error {
	rule, err := s.GetRule(ruleID)
	if err != nil {
		return err
	}

	if rule.Status != models.RuleStatusRunning {
		return fmt.Errorf("rule is not running")
	}

	// Find and drop the alert generation view
	alertViewName := fmt.Sprintf("rule_%s_alert_view", rule.ID)
	streams, err := s.tpClient.ListStreams(ctx)
	if err != nil {
		logrus.Warnf("Error listing streams: %v", err)
	} else {
		for _, stream := range streams {
			if stream == alertViewName {
				logrus.Infof("Dropping alert generation view %s", alertViewName)
				_, err := s.tpClient.ExecuteQuery(ctx, fmt.Sprintf("DROP VIEW `%s`", alertViewName))
				if err != nil {
					logrus.Warnf("Error dropping alert generation view: %v", err)
				}
			}
		}
	}

	// Delete the materialized view
	if err := s.tpClient.DeleteMaterializedView(ctx, rule.ViewName); err != nil {
		logrus.Warnf("Error deleting materialized view %s: %v", rule.ViewName, err)
	}

	// Delete the alert acks view as well
	acksViewName := fmt.Sprintf("rule_%s_acks_view", rule.ID)
	if err := s.tpClient.DeleteMaterializedView(ctx, acksViewName); err != nil {
		logrus.Warnf("Error deleting alert acks view %s: %v", acksViewName, err)
	}

	// Update rule status
	rule.Status = models.RuleStatusStopped
	rule.UpdatedAt = time.Now()

	return s.persistRule(ctx, rule, true)
}

// persistAlert persists an alert to the alert stream
func (s *RuleService) persistAlert(ctx context.Context, alert *models.Alert) error {
	// Use time.Time objects directly
	var acknowledgedAtStr string
	if alert.AcknowledgedAt != nil {
		acknowledgedAtStr = fmt.Sprintf("'%s'", alert.AcknowledgedAt.Format("2006-01-02 15:04:05.000"))
	} else {
		acknowledgedAtStr = "null"
	}

	// Create a query to insert the alert using explicit columns
	query := fmt.Sprintf(`
		INSERT INTO %s 
		(id, rule_id, rule_name, severity, triggered_at,
		 data, acknowledged, acknowledged_at, acknowledged_by)
		VALUES 
		('%s', '%s', '%s', '%s', '%s',
		 '%s', %t, %s, '%s')`,
		s.alertStream,
		strings.ReplaceAll(alert.ID, "'", "''"),
		strings.ReplaceAll(alert.RuleID, "'", "''"),
		strings.ReplaceAll(alert.RuleName, "'", "''"),
		strings.ReplaceAll(string(alert.Severity), "'", "''"),
		alert.TriggeredAt.Format("2006-01-02 15:04:05.000"),
		strings.ReplaceAll(alert.Data, "'", "''"),
		alert.Acknowledged,
		acknowledgedAtStr,
		strings.ReplaceAll(alert.AcknowledgedBy, "'", "''"),
	)

	// Execute the query directly
	_, err := s.tpClient.ExecuteQuery(ctx, query)
	return err
}

// GetTimeplusClient returns the Timeplus client
func (s *RuleService) GetTimeplusClient() timeplus.TimeplusClient {
	return s.tpClient
}

// GetActiveAlertAcks retrieves active alert acknowledgments from the mutable stream
func (s *RuleService) GetActiveAlertAcks(ctx context.Context, ruleID string, entityID string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM table(%s)", timeplus.AlertAcksMutableStream)

	// Add filter conditions if provided
	whereConditions := []string{}
	if ruleID != "" {
		whereConditions = append(whereConditions, fmt.Sprintf("rule_id = '%s'", ruleID))
	}
	if entityID != "" {
		whereConditions = append(whereConditions, fmt.Sprintf("entity_id = '%s'", entityID))
	}

	// Add state filter for active alerts
	whereConditions = append(whereConditions, fmt.Sprintf("state = '%s'", timeplus.AlertStateActive))

	// Add WHERE clause if any conditions
	if len(whereConditions) > 0 {
		query += " WHERE " + strings.Join(whereConditions, " AND ")
	}

	// Order by updated_at to get the most recent first
	query += " ORDER BY updated_at DESC"

	// Execute the query
	logrus.Infof("Querying alert acks with: %s", query)
	results, err := s.tpClient.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query alert acknowledgments: %w", err)
	}

	return results, nil
}

// AcknowledgeDevice acknowledges all active alerts for a specific entity
// entityID can be any identifier that uniquely identifies the alerting entity
// (device ID, IP address, user ID, transaction ID, etc.)
func (s *RuleService) AcknowledgeDevice(ctx context.Context, ruleID string, entityID string, acknowledgedBy string, comment string) error {
	// First, check if there are any active alerts for this entity
	acks, err := s.GetActiveAlertAcks(ctx, ruleID, entityID)
	if err != nil {
		return fmt.Errorf("failed to check for active alerts: %w", err)
	}

	if len(acks) == 0 {
		return fmt.Errorf("no active alerts found for entity %s with rule %s", entityID, ruleID)
	}

	// Update the alert acknowledgment in the mutable stream
	updateQuery := fmt.Sprintf(`
		INSERT INTO %s (rule_id, entity_id, state, created_at, updated_at, updated_by, comment)
		VALUES ('%s', '%s', '%s', now(), now(), '%s', '%s')
	`,
		timeplus.AlertAcksMutableStream,
		ruleID,
		entityID,
		timeplus.AlertStateAcknowledged,
		acknowledgedBy,
		comment)

	_, err = s.tpClient.ExecuteQuery(ctx, updateQuery)
	if err != nil {
		return fmt.Errorf("failed to acknowledge entity: %w", err)
	}

	logrus.Infof("Entity %s with rule %s acknowledged by %s", entityID, ruleID, acknowledgedBy)
	return nil
}

// CreateAlertFromData creates a new alert directly in the tp_alerts stream
// entityID is a generic identifier for the entity that triggered the alert
func (s *RuleService) CreateAlertFromData(ctx context.Context, rule *models.Rule, entityID string, extraData map[string]interface{}) (string, error) {
	// Generate a new alert ID
	alertID := uuid.New().String()
	now := time.Now()

	// Prepare data JSON
	data := map[string]interface{}{
		"entity_id": entityID,
	}

	// Add extra data
	for k, v := range extraData {
		data[k] = v
	}

	// Convert to JSON
	dataJSON, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal data to JSON: %w", err)
	}

	// Create alert object
	alert := &models.Alert{
		ID:           alertID,
		RuleID:       rule.ID,
		RuleName:     rule.Name,
		Severity:     rule.Severity,
		TriggeredAt:  now,
		Data:         string(dataJSON),
		Acknowledged: false,
	}

	// Persist to alert stream
	if err := s.persistAlert(ctx, alert); err != nil {
		return "", fmt.Errorf("failed to persist alert: %w", err)
	}

	logrus.Infof("Created alert %s for rule %s (entity %s)", alertID, rule.ID, entityID)
	return alertID, nil
}

// Helper function to safely get boolean values from map
func getBool(data map[string]interface{}, key string) bool {
	if val, ok := data[key].(bool); ok {
		return val
	}
	return false
}
