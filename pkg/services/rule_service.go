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

// ensureRuleStream ensures that the rule stream exists and is mutable
func ensureRuleStream(ctx context.Context, tpClient timeplus.TimeplusClient) error {
	exists, err := tpClient.StreamExists(ctx, RuleStreamName)
	if err != nil {
		return err
	}

	if !exists {
		logrus.Infof("Creating mutable rule stream: %s", RuleStreamName)
		ruleSchema := []timeplus.Column{
			{Name: "id", Type: "string"},
			{Name: "name", Type: "string"},
			{Name: "description", Type: "string"},
			{Name: "query", Type: "string"},
			{Name: "resolve_query", Type: "string", Nullable: true},
			{Name: "status", Type: "string"},
			{Name: "severity", Type: "string"},
			{Name: "throttle_minutes", Type: "int32"},
			{Name: "entity_id_columns", Type: "string"},
			{Name: "created_at", Type: "datetime64"},
			{Name: "updated_at", Type: "datetime64"},
			{Name: "last_triggered_at", Type: "datetime64", Nullable: true},
			{Name: "result_stream", Type: "string"},
			{Name: "view_name", Type: "string"},
			{Name: "resolve_view_name", Type: "string", Nullable: true},
			{Name: "last_error", Type: "string", Nullable: true},
			{Name: "dedicated_alert_acks_stream", Type: "bool", Nullable: true},
			{Name: "alert_acks_stream_name", Type: "string", Nullable: true},
			{Name: "_tp_time", Type: "datetime64"},
			{Name: "active", Type: "bool"},
		}

		// Construct the CREATE MUTABLE STREAM query manually
		columnsStr := ""
		for i, col := range ruleSchema {
			if i > 0 {
				columnsStr += ", "
			}
			nullableStr := ""
			if col.Nullable {
				nullableStr = " NULL"
			}
			columnsStr += fmt.Sprintf("`%s` %s%s", col.Name, col.Type, nullableStr)
		}
		// Define primary key
		primaryKeys := []string{"id"}
		pkStr := fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, ", "))

		createMutableStreamQuery := fmt.Sprintf("CREATE MUTABLE STREAM `%s` (%s) %s",
			RuleStreamName, columnsStr, pkStr)

		logrus.Infof("Executing query to create mutable rule stream: %s", createMutableStreamQuery)
		if err := tpClient.ExecuteDDL(ctx, createMutableStreamQuery); err != nil {
			return fmt.Errorf("failed to create mutable rule stream %s: %w", RuleStreamName, err)
		}

		logrus.Infof("Created mutable rule stream: %s", RuleStreamName)
	}

	// TODO: Handle schema migration if stream exists but schema is outdated?
	// For now, assume if it exists, it's correct or needs manual intervention.
	logrus.Infof("Mutable rule stream '%s' exists.", RuleStreamName)
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

	// Query to get the latest version of each active rule - removed source_stream
	query := fmt.Sprintf(`
		SELECT id, name, description, query, status, severity, 
			   throttle_minutes, entity_id_columns, created_at, updated_at, last_triggered_at,
			   result_stream, view_name, last_error,
			   dedicated_alert_acks_stream, alert_acks_stream_name
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

// mapToRule maps a data map to a Rule struct
func mapToRule(data map[string]interface{}) *models.Rule {
	// Log the raw data for debugging
	logrus.Debugf("MAP_TO_RULE: Input data map: %v", data)

	// Create a new rule
	rule := &models.Rule{
		ID:              getString(data, "id"),
		Name:            getString(data, "name"),
		Description:     getString(data, "description"),
		Query:           getString(data, "query"),
		ResolveQuery:    getString(data, "resolve_query"),
		Status:          models.RuleStatus(getString(data, "status")),
		Severity:        models.RuleSeverity(getString(data, "severity")),
		ThrottleMinutes: getInt(data, "throttle_minutes"),
		EntityIDColumns: getString(data, "entity_id_columns"),
		ResultStream:    getString(data, "result_stream"),
		ViewName:        getString(data, "view_name"),
		ResolveViewName: getString(data, "resolve_view_name"),
		LastError:       getString(data, "last_error"),
	}

	// Handle special fields: dedicated_alert_acks_stream (pointer to bool)
	if dedicatedStreamRaw, ok := data["dedicated_alert_acks_stream"]; ok && dedicatedStreamRaw != nil {
		// Debug raw value
		logrus.Debugf("MAP_TO_RULE [%s]: Raw 'dedicated_alert_acks_stream' value: %v (exists: %v, type: %T)",
			rule.ID, dedicatedStreamRaw, ok, dedicatedStreamRaw)

		// Try to convert to bool
		if dedicatedStream, ok := dedicatedStreamRaw.(bool); ok {
			rule.DedicatedAlertAcksStream = &dedicatedStream
		}
	}

	// Handle alert_acks_stream_name
	rule.AlertAcksStreamName = getString(data, "alert_acks_stream_name")

	// Parse time fields
	if createdAt, ok := data["created_at"].(time.Time); ok {
		rule.CreatedAt = createdAt
	}
	if updatedAt, ok := data["updated_at"].(time.Time); ok {
		rule.UpdatedAt = updatedAt
	}
	if lastTriggered, ok := data["last_triggered_at"]; ok && lastTriggered != nil {
		if timeVal, ok := lastTriggered.(time.Time); ok {
			rule.LastTriggeredAt = &timeVal
		}
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

	// Query to get the latest version of the specified rule - removed source_stream
	query := fmt.Sprintf(`
		SELECT id, name, description, query, resolve_query, status, severity, 
			   throttle_minutes, entity_id_columns, created_at, updated_at, last_triggered_at,
			   result_stream, view_name, resolve_view_name, last_error,
			   dedicated_alert_acks_stream, alert_acks_stream_name
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
	sanitizedRuleID := GetFormattedRuleID(ruleID)

	// Determine dedicated stream setting
	dedicatedStream := false // Default to false
	if req.DedicatedAlertAcksStream != nil {
		dedicatedStream = *req.DedicatedAlertAcksStream
	}

	// Create the rule
	rule := &models.Rule{
		ID:                       ruleID,
		Name:                     req.Name,
		Description:              req.Description,
		Query:                    req.Query,
		ResolveQuery:             req.ResolveQuery,
		Status:                   models.RuleStatusCreated,
		Severity:                 req.Severity,
		ThrottleMinutes:          req.ThrottleMinutes,
		EntityIDColumns:          req.EntityIDColumns,
		CreatedAt:                now,
		UpdatedAt:                now,
		ResultStream:             fmt.Sprintf("rule_%s_results", sanitizedRuleID),
		ViewName:                 fmt.Sprintf("rule_%s_view", sanitizedRuleID),
		DedicatedAlertAcksStream: &dedicatedStream,        // Store the determined value
		AlertAcksStreamName:      req.AlertAcksStreamName, // Copy optional name
	}

	// Only set ResolveViewName if ResolveQuery is provided
	if req.ResolveQuery != "" {
		rule.ResolveViewName = fmt.Sprintf("rule_%s_resolve_view", sanitizedRuleID)
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

	// Handle nullable boolean for DedicatedAlertAcksStream
	var dedicatedAlertAcksStream interface{}
	if rule.DedicatedAlertAcksStream != nil {
		dedicatedAlertAcksStream = *rule.DedicatedAlertAcksStream // Use the pointer's value
		logrus.Debugf("PERSIST_RULE: Using provided DedicatedAlertAcksStream value: %v", *rule.DedicatedAlertAcksStream)
	} else {
		// If the pointer is nil, determine value based on context (create vs update)
		if active { // 'active' is true during initial creation persistence
			dedicatedAlertAcksStream = false // Default to false if omitted during creation
			logrus.Debugf("PERSIST_RULE: DedicatedAlertAcksStream is nil, defaulting to false for active rule")
		} else {
			// During updates or soft deletes (active=false), nil means no change/use existing DB value
			dedicatedAlertAcksStream = nil // Represent as NULL for updates if omitted
			logrus.Debugf("PERSIST_RULE: DedicatedAlertAcksStream is nil for inactive rule, using nil value")
		}
	}

	// Explicitly prepare the boolean value for insertion
	var dedicatedStreamValue bool = false // Default to false
	if dedicatedAlertAcksStream != nil {
		// Type assert the interface{} back to bool
		if val, ok := dedicatedAlertAcksStream.(bool); ok {
			dedicatedStreamValue = val
			logrus.Debugf("PERSIST_RULE: Successfully type-asserted DedicatedAlertAcksStream to bool: %v", val)
		} else {
			logrus.Warnf("PERSIST_RULE: Failed to type-assert DedicatedAlertAcksStream to bool, defaulting to false")
		}
	} else {
		logrus.Debugf("PERSIST_RULE: DedicatedAlertAcksStream is nil, using default boolean value: false")
	}

	// Handle nullable string for AlertAcksStreamName
	var alertAcksStreamName interface{}
	if rule.AlertAcksStreamName != "" {
		alertAcksStreamName = rule.AlertAcksStreamName
	} else {
		alertAcksStreamName = nil // Use nil for database NULL
	}

	// Define columns for insertion - removed source_stream
	columns := []string{
		"id", "name", "description", "query", "resolve_query", "status", "severity", "throttle_minutes",
		"entity_id_columns", "created_at", "updated_at", "last_triggered_at",
		"result_stream", "view_name", "resolve_view_name", "last_error",
		"dedicated_alert_acks_stream", "alert_acks_stream_name",
		"active",
	}

	// Prepare values for insertion - removed source_stream value
	values := []interface{}{
		rule.ID,
		rule.Name,
		rule.Description,
		rule.Query,
		rule.ResolveQuery,
		string(rule.Status),
		string(rule.Severity),
		rule.ThrottleMinutes,
		rule.EntityIDColumns,
		rule.CreatedAt,
		rule.UpdatedAt,
		lastTriggeredAt, // Pass directly, InsertIntoStream handles formatting
		rule.ResultStream,
		rule.ViewName,
		rule.ResolveViewName,
		rule.LastError,
		dedicatedStreamValue, // Pass the explicitly typed boolean value
		alertAcksStreamName,  // Pass the interface{} value (string or nil)
		active,
	}

	// Log the values being inserted for debugging
	logrus.Debugf("PERSIST_RULE: Persisting rule %s with values: Status=%v, DedicatedStreamFlag=%v, StreamName=%v, Active=%v",
		rule.ID, string(rule.Status), dedicatedStreamValue, alertAcksStreamName, active)

	// Use the client's InsertIntoStream method
	err := s.tpClient.InsertIntoStream(ctx, s.ruleStream, columns, values)
	if err != nil {
		logrus.Errorf("PERSIST_RULE: Error inserting into stream: %v", err)
		return err
	}

	logrus.Debugf("PERSIST_RULE: Successfully persisted rule %s with DedicatedStreamFlag=%v", rule.ID, dedicatedStreamValue)
	return nil
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
	if req.ResolveQuery != nil {
		rule.ResolveQuery = *req.ResolveQuery
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
	if req.DedicatedAlertAcksStream != nil {
		rule.DedicatedAlertAcksStream = req.DedicatedAlertAcksStream
	}
	if req.AlertAcksStreamName != nil {
		rule.AlertAcksStreamName = *req.AlertAcksStreamName // Dereference pointer
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
	logrus.Debugf("DELETE_RULE: Starting deletion of rule %s", id)

	// First, stop the rule if it's running
	if err := s.StopRule(ctx, id); err != nil {
		logrus.Warnf("Error stopping rule %s before deletion: %v", id, err)
		// Continue with deletion even if stop fails
	}

	// Get the rule
	rule, err := s.GetRule(id)
	if err != nil {
		logrus.Errorf("DELETE_RULE: Failed to get rule %s: %v", id, err)
		return err
	}

	logrus.Debugf("DELETE_RULE: Retrieved rule %s for deletion, status=%s", rule.ID, rule.Status)

	// Cleanup Timeplus resources
	if err := s.tpClient.DeleteMaterializedView(ctx, rule.ViewName); err != nil {
		logrus.Warnf("Error deleting materialized view %s: %v", rule.ViewName, err)
		// Continue with other cleanup operations
	} else {
		logrus.Debugf("DELETE_RULE: Successfully deleted materialized view %s", rule.ViewName)
	}

	// Delete the alert acks view as well
	acksViewName := fmt.Sprintf("rule_%s_acks_view", rule.ID)
	if err := s.tpClient.DeleteMaterializedView(ctx, acksViewName); err != nil {
		logrus.Warnf("Error deleting alert acks view %s: %v", acksViewName, err)
		// Continue with other cleanup operations
	} else {
		logrus.Debugf("DELETE_RULE: Successfully deleted alert acks view %s", acksViewName)
	}

	// Delete the resolve views if they exist
	if rule.ResolveViewName != "" {
		resolveViewName := rule.ResolveViewName
		resolveMVName := fmt.Sprintf("rule_%s_resolve_mv", GetFormattedRuleID(rule.ID))

		// Try to drop the resolve materialized view
		if err := s.tpClient.DeleteMaterializedView(ctx, resolveMVName); err != nil {
			logrus.Warnf("Error deleting resolve materialized view %s: %v", resolveMVName, err)
		} else {
			logrus.Debugf("Successfully deleted resolve materialized view %s", resolveMVName)
		}

		// Try to drop the resolve plain view
		_, err := s.tpClient.ExecuteQuery(ctx, fmt.Sprintf("DROP VIEW IF EXISTS `%s`", resolveViewName))
		if err != nil {
			logrus.Warnf("Error dropping resolve view %s: %v", resolveViewName, err)
		} else {
			logrus.Debugf("Successfully dropped resolve view %s", resolveViewName)
		}
	}

	// Delete dedicated alert acks stream if it exists
	if rule.DedicatedAlertAcksStream != nil && *rule.DedicatedAlertAcksStream {
		dedicatedStreamName := ""
		if rule.AlertAcksStreamName != "" {
			dedicatedStreamName = rule.AlertAcksStreamName
		} else {
			sanitizedRuleID := GetFormattedRuleID(rule.ID)
			dedicatedStreamName = fmt.Sprintf("rule_%s_alert_acks", sanitizedRuleID)
		}

		if dedicatedStreamName != "" {
			logrus.Debugf("DELETE_RULE: Attempting to delete dedicated alert acks stream: %s", dedicatedStreamName)
			if err := s.tpClient.DeleteStream(ctx, dedicatedStreamName); err != nil {
				logrus.Warnf("Error deleting dedicated alert acks stream %s: %v", dedicatedStreamName, err)
				// Continue with other cleanup operations
			} else {
				logrus.Debugf("DELETE_RULE: Successfully deleted dedicated alert acks stream %s", dedicatedStreamName)
			}
		}
	}

	if err := s.tpClient.DeleteStream(ctx, rule.ResultStream); err != nil {
		logrus.Warnf("Error deleting result stream %s: %v", rule.ResultStream, err)
		// Continue with other cleanup operations
	} else {
		logrus.Debugf("DELETE_RULE: Successfully deleted result stream %s", rule.ResultStream)
	}

	// Mark the rule as inactive rather than physically deleting it
	// This is a soft delete approach
	rule.Status = models.RuleStatusStopped
	rule.UpdatedAt = time.Now()

	logrus.Debugf("DELETE_RULE: Marking rule %s as inactive", rule.ID)
	if err := s.persistRule(ctx, rule, false); err != nil {
		logrus.Errorf("DELETE_RULE: Failed to mark rule as deleted: %v", err)
		return fmt.Errorf("failed to mark rule as deleted: %w", err)
	}

	logrus.Infof("DELETE_RULE: Successfully deleted rule %s", rule.ID)
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
	time.Sleep(3 * time.Second) // Wait 3s - Increased delay for consistency

	rule, err := s.GetRule(ruleID)
	if err != nil {
		return err
	}

	logrus.Debugf("START_RULE: Starting rule %s, current state: Status=%s, DedicatedAlertAcksStream=%v",
		rule.ID, rule.Status, rule.DedicatedAlertAcksStream)

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
	resolveViewName := fmt.Sprintf("rule_%s_resolve_view", sanitizedRuleID)
	resolveMaterializedViewName := fmt.Sprintf("rule_%s_resolve_mv", sanitizedRuleID)

	// Determine target alert stream name based on rule config
	targetAlertStreamName := timeplus.AlertAcksMutableStream // Default to global
	useDedicatedStream := false

	if rule.DedicatedAlertAcksStream != nil {
		logrus.Debugf("START_RULE: DedicatedAlertAcksStream pointer value: %v", *rule.DedicatedAlertAcksStream)
	} else {
		logrus.Debugf("START_RULE: DedicatedAlertAcksStream pointer is nil")
	}

	if rule.AlertAcksStreamName != "" { // Explicit name overrides everything
		targetAlertStreamName = rule.AlertAcksStreamName
		useDedicatedStream = true
		logrus.Infof("Using explicitly named alert acks stream: %s", targetAlertStreamName)
	} else if rule.DedicatedAlertAcksStream != nil && *rule.DedicatedAlertAcksStream { // Dedicated flag is true
		targetAlertStreamName = fmt.Sprintf("rule_%s_alert_acks", sanitizedRuleID)
		useDedicatedStream = true
		logrus.Infof("Using generated dedicated alert acks stream: %s", targetAlertStreamName)
	} else {
		logrus.Infof("Using global alert acks stream: %s", targetAlertStreamName)
	}

	logrus.Debugf("START_RULE: Determined useDedicatedStream=%v, targetAlertStreamName=%s",
		useDedicatedStream, targetAlertStreamName)

	// Step 0: Ensure the target mutable alert acks stream exists if it's not the global one
	if useDedicatedStream {
		logrus.Infof("Ensuring dedicated alert acks stream exists: %s", targetAlertStreamName)
		ackSchema := timeplus.GetMutableAlertAcksSchema() // Use correct package qualifier
		primaryKeys := []string{"rule_id", "entity_id"}   // Define the primary key
		if err := s.tpClient.EnsureMutableStream(timeoutCtx, targetAlertStreamName, ackSchema, primaryKeys); err != nil {
			rule.Status = models.RuleStatusFailed
			rule.LastError = fmt.Sprintf("Failed to ensure dedicated mutable alert acks stream %s: %v", targetAlertStreamName, err)
			s.persistRule(timeoutCtx, rule, true)
			return fmt.Errorf("failed to ensure dedicated mutable alert acks stream %s: %w", targetAlertStreamName, err)
		}
		logrus.Infof("Ensured dedicated mutable alert acks stream exists: %s", targetAlertStreamName)
	} // else: Don't need to ensure global stream here, assumed to exist

	// Step 1: Force drop existing views with retries to ensure we're starting clean
	dropViews := []string{plainViewName, materializedViewName}
	// Add resolve views to drop list if a resolveQuery exists
	if rule.ResolveQuery != "" {
		dropViews = append(dropViews, resolveViewName, resolveMaterializedViewName)
	}

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

	// If resolveQuery is specified, create a temporary view for it to validate that it has the entity_id column
	if rule.ResolveQuery != "" {
		// Create the plain resolve view
		resolveViewQuery := fmt.Sprintf("CREATE VIEW %s AS %s", resolveViewName, rule.ResolveQuery)
		logrus.Infof("Creating resolve plain view with query: %s", resolveViewQuery)

		var resolveViewErr error
		for attempt := 1; attempt <= 3; attempt++ {
			resolveViewErr = s.tpClient.ExecuteDDL(timeoutCtx, resolveViewQuery)
			if resolveViewErr == nil {
				break
			}

			if strings.Contains(resolveViewErr.Error(), "already exists") {
				logrus.Warnf("Resolve view already exists, trying to forcefully drop it again")
				dropQuery := fmt.Sprintf("DROP VIEW IF EXISTS %s", resolveViewName)
				s.tpClient.ExecuteDDL(timeoutCtx, dropQuery)
				time.Sleep(2 * time.Second)
			} else {
				logrus.Warnf("Attempt %d to create resolve plain view failed: %v", attempt, resolveViewErr)
			}

			if attempt < 3 {
				time.Sleep(3 * time.Second)
			}
		}

		if resolveViewErr != nil {
			logrus.Errorf("Failed to create resolve plain view: %v", resolveViewErr)
			rule.Status = models.RuleStatusFailed
			rule.LastError = fmt.Sprintf("Failed to create resolve plain view: %v", resolveViewErr)
			s.persistRule(timeoutCtx, rule, true)
			// Clean up the rule view before returning
			s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", plainViewName))
			return fmt.Errorf("failed to create resolve plain view: %w", resolveViewErr)
		}
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
		// Clean up both views if resolveQuery exists
		s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", plainViewName))
		if rule.ResolveQuery != "" {
			s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", resolveViewName))
		}
		return fmt.Errorf("failed to get view columns: %w", err)
	}

	// Find a suitable ID column (prioritize common ID field names)
	idColumnName := ""
	var foundColumns []string
	needsCustomEntityId := false
	entityIdExpression := ""

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
				needsCustomEntityId = true

				// Build the concatenation expression with separators
				var concatParts []string
				for i, col := range foundColumns {
					if i > 0 {
						concatParts = append(concatParts, "'_'") // Add underscore separator
					}
					concatParts = append(concatParts, col)
				}
				entityIdExpression = fmt.Sprintf("concat(%s)", strings.Join(concatParts, ", "))

				// Drop the original view first
				// Use ExecuteDDL
				err = s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", plainViewName))
				if err != nil {
					logrus.Warnf("Error dropping plain view for concatenation: %v", err)
				}

				// Recreate the view with the concatenated entity_id
				modifiedQuery := fmt.Sprintf("CREATE VIEW %s AS SELECT *, %s AS entity_id FROM (%s)",
					plainViewName, entityIdExpression, rule.Query)
				// Use ExecuteDDL
				err = s.tpClient.ExecuteDDL(timeoutCtx, modifiedQuery)
				if err != nil {
					logrus.Errorf("Failed to create modified plain view with concatenation: %v", err)
					rule.Status = models.RuleStatusFailed
					rule.LastError = fmt.Sprintf("Failed to create modified plain view with concatenation: %v", err)
					s.persistRule(timeoutCtx, rule, true)
					// Clean up both views if resolveQuery exists
					if rule.ResolveQuery != "" {
						s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", resolveViewName))
					}
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
		needsCustomEntityId = true
		entityIdExpression = "lower(hex(md5(toString(_tp_time))))"

		// Drop the original view first
		// Use ExecuteDDL
		err = s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", plainViewName))
		if err != nil {
			logrus.Warnf("Error dropping plain view for modification: %v", err)
		}

		// Recreate with a hashed _tp_time field
		modifiedQuery := fmt.Sprintf("CREATE VIEW %s AS SELECT *, %s AS entity_id FROM (%s)",
			plainViewName, entityIdExpression, rule.Query)
		// Use ExecuteDDL
		err = s.tpClient.ExecuteDDL(timeoutCtx, modifiedQuery)
		if err != nil {
			logrus.Errorf("Failed to create modified plain view: %v", err)
			rule.Status = models.RuleStatusFailed
			rule.LastError = fmt.Sprintf("Failed to create modified plain view: %v", err)
			s.persistRule(timeoutCtx, rule, true)
			// Clean up both views if resolveQuery exists
			if rule.ResolveQuery != "" {
				s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", resolveViewName))
			}
			return fmt.Errorf("failed to create modified plain view: %w", err)
		}

		idColumnName = "entity_id"
	}

	logrus.Infof("Using column '%s' as the entity_id for rule %s", idColumnName, rule.ID)

	// Now, if resolveQuery is specified, ensure it has the same entity_id handling
	if rule.ResolveQuery != "" {
		// If we had to create a custom entity_id for the main query, do the same for the resolve query
		if needsCustomEntityId {
			// Drop the original resolve view
			err = s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", resolveViewName))
			if err != nil {
				logrus.Warnf("Error dropping resolve view for modification: %v", err)
			}

			// Recreate with the same entity_id expression
			modifiedResolveQuery := fmt.Sprintf("CREATE VIEW %s AS SELECT *, %s AS entity_id FROM (%s)",
				resolveViewName, entityIdExpression, rule.ResolveQuery)
			err = s.tpClient.ExecuteDDL(timeoutCtx, modifiedResolveQuery)
			if err != nil {
				logrus.Errorf("Failed to create modified resolve view: %v", err)
				rule.Status = models.RuleStatusFailed
				rule.LastError = fmt.Sprintf("Failed to create modified resolve view: %v", err)
				s.persistRule(timeoutCtx, rule, true)
				// Clean up both views
				s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", plainViewName))
				return fmt.Errorf("failed to create modified resolve view: %w", err)
			}

			logrus.Infof("Created entity_id field in resolve view using expression: %s", entityIdExpression)
		}

		// Validate that the entity_id column exists in the resolve view
		resolveColumnsQuery := fmt.Sprintf("DESCRIBE %s", resolveViewName)
		resolveColumnResults, err := s.tpClient.ExecuteQuery(timeoutCtx, resolveColumnsQuery)
		if err != nil {
			logrus.Errorf("Failed to get resolve view columns: %v", err)
			rule.Status = models.RuleStatusFailed
			rule.LastError = fmt.Sprintf("Failed to get resolve view columns: %v", err)
			s.persistRule(timeoutCtx, rule, true)
			// Clean up both views
			s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", plainViewName))
			s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", resolveViewName))
			return fmt.Errorf("failed to get resolve view columns: %w", err)
		}

		// Check if the entity_id column exists in the resolve view
		entityIdExists := false
		for _, column := range resolveColumnResults {
			colName := ""
			if name, ok := column["name"].(string); ok {
				colName = name
			}

			if colName == idColumnName {
				entityIdExists = true
				break
			}
		}

		if !entityIdExists {
			errorMsg := fmt.Sprintf("Entity ID column '%s' not found in resolveQuery results. The resolveQuery must return the same entity_id column as the main query.", idColumnName)
			logrus.Errorf(errorMsg)
			rule.Status = models.RuleStatusFailed
			rule.LastError = errorMsg
			s.persistRule(timeoutCtx, rule, true)
			// Clean up both views
			s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", plainViewName))
			s.tpClient.ExecuteDDL(timeoutCtx, fmt.Sprintf("DROP VIEW IF EXISTS %s", resolveViewName))
			return fmt.Errorf(errorMsg)
		}

		logrus.Infof("Validated that entity_id column '%s' exists in both the rule query and resolveQuery", idColumnName)
	}

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

	// Step 4: Create a materialized view that joins with the target alert acks stream
	materializedViewQuery := timeplus.GetRuleThrottledMaterializedViewQuery(
		rule.ID,
		rule.ThrottleMinutes,
		idColumnName,
		triggeringDataExpr,
		targetAlertStreamName, // Pass the determined target stream name
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

	// Step 5: Update rule status to running
	rule.Status = models.RuleStatusRunning
	rule.LastError = "" // Clear last error on success
	rule.UpdatedAt = time.Now()

	// Explicitly set the pointer value based on the determined logic
	// This ensures the correct value is persisted even if the original pointer was lost/overwritten.
	trueValue := useDedicatedStream // Create a copy to avoid potential issues with the reference
	rule.DedicatedAlertAcksStream = &trueValue

	// Update the AlertAcksStreamName if using a dedicated stream
	if useDedicatedStream && rule.AlertAcksStreamName == "" {
		rule.AlertAcksStreamName = targetAlertStreamName
	}

	// Log the value being persisted
	var dedicatedFlagValue string
	if rule.DedicatedAlertAcksStream == nil {
		dedicatedFlagValue = "nil_pointer"
	} else {
		dedicatedFlagValue = fmt.Sprintf("%t", *rule.DedicatedAlertAcksStream)
	}
	logrus.Debugf("START_RULE: Final persist in StartRule for rule %s. Status: %s, DedicatedFlagPointerValue: %s, AlertAcksStreamName: %s",
		rule.ID, rule.Status, dedicatedFlagValue, rule.AlertAcksStreamName)

	// Persist the final state
	if err := s.persistRule(ctx, rule, true); err != nil {
		logrus.Errorf("START_RULE: Failed to update rule status: %v", err)
		return fmt.Errorf("failed to update rule status: %w", err)
	}

	logrus.Infof("START_RULE: Successfully started rule %s with dedicated stream flag: %v", rule.ID, dedicatedFlagValue)

	// Step 6: Create the resolve query view and materialized view if provided
	if rule.ResolveQuery != "" {
		logrus.Infof("Creating resolve query view for rule %s", rule.ID)

		// Create the plain view with the resolve query
		resolveViewQuery := fmt.Sprintf("CREATE VIEW %s AS %s", resolveViewName, rule.ResolveQuery)

		logrus.Infof("Creating resolve plain view with query: %s", resolveViewQuery)

		var resolveViewErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			resolveViewErr = s.tpClient.ExecuteDDL(timeoutCtx, resolveViewQuery)
			if resolveViewErr == nil {
				break
			}
			logrus.Warnf("Attempt %d to create resolve plain view failed: %v", attempt, resolveViewErr)
			if attempt < maxAttempts {
				time.Sleep(2 * time.Second)
			}
		}

		if resolveViewErr != nil {
			logrus.Errorf("Failed to create resolve plain view: %v", resolveViewErr)
			rule.Status = models.RuleStatusFailed
			rule.LastError = fmt.Sprintf("Failed to create resolve plain view: %v", resolveViewErr)
			s.persistRule(timeoutCtx, rule, true)
			return fmt.Errorf("failed to create resolve plain view: %w", resolveViewErr)
		}

		// Create the materialized view that will auto-acknowledge alerts
		resolveMVQuery := timeplus.GetRuleResolveViewQuery(
			rule.ID,
			idColumnName,
			targetAlertStreamName,
		)

		logrus.Infof("Creating resolve materialized view with query: %s", resolveMVQuery)

		var resolveMVErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			resolveMVErr = s.tpClient.ExecuteDDL(timeoutCtx, resolveMVQuery)
			if resolveMVErr == nil {
				break
			}
			logrus.Warnf("Attempt %d to create resolve materialized view failed: %v", attempt, resolveMVErr)
			if attempt < maxAttempts {
				time.Sleep(2 * time.Second)
			}
		}

		if resolveMVErr != nil {
			logrus.Errorf("Failed to create resolve materialized view: %v", resolveMVErr)
			rule.Status = models.RuleStatusFailed
			rule.LastError = fmt.Sprintf("Failed to create resolve materialized view: %v", resolveMVErr)
			s.persistRule(timeoutCtx, rule, true)
			return fmt.Errorf("failed to create resolve materialized view: %w", resolveMVErr)
		}

		// Store the resolve view name in the rule
		rule.ResolveViewName = resolveViewName
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

	// Delete the resolve views if they exist
	if rule.ResolveViewName != "" {
		resolveViewName := rule.ResolveViewName
		resolveMVName := fmt.Sprintf("rule_%s_resolve_mv", GetFormattedRuleID(rule.ID))

		// Try to drop the resolve materialized view
		if err := s.tpClient.DeleteMaterializedView(ctx, resolveMVName); err != nil {
			logrus.Warnf("Error deleting resolve materialized view %s: %v", resolveMVName, err)
		} else {
			logrus.Debugf("Successfully deleted resolve materialized view %s", resolveMVName)
		}

		// Try to drop the resolve plain view
		_, err := s.tpClient.ExecuteQuery(ctx, fmt.Sprintf("DROP VIEW IF EXISTS `%s`", resolveViewName))
		if err != nil {
			logrus.Warnf("Error dropping resolve view %s: %v", resolveViewName, err)
		} else {
			logrus.Debugf("Successfully dropped resolve view %s", resolveViewName)
		}
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
