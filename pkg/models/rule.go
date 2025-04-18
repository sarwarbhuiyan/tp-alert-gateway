package models

import (
	"time"
)

// RuleStatus represents the current status of a rule
type RuleStatus string

const (
	RuleStatusCreated  RuleStatus = "created"
	RuleStatusStarting RuleStatus = "starting"
	RuleStatusRunning  RuleStatus = "running"
	RuleStatusStopping RuleStatus = "stopping"
	RuleStatusStopped  RuleStatus = "stopped"
	RuleStatusFailed   RuleStatus = "failed"
)

// RuleSeverity represents the severity level of a rule
type RuleSeverity string

const (
	RuleSeverityInfo     RuleSeverity = "info"
	RuleSeverityWarning  RuleSeverity = "warning"
	RuleSeverityCritical RuleSeverity = "critical"
)

// Rule represents an alert rule definition
type Rule struct {
	ID              string       `json:"id"`
	Name            string       `json:"name"`
	Description     string       `json:"description"`
	Query           string       `json:"query"`
	Status          RuleStatus   `json:"status"`
	Severity        RuleSeverity `json:"severity"`
	ThrottleMinutes int          `json:"throttleMinutes"` // 0 means no throttling
	EntityIDColumns string       `json:"entityIdColumns"` // Comma-separated list of columns to use as entity_id
	CreatedAt       time.Time    `json:"createdAt"`
	UpdatedAt       time.Time    `json:"updatedAt"`
	LastTriggeredAt *time.Time   `json:"lastTriggeredAt,omitempty"`

	// Configuration for Alert Acks Stream
	DedicatedAlertAcksStream *bool  `json:"dedicatedAlertAcksStream,omitempty"` // Use rule-specific stream if true
	AlertAcksStreamName      string `json:"alertAcksStreamName,omitempty"`      // Explicit stream name (overrides dedicated flag)

	// Timeplus resource references
	SourceStream string `json:"sourceStream,omitempty"`
	ResultStream string `json:"resultStream,omitempty"`
	ViewName     string `json:"viewName,omitempty"`

	// Error information if status is failed
	LastError string `json:"lastError,omitempty"`
}

// Alert represents a triggered alert instance
type Alert struct {
	ID             string       `json:"id"`
	RuleID         string       `json:"ruleId"`
	RuleName       string       `json:"ruleName"`
	Severity       RuleSeverity `json:"severity"`
	TriggeredAt    time.Time    `json:"triggeredAt"`
	Data           string       `json:"data"` // JSON string representation of the data that triggered the alert
	Acknowledged   bool         `json:"acknowledged"`
	AcknowledgedAt *time.Time   `json:"acknowledgedAt,omitempty"`
	AcknowledgedBy string       `json:"acknowledgedBy,omitempty"`
}

// CreateRuleRequest represents the request payload for creating a rule
type CreateRuleRequest struct {
	Name                     string       `json:"name"`
	Description              string       `json:"description"`
	Query                    string       `json:"query"`
	Severity                 RuleSeverity `json:"severity"`
	ThrottleMinutes          int          `json:"throttleMinutes"`
	EntityIDColumns          string       `json:"entityIdColumns"` // Comma-separated list of columns to use as entity_id
	SourceStream             string       `json:"sourceStream,omitempty"`
	DedicatedAlertAcksStream *bool        `json:"dedicatedAlertAcksStream,omitempty"` // Optional
	AlertAcksStreamName      string       `json:"alertAcksStreamName,omitempty"`      // Optional
}

// UpdateRuleRequest represents the request payload for updating a rule
type UpdateRuleRequest struct {
	Name                     *string       `json:"name,omitempty"`
	Description              *string       `json:"description,omitempty"`
	Query                    *string       `json:"query,omitempty"`
	Severity                 *RuleSeverity `json:"severity,omitempty"`
	ThrottleMinutes          *int          `json:"throttleMinutes,omitempty"`
	EntityIDColumns          *string       `json:"entityIdColumns,omitempty"`          // Comma-separated list of columns to use as entity_id
	DedicatedAlertAcksStream *bool         `json:"dedicatedAlertAcksStream,omitempty"` // Optional
	AlertAcksStreamName      *string       `json:"alertAcksStreamName,omitempty"`      // Optional
}

// AcknowledgeAlertRequest represents the request payload for acknowledging an alert
type AcknowledgeAlertRequest struct {
	AcknowledgedBy string `json:"acknowledgedBy"`
}
