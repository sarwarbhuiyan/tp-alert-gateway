package models

import (
	"time"
)

// AlertLegacy represents an alert triggered by a rule (legacy version)
type AlertLegacy struct {
	ID             string     `json:"id"`
	RuleID         string     `json:"ruleId"`
	RuleName       string     `json:"ruleName"`
	Severity       string     `json:"severity"`
	TriggeredAt    time.Time  `json:"triggeredAt"`
	Data           string     `json:"data"` // JSON string of the event data
	Acknowledged   bool       `json:"acknowledged"`
	AcknowledgedAt *time.Time `json:"acknowledgedAt,omitempty"`
	AcknowledgedBy string     `json:"acknowledgedBy,omitempty"`
}
