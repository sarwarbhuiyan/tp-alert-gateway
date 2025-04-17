package services

import (
	"strings"
)

// GetFormattedRuleID sanitizes a rule ID by replacing hyphens with underscores
// This is used to create valid names for views and streams in Timeplus
func GetFormattedRuleID(ruleID string) string {
	return strings.ReplaceAll(ruleID, "-", "_")
}
