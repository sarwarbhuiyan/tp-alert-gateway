package services

import (
	"fmt"
	"time"
)

// getTime extracts a time.Time value from a map using the given key
func getTime(data map[string]interface{}, key string) time.Time {
	if val, ok := data[key]; ok && val != nil {
		if t, err := parseTimeplus(val); err == nil {
			return t
		}
	}
	return time.Time{}
}

// parseTimeplus parses a Timeplus datetime value into a time.Time
func parseTimeplus(val interface{}) (time.Time, error) {
	switch v := val.(type) {
	case time.Time:
		return v, nil
	case string:
		// Try to parse various time formats
		layouts := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02T15:04:05.999999999",
		}

		for _, layout := range layouts {
			if t, err := time.Parse(layout, v); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("unable to parse time string: %s", v)
	default:
		return time.Time{}, fmt.Errorf("unsupported time type: %T", val)
	}
}
