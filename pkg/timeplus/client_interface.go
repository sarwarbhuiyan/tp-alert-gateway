package timeplus

import (
	"context"
)

// TimeplusClient defines the interface for a Timeplus client
// This allows us to mock the client for testing
type TimeplusClient interface {
	StreamExists(ctx context.Context, name string) (bool, error)
	CreateStream(ctx context.Context, name string, schema []Column) error
	CreateMaterializedView(ctx context.Context, name string, query string) error
	DeleteMaterializedView(ctx context.Context, name string) error
	ViewExists(ctx context.Context, name string) (bool, error)
	DeleteStream(ctx context.Context, name string) error
	ExecuteQuery(ctx context.Context, query string) ([]map[string]interface{}, error)
	StreamQuery(ctx context.Context, query string, callback func(row interface{})) error
	InsertIntoStream(ctx context.Context, streamName string, columns []string, values []interface{}) error

	// New methods
	ListStreams(ctx context.Context) ([]string, error)
	ListViews(ctx context.Context) ([]string, error)
	ListMaterializedViews(ctx context.Context) ([]string, error)
	ExecuteStreamingQuery(ctx context.Context, query string, callback func(result map[string]interface{}) error) error
	SetupAlertAcksStream(ctx context.Context) error
	SetupMutableAlertAcksStream(ctx context.Context) error
	CreateAlertAck(ctx context.Context, alertAck AlertAck) error
	GetAlertAck(ctx context.Context, alertID string) (*AlertAck, error)
	IsAlertAcknowledged(ctx context.Context, alertID string) (bool, error)
	CreateRuleResultsStream(ctx context.Context, ruleID string) error
	ExecuteDDL(ctx context.Context, query string) error
}

// Ensure Client implements TimeplusClient
var _ TimeplusClient = (*Client)(nil)
