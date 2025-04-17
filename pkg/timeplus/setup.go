package timeplus

import (
	"context"
)

// CreateAlertStream creates the alert stream in Timeplus
func (c *Client) CreateAlertStream(ctx context.Context) error {
	schema := GetAlertSchema()
	return c.CreateStream(ctx, AlertsStream, schema)
}

// SetupStreams creates all required streams in Timeplus
func (c *Client) SetupStreams(ctx context.Context) error {
	// Create alert stream
	if err := c.CreateAlertStream(ctx); err != nil {
		return err
	}

	// Create alert acks stream
	if err := c.SetupMutableAlertAcksStream(ctx); err != nil {
		return err
	}

	return nil
}
