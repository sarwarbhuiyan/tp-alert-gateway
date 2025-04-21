package timeplus

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
)

// TestNewClient tests the client initialization with various configurations
func TestNewClient(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *config.TimeplusConfig
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: &config.TimeplusConfig{
				Address:   "localhost:8464",
				Workspace: "default",
				Username:  "default",
				Password:  "default",
			},
			wantErr: false,
		},
		{
			name: "invalid address",
			cfg: &config.TimeplusConfig{
				Address:   "invalid:port",
				Workspace: "default",
				Username:  "default",
				Password:  "default",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.cfg)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, client)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
				if client != nil {
					defer client.conn.Close()
				}
			}
		})
	}
}

// TestCreateStream tests stream creation functionality
func TestCreateStream(t *testing.T) {
	// Skip if no Timeplus instance is available
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.TimeplusConfig{
		Address:   "localhost:8464",
		Workspace: "default",
		Username:  "default",
		Password:  "default",
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer client.conn.Close()

	ctx := context.Background()

	tests := []struct {
		name    string
		stream  string
		schema  []Column
		wantErr bool
	}{
		{
			name:   "valid stream",
			stream: "test_stream",
			schema: []Column{
				{Name: "id", Type: "string"},
				{Name: "value", Type: "int32"},
			},
			wantErr: false,
		},
		{
			name:   "invalid schema",
			stream: "invalid_stream",
			schema: []Column{
				{Name: "id", Type: "invalid_type"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.CreateStream(ctx, tt.stream, tt.schema)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				// Cleanup
				client.DeleteStream(ctx, tt.stream)
			}
		})
	}
}

// TestEnsureMutableStream tests mutable stream creation and management
func TestEnsureMutableStream(t *testing.T) {
	// Skip if no Timeplus instance is available
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.TimeplusConfig{
		Address:   "localhost:8464",
		Workspace: "default",
		Username:  "default",
		Password:  "default",
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer client.conn.Close()

	ctx := context.Background()

	tests := []struct {
		name       string
		stream     string
		schema     []Column
		primaryKey []string
		wantErr    bool
	}{
		{
			name:   "valid mutable stream",
			stream: "test_mutable_stream",
			schema: []Column{
				{Name: "id", Type: "string"},
				{Name: "value", Type: "int32"},
			},
			primaryKey: []string{"id"},
			wantErr:    false,
		},
		{
			name:   "no primary key",
			stream: "invalid_mutable_stream",
			schema: []Column{
				{Name: "id", Type: "string"},
				{Name: "value", Type: "int32"},
			},
			primaryKey: []string{},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.EnsureMutableStream(ctx, tt.stream, tt.schema, tt.primaryKey)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				// Cleanup
				client.DeleteStream(ctx, tt.stream)
			}
		})
	}
}

// TestMaterializedView tests materialized view creation and management
func TestMaterializedView(t *testing.T) {
	// Skip if no Timeplus instance is available
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.TimeplusConfig{
		Address:   "localhost:8464",
		Workspace: "default",
		Username:  "default",
		Password:  "default",
	}

	client, err := NewClient(cfg)
	require.NoError(t, err)
	defer client.conn.Close()

	ctx := context.Background()

	// Setup: Create a source stream
	sourceStream := "test_source_stream"
	err = client.CreateStream(ctx, sourceStream, []Column{
		{Name: "id", Type: "string"},
		{Name: "value", Type: "int32"},
	})
	require.NoError(t, err)
	defer client.DeleteStream(ctx, sourceStream)

	tests := []struct {
		name      string
		viewName  string
		query     string
		wantErr   bool
		errorType string
	}{
		{
			name:     "valid materialized view",
			viewName: "test_mv",
			query:    fmt.Sprintf("SELECT * FROM `%s`", sourceStream),
			wantErr:  false,
		},
		{
			name:      "invalid query",
			viewName:  "invalid_mv",
			query:     "SELECT * FROM non_existent_stream",
			wantErr:   true,
			errorType: "stream not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.CreateMaterializedView(ctx, tt.viewName, tt.query)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errorType != "" {
					assert.Contains(t, err.Error(), tt.errorType)
				}
			} else {
				assert.NoError(t, err)
				// Cleanup
				client.DeleteMaterializedView(ctx, tt.viewName)
			}
		})
	}
}
