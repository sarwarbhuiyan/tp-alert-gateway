package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

func TestTimeplusConnectionDiagnostics(t *testing.T) {
	// Skip by default, only run when specifically troubleshooting connection issues
	t.Skip("Skipping Timeplus connection test - run explicitly when troubleshooting")

	// Setup test configuration
	tpConfig := &config.TimeplusConfig{
		Address:   "localhost:8464",
		Username:  "test",
		Password:  "test123",
		Workspace: "default",
	}

	// Set up logging
	logrus.SetLevel(logrus.InfoLevel)
	logrus.Info("Starting Timeplus connection diagnostics")

	// Create the client
	client, err := timeplus.NewClient(tpConfig)
	require.NoError(t, err, "Failed to create Timeplus client")

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Run the connection tests
	err = TestTimeplusConnection(ctx, client)
	require.NoError(t, err, "Timeplus connection tests failed")

	// Try a more complex query to test query execution
	logrus.Info("Testing complex query")
	_, err = client.ExecuteQuery(ctx, "SELECT name, engine FROM system.tables LIMIT 10")
	require.NoError(t, err, "Failed to execute complex query")

	logrus.Info("Connection diagnostics completed successfully")
}
