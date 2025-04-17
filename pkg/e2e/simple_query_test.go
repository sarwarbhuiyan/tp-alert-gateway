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

// TestSimpleTimeplusQuery tests if we can execute a single simple query against Timeplus
func TestSimpleTimeplusQuery(t *testing.T) {
	// Configure logging
	logrus.SetLevel(logrus.InfoLevel)
	logrus.Info("Starting simple Timeplus query test")

	// Setup test configuration
	tpConfig := &config.TimeplusConfig{
		Address:   "localhost:8464",
		Username:  "test",
		Password:  "test123",
		Workspace: "default",
	}

	// Create a client
	client, err := timeplus.NewClient(tpConfig)
	require.NoError(t, err, "Failed to create Timeplus client")

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try a simple query
	logrus.Info("Executing simple SELECT 1 query")
	result, err := client.ExecuteQuery(ctx, "SELECT 1")
	require.NoError(t, err, "Failed to execute simple query")
	require.NotNil(t, result, "Result should not be nil")
	logrus.Infof("Query result: %v", result)

	// Try listing streams
	logrus.Info("Listing streams")
	streams, err := client.ListStreams(ctx)
	require.NoError(t, err, "Failed to list streams")
	logrus.Infof("Found %d streams", len(streams))
	for i, stream := range streams {
		if i < 5 { // Only print first 5 to avoid excessive output
			logrus.Infof("Stream: %s", stream)
		}
	}

	logrus.Info("Simple Timeplus query test completed successfully")
}
