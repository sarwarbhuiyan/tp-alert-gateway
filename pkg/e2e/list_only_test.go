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

// TestListExistingResources just lists existing streams and tables without creating anything
func TestListExistingResources(t *testing.T) {
	// Configure logging
	logrus.SetLevel(logrus.InfoLevel)
	logrus.Info("Starting list-only test")

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

	// Test 1: List streams
	logrus.Info("Test 1: Listing existing streams")
	streams, err := client.ListStreams(ctx)
	require.NoError(t, err, "Failed to list streams")
	logrus.Infof("Found %d streams:", len(streams))
	for i, stream := range streams {
		logrus.Infof("  %d: %s", i+1, stream)
	}

	// Test 2: List tables
	logrus.Info("Test 2: Listing all tables")
	result, err := client.ExecuteQuery(ctx, "SELECT name, engine FROM system.tables WHERE database = 'default'")
	require.NoError(t, err, "Failed to list tables")
	logrus.Infof("Found %d tables:", len(result))
	for i, row := range result {
		name := row["name"]
		engine := row["engine"]
		logrus.Infof("  %d: %s (%s)", i+1, name, engine)
	}

	// Test 3: List materialized views
	logrus.Info("Test 3: Listing materialized views")
	matViewResult, err := client.ExecuteQuery(ctx, "SELECT name FROM system.tables WHERE engine = 'MaterializedView'")
	require.NoError(t, err, "Failed to list materialized views")
	logrus.Infof("Found %d materialized views:", len(matViewResult))
	for i, row := range matViewResult {
		logrus.Infof("  %d: %s", i+1, row["name"])
	}

	// Test 4: Get system info
	logrus.Info("Test 4: Getting system info")
	infoResult, err := client.ExecuteQuery(ctx, "SELECT * FROM system.one")
	require.NoError(t, err, "Failed to get system info")
	logrus.Infof("System info: %v", infoResult)

	logrus.Info("List-only test completed successfully")
}
