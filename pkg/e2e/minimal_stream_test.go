package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

// TestMinimalStreamOperations focuses just on creating a stream, inserting data and reading it back
func TestMinimalStreamOperations(t *testing.T) {
	// Configure logging
	logrus.SetLevel(logrus.InfoLevel)
	logrus.Info("Starting minimal stream test")

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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Generate a unique stream name
	uniqueID := "t" + uuid.New().String()[:8]
	streamName := fmt.Sprintf("min_stream_%s", uniqueID)
	logrus.Infof("Using stream name: %s", streamName)

	// Cleanup after the test
	defer func() {
		// Don't use the canceled context for cleanup
		cleanupCtx := context.Background()
		logrus.Infof("Cleaning up stream %s", streamName)
		_, err := client.ExecuteQuery(cleanupCtx, fmt.Sprintf("DROP STREAM IF EXISTS `%s`", streamName))
		if err != nil {
			logrus.Warnf("Failed to drop stream: %v", err)
		}
	}()

	// Step 1: Create stream
	logrus.Info("Step 1: Creating stream")
	createQuery := fmt.Sprintf(
		"CREATE STREAM IF NOT EXISTS `%s` (name string, count int32, created_at datetime64(3))",
		streamName,
	)
	_, err = client.ExecuteQuery(ctx, createQuery)
	require.NoError(t, err, "Failed to create stream")
	logrus.Info("Stream created successfully")

	// Step 2: Insert data
	logrus.Info("Step 2: Inserting data")
	insertQuery := fmt.Sprintf(
		"INSERT INTO `%s` (name, count, created_at) VALUES ('test-item', 42, '%s')",
		streamName,
		time.Now().Format("2006-01-02 15:04:05.000"),
	)
	_, err = client.ExecuteQuery(ctx, insertQuery)
	require.NoError(t, err, "Failed to insert data")
	logrus.Info("Data inserted successfully")

	// Wait a bit for data propagation
	time.Sleep(1 * time.Second)

	// Step 3: Query data
	logrus.Info("Step 3: Querying data")
	selectQuery := fmt.Sprintf("SELECT * FROM table(`%s`) LIMIT 10", streamName)
	result, err := client.ExecuteQuery(ctx, selectQuery)
	require.NoError(t, err, "Failed to query data")

	// Print the results
	logrus.Infof("Query returned %d rows", len(result))
	for i, row := range result {
		logrus.Infof("Row %d: %v", i, row)
	}

	// Verify results exist
	require.NotEmpty(t, result, "Query should return data")

	logrus.Info("Minimal stream test completed successfully")
}
