package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

// TestSQLOnlyOperations uses only direct SQL queries for all operations
func TestSQLOnlyOperations(t *testing.T) {
	log.SetLevel(log.InfoLevel)
	log.Info("Starting SQL-only test")

	// Configuration
	tpConfig := &config.TimeplusConfig{
		Address:   "localhost:8464",
		Username:  "test",
		Password:  "test123",
		Workspace: "default",
	}

	// Connect with retry logic
	var client *timeplus.Client
	var err error

	log.Infof("Connecting to Timeplus at %s (workspace: %s)", tpConfig.Address, tpConfig.Workspace)

	// Retry connection up to 3 times
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		client, err = timeplus.NewClient(tpConfig)
		if err == nil {
			log.Info("Alert Gateway: Successfully connected to Timeplus.")
			break
		}
		log.Warnf("Failed to connect to Timeplus (attempt %d/%d): %v", i+1, maxRetries, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		t.Fatalf("Failed to connect to Timeplus after %d attempts: %v", maxRetries, err)
	}

	// Generate a unique stream name
	streamName := fmt.Sprintf("sql_stream_%s", uuid.New().String()[0:8])
	log.Infof("Using stream name: %s", streamName)

	// Clean up function
	defer func() {
		log.Infof("Cleaning up: Dropping stream %s", streamName)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		query := fmt.Sprintf("DROP STREAM IF EXISTS %s", streamName)
		log.Infof("Executing: %s", query)

		_, err := executeWithRetry(client, ctx, query, 3)
		if err != nil {
			log.Warnf("Failed to drop stream: %v", err)
		} else {
			log.Info("Stream dropped successfully")
		}
	}()

	// Step 1: Create a stream
	log.Info("Step 1: Creating stream using SQL")
	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel1()

	createStreamQuery := fmt.Sprintf("CREATE STREAM IF NOT EXISTS %s (name string, value float64, ts datetime64(3))", streamName)
	log.Infof("Executing: %s", createStreamQuery)

	_, err = executeWithRetry(client, ctx1, createStreamQuery, 3)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	log.Info("Stream created successfully")

	// Step 2: Verify the stream exists
	log.Info("Step 2: Verifying stream exists")
	time.Sleep(1 * time.Second) // Small pause to ensure stream is created

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	checkStreamQuery := "SELECT name FROM system.streams"
	log.Infof("Executing: %s", checkStreamQuery)

	result, err := executeWithRetry(client, ctx2, checkStreamQuery, 3)
	if err != nil {
		t.Fatalf("Failed to list streams: %v", err)
	}

	streamFound := false
	for _, row := range result {
		if name, ok := row["name"].(string); ok && name == streamName {
			streamFound = true
			break
		}
	}

	if !streamFound {
		t.Fatalf("Stream %s not found in system.streams", streamName)
	}
	log.Infof("Stream %s found in system.streams", streamName)

	// Step 3: Insert data into the stream
	log.Info("Step 3: Inserting data into stream")
	ctx3, cancel3 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel3()

	insertQuery := fmt.Sprintf("INSERT INTO %s (name, value, ts) VALUES ('test1', 1.23, now64(3)), ('test2', 4.56, now64(3))", streamName)
	log.Infof("Executing: %s", insertQuery)

	_, err = executeWithRetry(client, ctx3, insertQuery, 3)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	log.Info("Data inserted successfully")

	// Step 4: Query data from the stream
	log.Info("Step 4: Querying data from stream")
	// Small pause to ensure data is available for querying
	time.Sleep(2 * time.Second)

	ctx4, cancel4 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel4()

	selectQuery := fmt.Sprintf("SELECT * FROM %s LIMIT 10", streamName)
	log.Infof("Executing: %s", selectQuery)

	result, err = executeWithRetry(client, ctx4, selectQuery, 3)
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}

	if len(result) == 0 {
		t.Fatalf("No data found in stream %s", streamName)
	}
	log.Infof("Found %d rows in stream", len(result))
	for i, row := range result {
		log.Infof("Row %d: %v", i+1, row)
	}

	// Step 5: Create a simple view
	log.Info("Step 5: Creating a view")
	viewName := fmt.Sprintf("%s_view", streamName)

	ctx5, cancel5 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel5()

	createViewQuery := fmt.Sprintf("CREATE VIEW IF NOT EXISTS %s AS SELECT name, avg(value) as avg_value FROM %s GROUP BY name",
		viewName, streamName)
	log.Infof("Executing: %s", createViewQuery)

	_, err = executeWithRetry(client, ctx5, createViewQuery, 3)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}
	log.Info("View created successfully")

	// Clean up view in defer
	defer func() {
		log.Infof("Cleaning up: Dropping view %s", viewName)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		query := fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName)
		log.Infof("Executing: %s", query)

		_, err := executeWithRetry(client, ctx, query, 3)
		if err != nil {
			log.Warnf("Failed to drop view: %v", err)
		} else {
			log.Info("View dropped successfully")
		}
	}()

	// Success!
	log.Info("All SQL operations completed successfully")
}

// Helper function to execute a query with retries
func executeWithRetry(client *timeplus.Client, ctx context.Context, query string, maxRetries int) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	var err error

	for i := 0; i < maxRetries; i++ {
		result, err = client.ExecuteQuery(ctx, query)
		if err == nil {
			return result, nil
		}

		if i < maxRetries-1 {
			log.Warnf("Query failed (attempt %d/%d): %v - retrying...", i+1, maxRetries, err)
			time.Sleep(time.Duration(i+1) * time.Second)
		}
	}

	return nil, fmt.Errorf("query failed after %d attempts: %w", maxRetries, err)
}
