package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	tp "github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

func main() {
	fmt.Println("Testing StreamQuery with earliest_ts()")

	// Use the rule ID we created earlier
	ruleID := "92bc414d-c482-4c10-ba61-222ebeaba7af"
	viewName := fmt.Sprintf("rule_%s_view", ruleID)

	// Create a Timeplus client for StreamQuery
	tpConfig := &config.TimeplusConfig{
		Address:   "localhost:8464",
		Workspace: "default",
		Username:  "test",
		Password:  "test123",
	}

	tpClient, err := tp.NewClient(tpConfig)
	if err != nil {
		log.Fatalf("Failed to create Timeplus client: %v", err)
	}
	fmt.Println("Created Timeplus client")

	// Create a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// First check if the view exists and has data
	fmt.Printf("Checking if view %s exists and has data...\n", viewName)
	exists, err := tpClient.ViewExists(ctx, viewName)
	if err != nil {
		fmt.Printf("Error checking if view exists: %v\n", err)
	} else {
		fmt.Printf("View %s exists: %v\n", viewName, exists)
	}

	// Try direct query first
	fmt.Println("Querying view for data...")
	results, err := tpClient.ExecuteQuery(ctx, fmt.Sprintf("SELECT * FROM table(`%s`) LIMIT 5", viewName))
	if err != nil {
		fmt.Printf("Error querying view: %v\n", err)
	} else {
		fmt.Printf("Found %d rows in view\n", len(results))
		for i, row := range results {
			fmt.Printf("Row %d: %+v\n", i+1, row)
		}
	}

	// Use StreamQuery with earliest_ts
	fmt.Println("\nStarting StreamQuery with earliest_ts()...")
	streamQuery := fmt.Sprintf("SELECT * FROM `%s` WHERE _tp_time > earliest_ts()", viewName)
	fmt.Printf("Query: %s\n", streamQuery)

	// Start time for measuring duration
	startTime := time.Now()

	// Count how many rows we get
	rowCount := 0

	// StreamQuery will block until context is canceled or an error occurs
	err = tpClient.StreamQuery(ctx, streamQuery, func(row interface{}) {
		rowCount++

		fmt.Printf("StreamQuery callback fired for row #%d at %v (elapsed: %v)\n",
			rowCount, time.Now().Format(time.RFC3339), time.Since(startTime))

		// Print the row data
		if rowData, ok := row.(map[string]interface{}); ok {
			fmt.Printf("Row data: %+v\n", rowData)
		} else {
			fmt.Printf("Row is not a map: %T %+v\n", row, row)
		}
	})

	if err != nil {
		fmt.Printf("StreamQuery error: %v\n", err)
	} else {
		fmt.Printf("StreamQuery completed without error (processed %d rows)\n", rowCount)
	}

	fmt.Println("Test completed")
}
