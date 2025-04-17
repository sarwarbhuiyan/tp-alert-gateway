package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	tp "github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

func main() {
	fmt.Println("Testing earliest_ts() with Rule View")

	// Use the rule ID we created earlier
	ruleID := "92bc414d-c482-4c10-ba61-222ebeaba7af"
	viewName := fmt.Sprintf("rule_%s_view", ruleID)

	// Connect to Timeplus
	fmt.Println("Connecting to Timeplus...")
	conn, err := proton.Open(&proton.Options{
		Addr: []string{"localhost:8464"},
		Auth: proton.Auth{
			Database: "default",
			Username: "test",
			Password: "test123",
		},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("Failed to connect to Timeplus: %v", err)
	}
	fmt.Println("Connected to Timeplus")

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

	// Create a context for the streaming query with a longer timeout
	streamCtx, streamCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer streamCancel()

	// Set up a wait group to wait for both tests
	var wg sync.WaitGroup
	wg.Add(2)

	// Test 1: Query with table() and earliest_ts
	go func() {
		defer wg.Done()
		fmt.Println("\n=== TEST 1: Using table() with earliest_ts() ===")

		tableQuery := fmt.Sprintf("SELECT * FROM table(`%s`) WHERE _tp_time > earliest_ts() LIMIT 10", viewName)
		fmt.Printf("Query: %s\n", tableQuery)

		rows, err := conn.Query(ctx, tableQuery)
		if err != nil {
			fmt.Printf("Error querying with table(): %v\n", err)
			return
		}
		defer rows.Close()

		// Get column names and types
		columns := rows.Columns()
		columnTypes := rows.ColumnTypes()
		fmt.Printf("Columns: %v\n", columns)

		// Count and process rows
		rowCount := 0
		for rows.Next() {
			rowCount++

			// Create a slice for scanning, matching column types
			scanArgs := make([]interface{}, len(columns))
			for i, ct := range columnTypes {
				scanArgs[i] = reflect.New(ct.ScanType()).Interface()
			}

			if err := rows.Scan(scanArgs...); err != nil {
				fmt.Printf("Error scanning row: %v\n", err)
				continue
			}

			// Print row data
			fmt.Printf("Row %d:\n", rowCount)
			rowData := make(map[string]interface{})
			for i, col := range columns {
				// Dereference the pointer to get the value
				val := reflect.ValueOf(scanArgs[i]).Elem().Interface()
				rowData[col] = val
				fmt.Printf("  %s: %v\n", col, val)
			}
		}

		if err := rows.Err(); err != nil {
			fmt.Printf("Error iterating rows: %v\n", err)
			return
		}

		if rowCount == 0 {
			fmt.Printf("No data found using table() with earliest_ts\n")
		} else {
			fmt.Printf("Found %d rows using table() with earliest_ts\n", rowCount)
		}
	}()

	// Test 2: Use StreamQuery with earliest_ts
	go func() {
		defer wg.Done()
		fmt.Println("\n=== TEST 2: Using StreamQuery with earliest_ts() ===")

		streamQuery := fmt.Sprintf("SELECT * FROM `%s` WHERE _tp_time > earliest_ts()", viewName)
		fmt.Printf("StreamQuery: %s\n", streamQuery)

		// Track how many rows we receive
		rowCount := 0
		mu := sync.Mutex{} // For safe updates to rowCount from callback

		// Start time for measuring duration
		startTime := time.Now()

		err := tpClient.StreamQuery(streamCtx, streamQuery, func(row interface{}) {
			mu.Lock()
			rowCount++
			mu.Unlock()

			fmt.Printf("StreamQuery callback fired for row #%d at %v (elapsed: %v)\n",
				rowCount, time.Now().Format(time.RFC3339Nano), time.Since(startTime))

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
	}()

	// Wait for both tests to complete
	wg.Wait()
	fmt.Println("\nAll tests completed")
}
