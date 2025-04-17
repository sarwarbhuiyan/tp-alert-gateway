package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
)

func main() {
	fmt.Println("View Query Checker - Using table() to query materialized view directly")

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

	// Test the connection with retries
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Retry a few times
	for i := 0; i < 5; i++ {
		err := conn.Ping(ctx)
		if err == nil {
			break
		}
		fmt.Printf("Failed to connect (attempt %d/5): %v\n", i+1, err)
		time.Sleep(2 * time.Second)
	}

	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("Failed to connect to Timeplus: %v", err)
	}
	fmt.Println("Connected successfully")

	// Use table() function to query the view directly
	fmt.Printf("\nQuerying materialized view using table(%s)...\n", viewName)
	tableQuery := fmt.Sprintf("SELECT * FROM table(`%s`) LIMIT 10", viewName)
	fmt.Printf("Query: %s\n", tableQuery)

	queryCtx, queryCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer queryCancel()

	rows, err := conn.Query(queryCtx, tableQuery)
	if err != nil {
		fmt.Printf("Error querying view with table(): %v\n", err)
		return
	}
	defer rows.Close()

	// Get column names and types from rows
	columns := rows.Columns()
	columnTypes := rows.ColumnTypes()
	fmt.Printf("Columns: %v\n", columns)

	// Count and display rows
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
		for i, col := range columns {
			// Dereference the pointer received from Scan
			val := reflect.ValueOf(scanArgs[i]).Elem().Interface()
			fmt.Printf("  %s: %v\n", col, val)
		}
	}

	if err := rows.Err(); err != nil {
		fmt.Printf("Error iterating rows: %v\n", err)
		return
	}

	if rowCount == 0 {
		fmt.Printf("No data found in table(%s)\n", viewName)
	} else {
		fmt.Printf("Found %d rows in table(%s)\n", rowCount, viewName)
	}

	// Also check for high temperature data directly in device_temperatures
	fmt.Printf("\nVerifying high temperature data exists in source stream...\n")
	tempQuery := "SELECT * FROM device_temperatures WHERE temperature > 38 ORDER BY timestamp DESC LIMIT 5"
	tempRows, err := conn.Query(queryCtx, tempQuery)
	if err != nil {
		fmt.Printf("Error checking source data: %v\n", err)
		return
	}
	defer tempRows.Close()

	// Count temperature rows
	tempCount := 0
	for tempRows.Next() {
		tempCount++
	}

	if tempCount == 0 {
		fmt.Printf("No high temperature data found in source stream\n")
	} else {
		fmt.Printf("Found %d high temperature readings in source stream\n", tempCount)
	}
}
