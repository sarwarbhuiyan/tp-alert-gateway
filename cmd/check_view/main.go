package main

import (
	"context"
	"fmt"
	"log"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
)

func main() {
	fmt.Println("Checking rule results stream with table()")

	// Rule ID of our latest rule
	ruleID := "af98d54c-b23b-4898-8105-2dd2ca72ce06"
	resultStream := fmt.Sprintf("rule_%s_results", ruleID)

	// Connect to Timeplus using the correct connection method
	fmt.Println("Connecting to Timeplus...")

	// Create connection using Open method - NOT OpenDB
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
		log.Fatalf("Failed to connect to Timeplus: %v", err)
	}
	defer conn.Close()

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping Timeplus: %v", err)
	}
	fmt.Println("Connected successfully")

	// Query the results stream
	fmt.Printf("Querying result stream: %s\n", resultStream)

	// Use table() syntax as required by Timeplus
	query := fmt.Sprintf("SELECT * FROM table(`%s`) ORDER BY triggered_at DESC LIMIT 10", resultStream)
	fmt.Printf("Query: %s\n", query)

	rows, err := conn.Query(ctx, query)
	if err != nil {
		log.Fatalf("Error executing query: %v", err)
	}
	defer rows.Close()

	// Get columns
	columns := rows.Columns()
	fmt.Printf("Result columns: %v\n", columns)

	// Print rows
	rowCount := 0
	for rows.Next() {
		rowCount++
		// Create a slice of interface{} to hold all column values
		values := make([]interface{}, len(columns))
		valuePointers := make([]interface{}, len(columns))

		// Initialize pointers to values
		for i := range values {
			valuePointers[i] = &values[i]
		}

		// Scan the row into value pointers
		if err := rows.Scan(valuePointers...); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}

		// Print row data
		fmt.Printf("Row %d:\n", rowCount)
		for i, col := range columns {
			fmt.Printf("  %s: %v\n", col, values[i])
		}
		fmt.Println()
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating rows: %v", err)
	}

	if rowCount == 0 {
		fmt.Println("No results found in stream")
	} else {
		fmt.Printf("Found %d results\n", rowCount)
	}
}
