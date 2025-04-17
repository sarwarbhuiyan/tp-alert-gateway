package main

import (
	"context"
	"fmt"
	"log"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/lib/driver"
)

func main() {
	fmt.Println("Rule View Query Utility")

	// Rule ID to query
	ruleID := "d00a5121-d7d9-49d0-8c01-2eeabfb46b8a"
	viewName := fmt.Sprintf("rule_%s_view", ruleID)
	resultsName := fmt.Sprintf("rule_%s_results", ruleID)

	// Connect to Timeplus using the correct connection method
	fmt.Println("\nConnecting to Timeplus...")

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

	// 1. Query the materialized view
	fmt.Printf("\nQuerying materialized view: %s\n", viewName)
	queryView := fmt.Sprintf("SELECT * FROM table(`%s`) LIMIT 10", viewName)
	fmt.Printf("Query: %s\n", queryView)

	viewRows, err := conn.Query(ctx, queryView)
	if err != nil {
		fmt.Printf("Error querying view: %v\n", err)
	} else {
		defer viewRows.Close()
		printRows(viewRows)
	}

	// 2. Query the results stream
	fmt.Printf("\nQuerying results stream: %s\n", resultsName)
	queryResults := fmt.Sprintf("SELECT * FROM table(`%s`) LIMIT 10", resultsName)
	fmt.Printf("Query: %s\n", queryResults)

	resultRows, err := conn.Query(ctx, queryResults)
	if err != nil {
		fmt.Printf("Error querying results: %v\n", err)
	} else {
		defer resultRows.Close()
		printRows(resultRows)
	}

	// 3. Query the device_temperatures stream
	fmt.Printf("\nQuerying source stream: device_temperatures\n")
	querySource := "SELECT * FROM table(device_temperatures) ORDER BY timestamp DESC LIMIT 10"
	fmt.Printf("Query: %s\n", querySource)

	sourceRows, err := conn.Query(ctx, querySource)
	if err != nil {
		fmt.Printf("Error querying source: %v\n", err)
	} else {
		defer sourceRows.Close()
		printRows(sourceRows)
	}
}

func printRows(rows driver.Rows) {
	// Get columns
	columns := rows.Columns()
	fmt.Printf("Columns: %v\n", columns)

	// Print rows
	rowCount := 0
	for rows.Next() {
		rowCount++

		// Create array of interface{} to hold values
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		if err := rows.Scan(values...); err != nil {
			fmt.Printf("Error scanning row: %v\n", err)
			return
		}

		// Print row data
		fmt.Printf("Row %d:\n", rowCount)
		for i, col := range columns {
			val := *(values[i].(*interface{}))
			fmt.Printf("  %s: %v\n", col, val)
		}
		fmt.Println()
	}

	if err := rows.Err(); err != nil {
		fmt.Printf("Error during row iteration: %v\n", err)
		return
	}

	if rowCount == 0 {
		fmt.Println("No rows found")
	} else {
		fmt.Printf("Found %d rows\n", rowCount)
	}
}
