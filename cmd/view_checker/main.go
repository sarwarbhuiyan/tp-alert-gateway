package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: view_checker <rule_id>")
		os.Exit(1)
	}

	ruleID := os.Args[1]
	viewName := fmt.Sprintf("rule_%s_view", ruleID)

	fmt.Printf("Checking materialized view %s\n", viewName)

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

	// Query the materialized view
	tableQuery := fmt.Sprintf("SELECT * FROM table(`%s`) LIMIT 10", viewName)
	fmt.Printf("Query: %s\n", tableQuery)

	rows, err := conn.Query(ctx, tableQuery)
	if err != nil {
		log.Fatalf("Error querying view: %v", err)
	}
	defer rows.Close()

	// Get column names and types
	columns := rows.Columns()
	columnTypes := rows.ColumnTypes()
	fmt.Printf("Columns: %v\n", columns)

	// Print rows
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
			// Dereference the pointer
			val := reflect.ValueOf(scanArgs[i]).Elem().Interface()
			fmt.Printf("  %s: %v\n", col, val)
		}
	}

	if rowCount == 0 {
		fmt.Printf("No data found in view %s\n", viewName)
	} else {
		fmt.Printf("Found %d rows in view %s\n", rowCount, viewName)
	}
}
