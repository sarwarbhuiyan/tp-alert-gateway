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
	fmt.Println("High Temperature Data Checker")

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
		Compression: &proton.Compression{
			Method: proton.CompressionLZ4,
		},
	})
	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}

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

	// Check for high temperature data
	fmt.Printf("\nChecking for high temperature data...\n")

	// Give the query more time
	queryCtx, queryCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer queryCancel()

	highTempQuery := "SELECT * FROM table(device_temperatures) WHERE temperature > 38 ORDER BY timestamp DESC LIMIT 10"
	highTempRows, err := conn.Query(queryCtx, highTempQuery)
	if err != nil {
		fmt.Printf("Error querying high temperatures: %v\n", err)
		return
	}
	defer highTempRows.Close()

	// Get column names
	columns := highTempRows.Columns()
	columnTypes := highTempRows.ColumnTypes()
	fmt.Printf("Columns: %v\n", columns)

	// Count rows
	rowCount := 0
	for highTempRows.Next() {
		rowCount++
		// Create a slice for scanning, matching column types
		scanArgs := make([]interface{}, len(columns))
		for i, ct := range columnTypes {
			scanArgs[i] = reflect.New(ct.ScanType()).Interface()
		}

		if err := highTempRows.Scan(scanArgs...); err != nil {
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

	if err := highTempRows.Err(); err != nil {
		fmt.Printf("Error iterating rows: %v\n", err)
		return
	}

	if rowCount == 0 {
		fmt.Printf("No high temperature data found\n")
	} else {
		fmt.Printf("Found %d high temperature readings\n", rowCount)
	}

	// Insert a high temperature reading
	fmt.Printf("\nInserting a test high temperature reading...\n")
	// Prepare a batch for insertion
	query := "INSERT INTO device_temperatures (device_id, temperature, timestamp)"
	batch, err := conn.PrepareBatch(queryCtx, query)
	if err != nil {
		fmt.Printf("Failed to prepare batch: %v\n", err)
		return
	}
	defer batch.Abort()

	// Add values to the batch
	err = batch.Append(
		"device_test",
		49.9,
		time.Now(),
	)
	if err != nil {
		fmt.Printf("Failed to append data: %v\n", err)
		return
	}

	// Send the batch
	if err := batch.Send(); err != nil {
		fmt.Printf("Failed to send batch: %v\n", err)
		return
	}

	fmt.Println("Successfully inserted test temperature 49.9°C for device_test")

	// Check again to verify the insert worked
	fmt.Printf("\nVerifying the insert by querying high temperatures again...\n")
	verifyQuery := "SELECT * FROM table(device_temperatures) WHERE temperature > 45 ORDER BY timestamp DESC LIMIT 5"
	verifyRows, err := conn.Query(queryCtx, verifyQuery)
	if err != nil {
		fmt.Printf("Error verifying: %v\n", err)
		return
	}
	defer verifyRows.Close()

	// Get updated column info
	columns = verifyRows.Columns()
	columnTypes = verifyRows.ColumnTypes()

	verifyCount := 0
	for verifyRows.Next() {
		verifyCount++
		// Create a slice for scanning, matching column types
		scanArgs := make([]interface{}, len(columns))
		for i, ct := range columnTypes {
			scanArgs[i] = reflect.New(ct.ScanType()).Interface()
		}

		if err := verifyRows.Scan(scanArgs...); err != nil {
			fmt.Printf("Error scanning verification row: %v\n", err)
			continue
		}

		// Print row data
		fmt.Printf("Verification Row %d:\n", verifyCount)
		for i, col := range columns {
			// Dereference the pointer received from Scan
			val := reflect.ValueOf(scanArgs[i]).Elem().Interface()
			fmt.Printf("  %s: %v\n", col, val)
		}
	}

	if verifyCount == 0 {
		fmt.Printf("Verification failed: No high temperature data found after insert\n")
	} else {
		fmt.Printf("Verification successful: Found %d high temperature readings after insert\n", verifyCount)
	}
}
