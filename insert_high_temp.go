package temp_alert

import (
	"context"
	"fmt"
	"log"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
)

func Run() {
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
		log.Fatalf("Failed to connect to Timeplus: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()

	// Insert high temperature data
	deviceID := "device1"
	temperature := 55.0

	query := fmt.Sprintf("INSERT INTO temperatures (device_id, temperature) VALUES ('%s', %f)",
		deviceID, temperature)

	fmt.Printf("Inserting data: device=%s, temperature=%.1f\n", deviceID, temperature)
	err = conn.Exec(ctx, query)
	if err != nil {
		fmt.Printf("Failed to insert temperature data: %v\n", err)
	} else {
		fmt.Printf("Successfully inserted temperature data for device %s\n", deviceID)
	}

	// Wait a bit to see if an alert is generated
	fmt.Println("Waiting 5 seconds for potential alert generation...")
	time.Sleep(5 * time.Second)

	// Query alerts table to see if an alert was generated
	fmt.Println("\nChecking tp_alerts stream for any alerts...")
	alertQuery := "SELECT * FROM table(`tp_alerts`) LIMIT 10"

	rows, err := conn.Query(ctx, alertQuery)
	if err != nil {
		fmt.Printf("Failed to query alerts: %v\n", err)
		return
	}
	defer rows.Close()

	// Check if we have any rows
	alertCount := 0
	for rows.Next() {
		alertCount++
		// Get column names
		columns := rows.Columns()

		// Scan the row
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			fmt.Printf("Error scanning row: %v\n", err)
			continue
		}

		// Print row data
		fmt.Println("Alert data:")
		for i, col := range columns {
			fmt.Printf("  %s: %v\n", col, values[i])
		}
		fmt.Println()
	}

	if alertCount == 0 {
		fmt.Println("No alerts found in tp_alerts stream")
	} else {
		fmt.Printf("Found %d alerts in tp_alerts stream\n", alertCount)
	}
}
