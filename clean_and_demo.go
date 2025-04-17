package main

import (
	"context"
	"fmt"
	"log"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
)

func main() {
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

	// 1. Clean up all views
	fmt.Println("\n--- Step 1: Cleaning up all materialized views ---")
	cleanViews(ctx, conn)

	// 2. Clean up all streams
	fmt.Println("\n--- Step 2: Cleaning up all streams ---")
	cleanStreams(ctx, conn)

	// 3. Create temperature stream for demo
	fmt.Println("\n--- Step 3: Creating temperature stream ---")
	createTemperatureStream(ctx, conn)

	// 4. Insert some baseline data
	fmt.Println("\n--- Step 4: Inserting baseline data ---")
	insertTemperatureData(ctx, conn, "device1", 25.0) // Normal temperature
	insertTemperatureData(ctx, conn, "device2", 20.0) // Normal temperature

	// 5. Start the alert gateway service
	fmt.Println("\n--- Step 5: Starting Alert Gateway service ---")
	fmt.Println("Please start the Alert Gateway service in another terminal with:")
	fmt.Println("  go run cmd/server/main.go")
	fmt.Println("Wait for the service to start, then press Enter to continue...")
	fmt.Scanln()

	// 6. Create a rule to detect high temperatures
	fmt.Println("\n--- Step 6: Creating rule for high temperatures ---")
	createHighTemperatureRule()
	fmt.Println("Rule created. Wait for it to start and press Enter to continue...")
	fmt.Scanln()

	// 7. Insert data that triggers the rule
	fmt.Println("\n--- Step 7: Inserting data that will trigger an alert ---")
	insertTemperatureData(ctx, conn, "device1", 55.0) // High temperature that should trigger alert
	fmt.Println("Waiting 5 seconds for alert to be generated...")
	time.Sleep(5 * time.Second)

	// 8. Check generated alerts
	fmt.Println("\n--- Step 8: Checking for generated alerts ---")
	checkAlerts()

	// 9. View alert data
	fmt.Println("\n--- Step 9: Viewing alert data to verify JSON payload ---")
	checkAlertData()

	fmt.Println("\n--- Demo completed successfully! ---")
}

// cleanViews drops all materialized views in the system
func cleanViews(ctx context.Context, conn proton.Conn) {
	// Use the correct query to list views in Timeplus
	rows, err := conn.Query(ctx, "SELECT name FROM system.tables WHERE engine = 'MaterializedView' OR engine = 'View'")
	if err != nil {
		fmt.Printf("Failed to list views: %v\n", err)
		return
	}
	defer rows.Close()

	var views []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			fmt.Printf("Error scanning view: %v\n", err)
			continue
		}
		views = append(views, name)
	}

	if len(views) == 0 {
		fmt.Println("No views found")
		return
	}

	fmt.Printf("Found %d views to delete\n", len(views))
	for _, view := range views {
		dropQuery := fmt.Sprintf("DROP VIEW IF EXISTS `%s`", view)
		fmt.Printf("Dropping view: %s\n", view)
		err := conn.Exec(ctx, dropQuery)
		if err != nil {
			fmt.Printf("Failed to drop view %s: %v\n", view, err)
		} else {
			fmt.Printf("Successfully dropped view: %s\n", view)
		}
	}
}

// cleanStreams drops all streams in the system
func cleanStreams(ctx context.Context, conn proton.Conn) {
	rows, err := conn.Query(ctx, "SHOW STREAMS")
	if err != nil {
		fmt.Printf("Failed to list streams: %v\n", err)
		return
	}
	defer rows.Close()

	var streams []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			fmt.Printf("Error scanning stream: %v\n", err)
			continue
		}
		streams = append(streams, name)
	}

	if len(streams) == 0 {
		fmt.Println("No streams found")
		return
	}

	fmt.Printf("Found %d streams to delete\n", len(streams))
	for _, stream := range streams {
		dropQuery := fmt.Sprintf("DROP STREAM IF EXISTS `%s`", stream)
		fmt.Printf("Dropping stream: %s\n", stream)
		err := conn.Exec(ctx, dropQuery)
		if err != nil {
			fmt.Printf("Failed to drop stream %s: %v\n", stream, err)
		} else {
			fmt.Printf("Successfully dropped stream: %s\n", stream)
		}
	}
}

// createTemperatureStream creates a stream for temperature data
func createTemperatureStream(ctx context.Context, conn proton.Conn) {
	// Fix the data types to use lowercase
	query := `
CREATE STREAM temperatures (
  device_id string,
  temperature float64
)
`
	fmt.Println("Creating temperatures stream...")
	err := conn.Exec(ctx, query)
	if err != nil {
		fmt.Printf("Failed to create temperatures stream: %v\n", err)
	} else {
		fmt.Println("Successfully created temperatures stream")
	}
}

// insertTemperatureData inserts a temperature reading for a device
func insertTemperatureData(ctx context.Context, conn proton.Conn, deviceID string, temperature float64) {
	// Don't include _tp_time in the INSERT as it's added automatically
	query := fmt.Sprintf("INSERT INTO temperatures (device_id, temperature) VALUES ('%s', %f)",
		deviceID, temperature)

	fmt.Printf("Inserting data: device=%s, temperature=%.1f\n", deviceID, temperature)
	err := conn.Exec(ctx, query)
	if err != nil {
		fmt.Printf("Failed to insert temperature data: %v\n", err)
	} else {
		fmt.Printf("Successfully inserted temperature data for device %s\n", deviceID)
	}
}

// createHighTemperatureRule creates a rule to detect high temperatures
func createHighTemperatureRule() {
	ruleJSON := `{
		"name": "High Temperature Alert",
		"description": "Detect when temperature exceeds 50°C",
		"query": "SELECT device_id, temperature FROM temperatures WHERE temperature > 50",
		"severity": "critical",
		"throttleMinutes": 5,
		"sourceStream": "temperatures"
	}`

	fmt.Println("Creating high temperature rule via API...")
	fmt.Println("curl -X POST -H \"Content-Type: application/json\" -d '" + ruleJSON + "' http://localhost:8080/api/rules")
	fmt.Println("Please execute the above curl command or create the rule through the UI.")
}

// checkAlerts checks for alerts generated by the rule
func checkAlerts() {
	fmt.Println("Checking for alerts via API...")
	fmt.Println("curl http://localhost:8080/api/alerts")
	fmt.Println("Please execute the above curl command or check alerts through the UI.")
}

// checkAlertData views the raw JSON data of the latest alert
func checkAlertData() {
	fmt.Println("To view alert data, first get the alerts to find the alert ID:")
	fmt.Println("curl http://localhost:8080/api/alerts")
	fmt.Println("Then view the alert data using the alert ID:")
	fmt.Println("curl http://localhost:8080/api/alerts/ALERT_ID_HERE/data")
	fmt.Println("Please execute the above curl commands or check alert data through the UI.")
}
