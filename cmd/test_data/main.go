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
	fmt.Println("Test Data Generator - Sending high temperature readings")

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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	// Insert a high temperature for device_1 that should trigger the first rule
	fmt.Println("Inserting device_1 temperature = 26.5°C (should trigger 'Device 1 Temperature Alert')")
	insertData(conn, "device_1", 26.5)

	// Wait a moment
	time.Sleep(1 * time.Second)

	// Insert a very high temperature that should trigger the high temp rule
	fmt.Println("Inserting device_2 temperature = 35.0°C (should trigger 'High Temperature Alert')")
	insertData(conn, "device_2", 35.0)

	// Wait a moment
	time.Sleep(1 * time.Second)

	// Insert a low temperature that should trigger the low temp rule
	fmt.Println("Inserting device_3 temperature = 18.0°C (should trigger 'Low Temperature Alert')")
	insertData(conn, "device_3", 18.0)

	fmt.Println("Test data inserted successfully")

	// Let's verify the data was inserted
	fmt.Println("\nVerifying data in device_temperatures stream:")
	displayLatestTemperatures(conn)

	fmt.Println("\nDone. Wait a moment and then check for alerts using: curl http://localhost:8080/api/alerts")
}

func insertData(conn driver.Conn, deviceID string, temperature float64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Prepare a batch for insertion
	query := "INSERT INTO device_temperatures (device_id, temperature, timestamp)"
	batch, err := conn.PrepareBatch(ctx, query)
	if err != nil {
		log.Fatalf("Failed to prepare batch: %v", err)
	}
	defer batch.Abort()

	// Add values to the batch
	err = batch.Append(
		deviceID,
		temperature,
		time.Now(),
	)
	if err != nil {
		log.Fatalf("Failed to append data for %s: %v", deviceID, err)
	}

	// Send the batch
	if err := batch.Send(); err != nil {
		log.Fatalf("Failed to send batch for %s: %v", deviceID, err)
	}
	fmt.Printf("Successfully inserted temperature %.1f°C for %s\n", temperature, deviceID)
}

func displayLatestTemperatures(conn driver.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := conn.Query(ctx, "SELECT device_id, temperature, timestamp FROM table(device_temperatures) ORDER BY timestamp DESC LIMIT 5")
	if err != nil {
		log.Fatalf("Failed to query device_temperatures: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		var deviceID string
		var temperature float64
		var timestamp time.Time

		if err := rows.Scan(&deviceID, &temperature, &timestamp); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}

		fmt.Printf("Row %d: device_id: %s, temperature: %.1f°C, timestamp: %s\n",
			count, deviceID, temperature, timestamp.Format(time.RFC3339))
	}

	if count == 0 {
		fmt.Println("No data found in device_temperatures stream")
	}
}
