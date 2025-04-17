package main

import (
	"context"
	"fmt"
	"log"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
)

func main() {
	fmt.Println("MEGA SPIKE Generator - Sending extreme temperature (50°C)")

	// Connect to Timeplus
	fmt.Println("Connecting to Timeplus...")
	conn, err := proton.Open(&proton.Options{
		Addr: []string{"localhost:8464"},
		Auth: proton.Auth{
			Database: "default",
			Username: "test",
			Password: "test123",
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

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

	// Insert MEGA temperature spike for device_1 (50°C)
	fmt.Println("Inserting MEGA temperature spike: device_1 = 50.0°C")
	insertSQL := fmt.Sprintf(
		"INSERT INTO device_temperatures (device_id, temperature, timestamp) VALUES ('device_1', 50.0, '%s')",
		time.Now().Format("2006-01-02 15:04:05"),
	)

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = conn.Exec(ctx, insertSQL)
	if err != nil {
		log.Fatalf("Failed to insert mega temperature spike: %v", err)
	}
	fmt.Println("Successfully inserted MEGA temperature 50.0°C for device_1")

	// Insert another extreme one for device_2 just to be sure
	fmt.Println("Inserting MEGA temperature spike: device_2 = 45.0°C")
	insertSQL = fmt.Sprintf(
		"INSERT INTO device_temperatures (device_id, temperature, timestamp) VALUES ('device_2', 45.0, '%s')",
		time.Now().Format("2006-01-02 15:04:05"),
	)

	err = conn.Exec(ctx, insertSQL)
	if err != nil {
		log.Fatalf("Failed to insert mega temperature spike: %v", err)
	}
	fmt.Println("Successfully inserted MEGA temperature 45.0°C for device_2")

	// Wait a few seconds for the alert to be processed
	fmt.Println("Waiting 5 seconds for alert to be processed...")
	time.Sleep(5 * time.Second)

	// Check for alerts from API
	fmt.Println("\nLet's check for alerts: curl http://localhost:8080/api/alerts")
}
