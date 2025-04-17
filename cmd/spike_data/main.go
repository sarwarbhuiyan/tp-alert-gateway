package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
)

func main() {
	// Parse command line flags
	tempPtr := flag.Float64("temperature", 40.0, "Temperature value to send")
	devicePtr := flag.String("device", "device_2", "Device ID to use")
	flag.Parse()

	fmt.Printf("Temperature Spike Generator - Sending temperature %.1f°C for %s\n", *tempPtr, *devicePtr)

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

	// Insert temperature for the device
	fmt.Printf("Inserting temperature: %s = %.1f°C\n", *devicePtr, *tempPtr)
	insertSQL := fmt.Sprintf(
		"INSERT INTO device_temperatures (device_id, temperature, timestamp) VALUES ('%s', %.1f, '%s')",
		*devicePtr,
		*tempPtr,
		time.Now().Format("2006-01-02 15:04:05"), // Use a simpler date format that Timeplus accepts
	)

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = conn.Exec(ctx, insertSQL)
	if err != nil {
		log.Fatalf("Failed to insert temperature: %v", err)
	}
	fmt.Println("Successfully inserted temperature")

	// Wait a few seconds for the alert to be processed
	fmt.Println("Waiting 3 seconds for alert to be processed...")
	time.Sleep(3 * time.Second)

	// Check for alerts from API
	fmt.Println("\nLet's check for alerts: curl http://localhost:8081/api/alerts")
}
