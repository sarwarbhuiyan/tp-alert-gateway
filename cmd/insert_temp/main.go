package main

import (
	"context"
	"fmt"
	"log"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
)

func main() {
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
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()

	// Format time without timezone information
	currentTime := time.Now().Format("2006-01-02 15:04:05")

	// Insert high temperature data to trigger alerts
	insertQuery := fmt.Sprintf(
		"INSERT INTO device_temperatures (device_id, temperature, timestamp) VALUES ('device_1', 35.0, '%s')",
		currentTime,
	)

	err = conn.Exec(ctx, insertQuery)
	if err != nil {
		log.Fatalf("Failed to insert: %v", err)
	}
	fmt.Println("Successfully inserted high temperature data")

	// Wait a bit for the alert to be generated
	fmt.Println("Waiting 5 seconds for alert to be generated...")
	time.Sleep(5 * time.Second)
}
