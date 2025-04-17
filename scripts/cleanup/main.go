package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

func main() {
	fmt.Println("Timeplus Cleanup Tool - Removing rule-related streams and views")

	// Load the configuration
	cfg, err := config.LoadConfig("config.local.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create a Timeplus client
	client, err := timeplus.NewClient(&cfg.Timeplus)
	if err != nil {
		log.Fatalf("Failed to create Timeplus client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Find streams with names containing "rule_"
	fmt.Println("Querying for rule-related streams...")
	results, err := client.ExecuteQuery(ctx, "SHOW STREAMS")
	if err != nil {
		log.Fatalf("Failed to query streams: %v", err)
	}

	// Track number of successfully deleted streams
	deletedCount := 0

	// Process each stream
	for _, result := range results {
		// Get stream name
		streamName, ok := result["name"].(string)
		if !ok {
			continue
		}

		// Check if it's a rule-related stream
		if strings.Contains(streamName, "rule_") {
			fmt.Printf("Found rule stream: %s\n", streamName)

			// Try to delete using client.DeleteStream
			fmt.Printf("Attempting to delete stream: %s\n", streamName)
			err := client.DeleteStream(ctx, streamName)
			if err != nil {
				fmt.Printf("  Error: %v\n", err)

				// As a fallback, try to drop it using raw SQL
				fmt.Printf("  Trying alternative method for %s...\n", streamName)
				_, dropErr := client.ExecuteQuery(ctx, fmt.Sprintf("DROP STREAM `%s`", streamName))
				if dropErr != nil {
					fmt.Printf("  Failed with alternative method: %v\n", dropErr)
				} else {
					fmt.Printf("  Successfully deleted %s using SQL\n", streamName)
					deletedCount++
				}
			} else {
				fmt.Printf("  Successfully deleted %s\n", streamName)
				deletedCount++
			}
		}
	}

	fmt.Printf("\nDeleted %d rule-related streams\n", deletedCount)
	fmt.Println("Cleanup complete!")
}
