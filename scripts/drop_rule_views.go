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
	fmt.Println("Drop Rule Views Tool - Removing all streams that start with 'rule_'")

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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get all streams
	fmt.Println("Querying for all streams...")
	results, err := client.ExecuteQuery(ctx, "SHOW STREAMS")
	if err != nil {
		log.Fatalf("Failed to query streams: %v", err)
	}

	var streamsToDrop []string
	for _, result := range results {
		// Get stream name from the result
		streamName, ok := result["name"].(string)
		if !ok {
			continue
		}

		// Check if it's a stream that starts with "rule_"
		if strings.HasPrefix(streamName, "rule_") {
			streamsToDrop = append(streamsToDrop, streamName)
		}
	}

	fmt.Printf("Found %d rule streams to drop\n", len(streamsToDrop))

	// Drop each stream
	for _, streamName := range streamsToDrop {
		fmt.Printf("Dropping stream: %s\n", streamName)
		_, err := client.ExecuteQuery(ctx, fmt.Sprintf("DROP STREAM IF EXISTS `%s`", streamName))
		if err != nil {
			fmt.Printf("Error dropping %s: %v\n", streamName, err)
		} else {
			fmt.Printf("Successfully dropped %s\n", streamName)
		}
	}

	fmt.Println("Done!")
}
