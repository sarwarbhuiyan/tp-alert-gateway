package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

func main() {
	fmt.Println("Drop Rule Streams Tool - Removing all streams that start with 'rule_'")

	// Set up logging
	logrus.SetLevel(logrus.InfoLevel)

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

	// Drop each stream using the client's DeleteStream method
	for _, streamName := range streamsToDrop {
		fmt.Printf("Dropping stream: %s\n", streamName)
		err := client.DeleteStream(ctx, streamName)
		if err != nil {
			fmt.Printf("Error dropping %s: %v\n", streamName, err)
		} else {
			fmt.Printf("Successfully dropped %s\n", streamName)
		}
	}

	fmt.Println("Done!")

	// As a secondary step, find and drop any materialized views
	fmt.Println("\nChecking for any remaining rule views...")

	// Check if there are any views with "rule" in their name
	checkViewsQuery := "SELECT name FROM system.streams WHERE stream_type = 'materialized_view' AND name LIKE '%rule%'"
	viewResults, err := client.ExecuteQuery(ctx, checkViewsQuery)
	if err != nil {
		fmt.Printf("Error checking for views: %v\n", err)
		return
	}

	if len(viewResults) > 0 {
		fmt.Printf("Found %d rule-related materialized views\n", len(viewResults))
		for _, result := range viewResults {
			viewName, ok := result["name"].(string)
			if !ok {
				continue
			}

			fmt.Printf("Attempting to drop view: %s\n", viewName)
			dropViewQuery := fmt.Sprintf("DROP VIEW IF EXISTS `%s`", viewName)
			_, err := client.ExecuteQuery(ctx, dropViewQuery)
			if err != nil {
				fmt.Printf("Error dropping view %s: %v\n", viewName, err)
			} else {
				fmt.Printf("Successfully dropped view %s\n", viewName)
			}
		}
	} else {
		fmt.Println("No rule-related materialized views found.")
	}
}
