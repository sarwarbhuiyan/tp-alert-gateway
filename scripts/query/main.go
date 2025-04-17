package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

func main() {
	fmt.Println("Query Timeplus View Tool")

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

	// First, let's check our device_temperatures stream using table()
	fmt.Println("\nQuerying device_temperatures:")
	results, err := client.ExecuteQuery(ctx, "SELECT * FROM table(device_temperatures) ORDER BY _tp_time DESC LIMIT 10")
	if err != nil {
		log.Fatalf("Failed to query device_temperatures: %v", err)
	}

	for _, result := range results {
		// Pretty print the result
		jsonResult, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(jsonResult))
	}

	// Now check the rule view using table()
	viewName := "rule_dc7db4da-74f6-4378-af3e-3f2226db2622_view"
	fmt.Printf("\nQuerying rule view %s:\n", viewName)

	viewResults, err := client.ExecuteQuery(ctx, fmt.Sprintf("SELECT * FROM table(`%s`) ORDER BY _tp_time DESC LIMIT 10", viewName))
	if err != nil {
		fmt.Printf("Error querying view: %v\n", err)
	} else if len(viewResults) == 0 {
		fmt.Println("No results found in the view.")
	} else {
		for _, result := range viewResults {
			jsonResult, _ := json.MarshalIndent(result, "", "  ")
			fmt.Println(string(jsonResult))
		}
	}
}
