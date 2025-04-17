package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

func main() {
	fmt.Println("Mutable Stream Check Utility")

	// Initialize logger
	logrus.SetLevel(logrus.InfoLevel)
	logrus.SetOutput(os.Stdout)

	// Create config
	cfg := &config.TimeplusConfig{
		Address:   "localhost:8464",
		Username:  "test",
		Password:  "test123",
		Workspace: "default",
	}

	// Create Timeplus client
	client, err := timeplus.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create Timeplus client: %v", err)
	}

	// Create context
	ctx := context.Background()

	// Check if mutable stream exists
	streams, err := client.ListStreams(ctx)
	if err != nil {
		log.Fatalf("Failed to list streams: %v", err)
	}

	fmt.Println("\nAll streams in Timeplus:")
	for _, stream := range streams {
		fmt.Printf("- %s\n", stream)
	}

	// Check device_temperatures
	fmt.Println("\nQuerying device_temperatures:")
	temperatureResults, err := client.ExecuteQuery(ctx, "SELECT * FROM table(device_temperatures) ORDER BY timestamp DESC LIMIT 5")
	if err != nil {
		fmt.Printf("Error querying device_temperatures: %v\n", err)
	} else {
		printResults(temperatureResults)
	}

	// Check tp_alert_acks_mutable
	fmt.Println("\nQuerying tp_alert_acks_mutable:")
	acksResults, err := client.ExecuteQuery(ctx, "SELECT * FROM table(tp_alert_acks_mutable) LIMIT 5")
	if err != nil {
		fmt.Printf("Error querying tp_alert_acks_mutable: %v\n", err)
	} else {
		printResults(acksResults)
	}

	// Check if our rule's view exists
	ruleID := "d00a5121-d7d9-49d0-8c01-2eeabfb46b8a"
	viewName := fmt.Sprintf("rule_%s_view", ruleID)
	viewExists := false

	for _, stream := range streams {
		if stream == viewName {
			viewExists = true
			break
		}
	}

	if viewExists {
		fmt.Printf("\nRule view %s exists\n", viewName)

		// Query the view
		fmt.Printf("Querying %s:\n", viewName)
		viewResults, err := client.ExecuteQuery(ctx, fmt.Sprintf("SELECT * FROM table(`%s`) LIMIT 5", viewName))
		if err != nil {
			fmt.Printf("Error querying view: %v\n", err)
		} else {
			printResults(viewResults)
		}
	} else {
		fmt.Printf("\nRule view %s does not exist\n", viewName)
	}

	// Check tp_alerts stream
	fmt.Println("\nQuerying tp_alerts stream:")
	alertsResults, err := client.ExecuteQuery(ctx, "SELECT * FROM table(tp_alerts) LIMIT 10")
	if err != nil {
		fmt.Printf("Error querying tp_alerts: %v\n", err)
	} else {
		printResults(alertsResults)
	}

	// Check rule results stream
	resultsStreamName := fmt.Sprintf("rule_%s_results", ruleID)
	fmt.Printf("\nQuerying rule results stream %s:\n", resultsStreamName)
	ruleResults, err := client.ExecuteQuery(ctx, fmt.Sprintf("SELECT * FROM table(`%s`) LIMIT 10", resultsStreamName))
	if err != nil {
		fmt.Printf("Error querying rule results: %v\n", err)
	} else {
		printResults(ruleResults)
	}
}

func printResults(results []map[string]interface{}) {
	if len(results) == 0 {
		fmt.Println("No results found")
		return
	}

	// Get column names from first result
	var columns []string
	for key := range results[0] {
		columns = append(columns, key)
	}

	// Print each row
	for i, row := range results {
		fmt.Printf("Row %d:\n", i+1)
		for _, col := range columns {
			fmt.Printf("  %s: %v\n", col, row[col])
		}
		fmt.Println()
	}

	fmt.Printf("Found %d rows\n", len(results))
}
