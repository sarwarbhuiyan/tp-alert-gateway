package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
)

// Rule represents a simplified alert rule from the API
type Rule struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Status       string `json:"status"`
	ViewName     string `json:"viewName"`
	ResultStream string `json:"resultStream"`
}

func main() {
	// Get Timeplus connection details from environment variables
	tpAddress := getEnvWithDefault("TIMEPLUS_ADDRESS", "localhost:8464")
	tpUser := getEnvWithDefault("TIMEPLUS_USER", "test")
	tpPassword := getEnvWithDefault("TIMEPLUS_PASSWORD", "test123")
	tpWorkspace := getEnvWithDefault("TIMEPLUS_WORKSPACE", "default")

	fmt.Printf("Connecting to Timeplus at %s (user: %s)\n", tpAddress, tpUser)

	// Connect to Timeplus
	conn, err := proton.Open(&proton.Options{
		Addr: []string{tpAddress},
		Auth: proton.Auth{
			Database: tpWorkspace,
			Username: tpUser,
			Password: tpPassword,
		},
		DialTimeout: 5 * time.Second,
		Compression: &proton.Compression{
			Method: proton.CompressionLZ4,
		},
	})
	if err != nil {
		log.Fatalf("Failed to connect to Timeplus: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()

	// List materialized views
	fmt.Println("Listing materialized views (using direct query):")
	viewResult, err := conn.Query(ctx, "SELECT name FROM system.tables WHERE engine='MaterializedView'")
	if err != nil {
		log.Fatalf("Failed to list views: %v", err)
	}
	defer viewResult.Close()

	// Store view names for further inspection
	var viewNames []string

	// Print view names
	for viewResult.Next() {
		var name string
		if err := viewResult.Scan(&name); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("- %s\n", name)
		viewNames = append(viewNames, name)
	}
	if err := viewResult.Err(); err != nil {
		log.Fatalf("Error during rows iteration: %v", err)
	}

	// Get and print the definition of each view
	fmt.Println("\nView Definitions:")
	for _, viewName := range viewNames {
		// SHOW CREATE VIEW query
		showCreateQuery := fmt.Sprintf("SHOW CREATE VIEW `%s`", viewName)
		createRows, err := conn.Query(ctx, showCreateQuery)
		if err != nil {
			log.Printf("Failed to get definition for view %s: %v", viewName, err)
			continue
		}

		if createRows.Next() {
			var statement string
			if err := createRows.Scan(&statement); err != nil {
				log.Printf("Failed to scan definition for view %s: %v", viewName, err)
			} else {
				fmt.Printf("View: %s\nDefinition: %s\n\n", viewName, statement)
			}
		}
		createRows.Close()
	}

	// Check for streams
	fmt.Println("\nListing streams:")
	streamRows, err := conn.Query(ctx, "SELECT name FROM system.tables WHERE engine='Stream'")
	if err != nil {
		log.Fatalf("Failed to list streams: %v", err)
	}
	defer streamRows.Close()

	// Store stream names
	var allStreamNames []string
	// Store rule result stream names for further inspection
	var resultStreams []string

	// Print stream names
	for streamRows.Next() {
		var name string
		if err := streamRows.Scan(&name); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		allStreamNames = append(allStreamNames, name)

		// Check if this is a rule result stream
		if contains(name, "_results") {
			fmt.Printf("- [RULE RESULT] %s\n", name)
			resultStreams = append(resultStreams, name)
		} else {
			fmt.Printf("- %s\n", name)
		}
	}
	if err := streamRows.Err(); err != nil {
		log.Fatalf("Error during rows iteration: %v", err)
	}

	// Show sample data from rule result streams
	if len(resultStreams) > 0 {
		fmt.Println("\nSample data from rule result streams:")
		for _, streamName := range resultStreams {
			fmt.Printf("\nStream: %s\n", streamName)
			sampleQuery := fmt.Sprintf("SELECT * FROM `%s` LIMIT 5", streamName)
			sampleRows, err := conn.Query(ctx, sampleQuery)
			if err != nil {
				log.Printf("Failed to query sample data from %s: %v", streamName, err)
				continue
			}

			// Get column names
			columns := sampleRows.Columns()
			if len(columns) > 0 {
				fmt.Printf("Columns: %v\n", columns)
			}

			// Track if we found any data
			rowCount := 0

			// Print first few rows of data
			for sampleRows.Next() && rowCount < 5 {
				rowCount++
				// We don't know the exact schema, so we'll use a simple approach
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := sampleRows.Scan(valuePtrs...); err != nil {
					log.Printf("Failed to scan row data: %v", err)
					break
				}

				fmt.Printf("Row %d: ", rowCount)
				for i, col := range columns {
					fmt.Printf("%s: %v, ", col, values[i])
				}
				fmt.Println()
			}

			if rowCount == 0 {
				fmt.Println("No data found")
			}

			sampleRows.Close()
		}
	}

	// Check rule status from the API
	fmt.Println("\nChecking rule status from the API:")

	resp, err := http.Get("http://localhost:8080/api/rules")
	if err != nil {
		log.Printf("Warning: Failed to get rules from API: %v", err)
	} else {
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Warning: Failed to read API response: %v", err)
		} else {
			var rules []Rule
			if err := json.Unmarshal(body, &rules); err != nil {
				log.Printf("Warning: Failed to parse rules from API: %v", err)
			} else {
				// Print rules and check if materialized views exist
				for _, rule := range rules {
					viewExists := false
					for _, viewName := range viewNames {
						if viewName == rule.ViewName {
							viewExists = true
							break
						}
					}

					resultStreamExists := false
					for _, streamName := range resultStreams {
						if streamName == rule.ResultStream {
							resultStreamExists = true
							break
						}
					}

					fmt.Printf("- Rule: %s (status: %s)\n", rule.Name, rule.Status)
					fmt.Printf("  View: %s (exists: %t)\n", rule.ViewName, viewExists)
					fmt.Printf("  Result Stream: %s (exists: %t)\n", rule.ResultStream, resultStreamExists)
				}
			}
		}
	}

	// Try to create sample test data
	fmt.Println("\nInserting test data that should trigger an alert:")
	// Check if device_temperatures stream exists
	deviceTempExists := false
	for _, name := range allStreamNames {
		if name == "device_temperatures" {
			deviceTempExists = true
			break
		}
	}

	if deviceTempExists {
		// Insert a high temperature reading that should trigger a rule
		insertQuery := "INSERT INTO device_temperatures (device_id, temperature, timestamp) VALUES ('device_1', 35.0, now())"
		if err := conn.Exec(ctx, insertQuery); err != nil {
			log.Printf("Failed to insert test data: %v", err)
		} else {
			fmt.Println("Successfully inserted test data (device_1 temperature = 35.0)")
		}
	} else {
		fmt.Println("Warning: device_temperatures stream not found, cannot insert test data")
	}
}

// Helper function to get environment variable with a default value
func getEnvWithDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
