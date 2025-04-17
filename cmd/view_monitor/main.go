package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/lib/driver"
)

// Run a continuous monitor to check that the view has a reasonable data rate
// and restart it if not

func main() {
	fmt.Println("View Monitor")

	// Parse command line arguments
	// Example: go run cmd/view_monitor/main.go rule_1234_view
	args := os.Args
	if len(args) < 2 {
		log.Fatal("Usage: view_monitor <view_name>")
	}
	viewName := args[1]

	// Connect to Timeplus
	fmt.Printf("Connecting to Timeplus to monitor view: %s\n", viewName)
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

	// Set up a streaming query on the specified view
	// The streaming query will run continuously, and we'll check that it's
	// receiving data at a reasonable rate
	streamingCtx, streamingCancel := context.WithCancel(context.Background())
	defer streamingCancel()

	// First, make sure the view exists
	checkViewQuery := fmt.Sprintf("SHOW STREAMS LIKE '%s'", viewName)
	rows, err := conn.Query(context.Background(), checkViewQuery)
	if err != nil {
		log.Fatalf("Error checking if view exists: %v", err)
	}
	defer rows.Close()

	viewExists := rows.Next()
	if !viewExists {
		log.Fatalf("View %s does not exist", viewName)
	}

	// Start a goroutine to execute the streaming query and report data as it arrives
	dataChan := make(chan time.Time, 100) // Channel to track when data arrives
	doneChan := make(chan struct{})       // Channel to signal when the streaming query is done

	go func() {
		defer close(doneChan)

		// Create a streaming query that selects all columns from the view
		streamingQuery := fmt.Sprintf("SELECT * FROM table(`%s`)", viewName)
		fmt.Printf("Starting streaming query: %s\n", streamingQuery)

		// Execute the streaming query
		rows, err := conn.Query(streamingCtx, streamingQuery)
		if err != nil {
			log.Printf("Error executing streaming query: %v", err)
			return
		}
		defer rows.Close()

		// Process each row as it arrives
		rowCount := 0
		for rows.Next() {
			// We don't need to process the data, just count it
			rowCount++
			dataChan <- time.Now() // Signal that data arrived

			// Just for debugging - print a dot for every row
			if rowCount%10 == 0 {
				fmt.Print(".")
			}
			if rowCount%100 == 0 {
				fmt.Println() // Line break every 100 dots
			}

			// Check for context cancellation
			select {
			case <-streamingCtx.Done():
				fmt.Println("Streaming query canceled")
				return
			default:
				// Continue
			}
		}

		if err := rows.Err(); err != nil {
			log.Printf("Error in streaming query: %v", err)
		}
	}()

	// Set up a ticker to check the data rate
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var lastDataTime time.Time
	var noDataCount int

	// Monitor the data rate
	for {
		select {
		case dataTime := <-dataChan:
			// Data arrived, update the last data time
			lastDataTime = dataTime
			noDataCount = 0 // Reset the counter when data arrives

		case <-ticker.C:
			// Check how long it's been since we last received data
			sinceLast := time.Since(lastDataTime)
			if lastDataTime.IsZero() {
				// We haven't received any data yet
				fmt.Println("No data received yet")
			} else if sinceLast > 30*time.Second {
				// It's been more than 30 seconds since we received data
				noDataCount++
				fmt.Printf("No data received in %v (count: %d)\n", sinceLast.Round(time.Second), noDataCount)

				if noDataCount >= 3 {
					fmt.Println("No data received for too long, checking if the view needs to be recreated")
					// TODO: Add logic to recreate the view if needed
					// For now, just restart the streaming query
					fmt.Println("Restarting the streaming query")
					streamingCancel() // Cancel the current query
					time.Sleep(1 * time.Second)

					// Create a new context and start a new streaming query
					streamingCtx, streamingCancel = context.WithCancel(context.Background())
					go restartStreamingQuery(streamingCtx, doneChan, dataChan, conn, viewName)

					noDataCount = 0 // Reset the counter
				}
			} else {
				// We're receiving data at a reasonable rate
				fmt.Printf("Last data received %v ago\n", sinceLast.Round(time.Second))
			}

		case <-doneChan:
			// Streaming query ended
			fmt.Println("Streaming query ended")
			return
		}
	}
}

// Restart the streaming query with a new context
func restartStreamingQuery(ctx context.Context, doneChan chan struct{}, dataChan chan time.Time, conn driver.Conn, viewName string) {
	defer func() {
		doneChan <- struct{}{} // Signal that the streaming query is done
	}()

	// Create a streaming query that selects all columns from the view
	streamingQuery := fmt.Sprintf("SELECT * FROM table(`%s`)", viewName)
	fmt.Printf("Restarting streaming query: %s\n", streamingQuery)

	// Execute the streaming query
	rows, err := conn.Query(ctx, streamingQuery)
	if err != nil {
		log.Printf("Error executing streaming query: %v", err)
		return
	}
	defer rows.Close()

	// Process each row as it arrives
	rowCount := 0
	for rows.Next() {
		// We don't need to process the data, just count it
		rowCount++
		dataChan <- time.Now() // Signal that data arrived

		// Just for debugging - print a dot for every row
		if rowCount%10 == 0 {
			fmt.Print(".")
		}
		if rowCount%100 == 0 {
			fmt.Println() // Line break every 100 dots
		}

		// Check for context cancellation
		select {
		case <-ctx.Done():
			fmt.Println("Streaming query canceled")
			return
		default:
			// Continue
		}
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error in streaming query: %v", err)
	}
}
