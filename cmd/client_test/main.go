package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

func main() {
	// Setup logging
	logrus.SetLevel(logrus.InfoLevel)
	logrus.SetOutput(os.Stdout)

	// Create Timeplus client
	tpConfig := &config.TimeplusConfig{
		Address:   "localhost:8464",
		Username:  "test",
		Password:  "test123",
		Workspace: "default",
	}

	client, err := timeplus.NewClient(tpConfig)
	if err != nil {
		logrus.Fatalf("Failed to create Timeplus client: %v", err)
	}

	fmt.Println("Connected to Timeplus successfully.")

	// Define timeout context for all operations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test 1: Create a test stream
	testStream := fmt.Sprintf("client_test_stream_%d", time.Now().Unix())
	fmt.Printf("\n=== Test 1: Create Stream '%s' ===\n", testStream)

	schema := []timeplus.Column{
		{Name: "id", Type: "string"},
		{Name: "value", Type: "float64"},
		{Name: "timestamp", Type: "datetime64(3)", Nullable: true},
	}

	err = client.CreateStream(ctx, testStream, schema)
	if err != nil {
		logrus.Fatalf("Failed to create test stream: %v", err)
	}
	fmt.Println("✅ Stream created successfully")

	// Test 2: Insert data into stream
	fmt.Printf("\n=== Test 2: Insert Data into Stream ===\n")
	insertColumns := []string{"id", "value", "timestamp"}
	insertValues := []interface{}{
		"test1",
		42.5,
		time.Now(),
	}

	err = client.InsertIntoStream(ctx, testStream, insertColumns, insertValues)
	if err != nil {
		logrus.Fatalf("Failed to insert data: %v", err)
	}
	fmt.Println("✅ Data inserted successfully")

	// Test 3: Execute query to retrieve data
	fmt.Printf("\n=== Test 3: Execute Query ===\n")

	// Wait longer for the data to be available
	fmt.Println("Waiting 3 seconds for data to be available...")
	time.Sleep(3 * time.Second)

	result, err := client.ExecuteQuery(ctx, fmt.Sprintf("SELECT * FROM table(%s) LIMIT 10", testStream))
	if err != nil {
		logrus.Fatalf("Failed to execute query: %v", err)
	}

	fmt.Printf("Query returned %d rows\n", len(result))
	for i, row := range result {
		fmt.Printf("Row %d: %+v\n", i+1, row)
	}
	fmt.Println("✅ Query executed successfully")

	// Clean up
	err = client.DeleteStream(ctx, testStream)
	if err != nil {
		logrus.Warnf("Failed to delete stream: %v", err)
	}

	fmt.Println("\n=== All tests completed successfully! ===")
}
