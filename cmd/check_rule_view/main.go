package main

import (
	"context"
	"fmt"
	"log"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
)

func main() {
	// Connect to Timeplus
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

	// List all views to find the rule view
	fmt.Println("Listing all materialized views:")
	viewQuery := "SELECT name FROM system.tables WHERE engine = 'MaterializedView' OR engine = 'View'"
	views, err := conn.Query(ctx, viewQuery)
	if err != nil {
		log.Fatalf("Failed to list views: %v", err)
	}
	defer views.Close()

	// Look for the rule view
	ruleViewName := ""
	for views.Next() {
		var name string
		if err := views.Scan(&name); err != nil {
			log.Printf("Error scanning view name: %v", err)
			continue
		}

		fmt.Printf("Found view: %s\n", name)

		// Look for views that match our rule ID pattern
		if len(name) > 5 && name[:5] == "rule_" {
			ruleViewName = name
		}
	}

	if ruleViewName == "" {
		fmt.Println("No rule view found")
		return
	}

	// Check the content of the rule view
	fmt.Printf("\nChecking rule view: %s\n", ruleViewName)

	// Instead of using * with unknown columns, let's examine the schema first
	schemaQuery := fmt.Sprintf("DESCRIBE `%s`", ruleViewName)
	schemaRows, err := conn.Query(ctx, schemaQuery)
	if err != nil {
		log.Printf("Failed to get schema: %v\n", err)
		fmt.Println("Skipping schema query, will try direct query...")
	} else {
		defer schemaRows.Close()
		fmt.Println("Schema for rule view:")
		for schemaRows.Next() {
			var name, typeStr, defaultType, defaultExpr string
			if err := schemaRows.Scan(&name, &typeStr, &defaultType, &defaultExpr); err != nil {
				log.Printf("Error scanning schema: %v", err)
				continue
			}
			fmt.Printf("Column: %s, Type: %s\n", name, typeStr)
		}
	}

	// Insert a high temperature reading to trigger the rule
	fmt.Println("\nInserting a high temperature reading to trigger the rule...")
	err = conn.Exec(ctx, "INSERT INTO temperatures (device_id, temperature) VALUES ('device1', 55.0)")
	if err != nil {
		log.Fatalf("Failed to insert temperature data: %v", err)
	}
	fmt.Println("Successfully inserted high temperature data")

	// Wait a bit
	fmt.Println("Waiting 2 seconds...")
	time.Sleep(2 * time.Second)

	// Now let's check if the rule was triggered
	fmt.Println("\nChecking tp_alerts for any alerts...")
	rows, err := conn.Query(ctx, "SELECT id, rule_id, rule_name, severity, triggered_at, data FROM table(tp_alerts) LIMIT 10")
	if err != nil {
		log.Fatalf("Failed to query alerts: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		var id, ruleID, ruleName, severity, data string
		var triggeredAt time.Time

		if err := rows.Scan(&id, &ruleID, &ruleName, &severity, &triggeredAt, &data); err != nil {
			log.Printf("Error scanning alert row: %v", err)
			continue
		}

		fmt.Println("Alert found:")
		fmt.Printf("  id: %s\n", id)
		fmt.Printf("  rule_id: %s\n", ruleID)
		fmt.Printf("  rule_name: %s\n", ruleName)
		fmt.Printf("  severity: %s\n", severity)
		fmt.Printf("  triggered_at: %v\n", triggeredAt)
		fmt.Printf("  data: %s\n", data)
		fmt.Println()
	}

	if count == 0 {
		fmt.Println("No alerts found")
	} else {
		fmt.Printf("Found %d alerts\n", count)
	}
}
