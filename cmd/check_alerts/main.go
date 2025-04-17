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

	fmt.Println("Checking tp_alerts for any alerts...")
	rows, err := conn.Query(ctx, "SELECT * FROM table(tp_alerts) LIMIT 10")
	if err != nil {
		log.Fatalf("Failed to query alerts: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		columns := rows.Columns()
		values := make([]interface{}, len(columns))
		valPtrs := make([]interface{}, len(columns))
		for i := range values {
			valPtrs[i] = &values[i]
		}

		if err := rows.Scan(valPtrs...); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		fmt.Println("Alert found:")
		for i, col := range columns {
			fmt.Printf("  %s: %v\n", col, values[i])
		}
		fmt.Println()
	}

	if count == 0 {
		fmt.Println("No alerts found")
	} else {
		fmt.Printf("Found %d alerts\n", count)
	}
}
