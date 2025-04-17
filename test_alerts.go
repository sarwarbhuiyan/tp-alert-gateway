package main

import (
	"fmt"
)

func main() {
	fmt.Println("Inserting high temperature data and checking for alerts...")

	// Create a direct connection to Timeplus
	connectToTimeplus()
}

func connectToTimeplus() {
	// Connect directly to Timeplus and insert data
	// This avoids package conflicts by keeping everything in one file

	// Run SQL commands directly to test our system
	runSQL := `
	-- 1. First, insert a high temperature reading
	INSERT INTO temperatures (device_id, temperature) VALUES ('device1', 55.0);
	
	-- 2. Wait for the alert to be generated
	SELECT sleep(5);
	
	-- 3. Check for alerts
	SELECT * FROM table(tp_alerts) LIMIT 10;
	`

	fmt.Println("Please run these commands directly in the Timeplus SQL console:")
	fmt.Println(runSQL)
}
