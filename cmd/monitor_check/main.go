package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	proton "github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/lib/driver"
)

type RuleStatus struct {
	RuleID        string
	ViewName      string
	Status        string
	LastHeartbeat time.Time
}

// Global map to track rule status (simulating what would be in the service)
var (
	activeRules = make(map[string]*RuleStatus)
	ruleMutex   sync.RWMutex
)

// Simulate a heartbeat that would be used in the rule service
func monitorRule(ctx context.Context, ruleID, viewName string, conn driver.Conn, heartbeatCh chan<- string) {
	defer fmt.Printf("Rule monitor for %s stopped\n", ruleID)

	// Update rule status
	updateRuleStatus(ruleID, "starting", viewName)

	// Report heartbeat every 2 seconds
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Query the view every 5 seconds
	queryTicker := time.NewTicker(5 * time.Second)
	defer queryTicker.Stop()

	// Track query metrics
	queryCount := 0
	rowCount := 0

	// This would normally be a long-running goroutine
	for {
		select {
		case <-ctx.Done():
			updateRuleStatus(ruleID, "stopped", viewName)
			return
		case <-ticker.C:
			// Send heartbeat
			select {
			case heartbeatCh <- ruleID:
				// Heartbeat sent successfully
			default:
				// Channel full or closed - log and continue
				fmt.Printf("WARNING: Could not send heartbeat for rule %s - channel blocked\n", ruleID)
			}
		case <-queryTicker.C:
			// Query the view directly
			queryCount++
			updateRuleStatus(ruleID, fmt.Sprintf("active (queries: %d, rows: %d)", queryCount, rowCount), viewName)

			tableQuery := fmt.Sprintf("SELECT * FROM table(`%s`) LIMIT 10", viewName)
			rows, err := conn.Query(ctx, tableQuery)
			if err != nil {
				fmt.Printf("Error querying view %s: %v\n", viewName, err)
				continue
			}

			// Count how many rows we get
			localRowCount := 0
			for rows.Next() {
				localRowCount++
			}
			rowCount += localRowCount
			rows.Close()

			fmt.Printf("Rule %s: Query %d found %d rows in view %s\n",
				ruleID, queryCount, localRowCount, viewName)
		}
	}
}

// Update the status of a rule
func updateRuleStatus(ruleID, status, viewName string) {
	ruleMutex.Lock()
	defer ruleMutex.Unlock()

	if _, exists := activeRules[ruleID]; !exists {
		activeRules[ruleID] = &RuleStatus{
			RuleID:   ruleID,
			ViewName: viewName,
		}
	}

	activeRules[ruleID].Status = status
	activeRules[ruleID].LastHeartbeat = time.Now()
}

// Process heartbeats from rule monitors
func processHeartbeats(heartbeatCh <-chan string) {
	for ruleID := range heartbeatCh {
		ruleMutex.Lock()
		if rule, exists := activeRules[ruleID]; exists {
			rule.LastHeartbeat = time.Now()
		}
		ruleMutex.Unlock()
	}
}

// Print status of all active rules
func printRuleStatus() {
	ruleMutex.RLock()
	defer ruleMutex.RUnlock()

	fmt.Println("\n==== RULE MONITOR STATUS ====")
	fmt.Printf("Total active rule monitors: %d\n", len(activeRules))

	if len(activeRules) == 0 {
		fmt.Println("No active rule monitors found")
		return
	}

	for _, rule := range activeRules {
		timeSinceHeartbeat := time.Since(rule.LastHeartbeat)
		health := "healthy"
		if timeSinceHeartbeat > 5*time.Second {
			health = "STALLED/DEAD"
		}

		fmt.Printf("Rule %s (%s):\n", rule.RuleID, rule.ViewName)
		fmt.Printf("  Status: %s\n", rule.Status)
		fmt.Printf("  Last heartbeat: %s ago\n", timeSinceHeartbeat.Round(time.Millisecond))
		fmt.Printf("  Health: %s\n", health)
	}
	fmt.Println("===============================")
}

func main() {
	fmt.Println("Rule Monitor Health Checker")

	// Connect to Timeplus to check views
	fmt.Println("Connecting to Timeplus...")
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

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("Failed to connect to Timeplus: %v", err)
	}
	fmt.Println("Connected to Timeplus")

	// Create a buffered channel for heartbeats
	heartbeatCh := make(chan string, 100)

	// Start heartbeat processor
	go processHeartbeats(heartbeatCh)

	// Create cancellable context for all rule monitors
	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	defer monitorCancel()

	// Start some rule monitors (simulating what the service would do)
	rules := []struct {
		id       string
		name     string
		viewName string
	}{
		{"rule1", "Test Rule 1", "rule_1_view"},
		{"rule2", "Test Rule 2", "rule_2_view"},
		{"92bc414d-c482-4c10-ba61-222ebeaba7af", "Debug Test Alert", "rule_92bc414d-c482-4c10-ba61-222ebeaba7af_view"},
	}

	for _, rule := range rules {
		go monitorRule(monitorCtx, rule.id, rule.viewName, conn, heartbeatCh)
		fmt.Printf("Started monitor for rule %s (%s)\n", rule.id, rule.name)
	}

	// Periodically print status
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// Run for 60 seconds then exit
	timeout := time.After(60 * time.Second)

	// Simulate cancelling one rule after 15 seconds
	killOneRule := time.After(15 * time.Second)

	// Main loop
	for {
		select {
		case <-ticker.C:
			printRuleStatus()
		case <-killOneRule:
			// Simulate one rule dying
			fmt.Println("\n⚠️ Simulating rule 'rule1' goroutine dying...")

			// Update its status to failed (this would happen automatically when the goroutine exits)
			updateRuleStatus("rule1", "failed/crashed", "rule_1_view")

			// Note: In a real implementation, we would monitor for this failure and restart it
		case <-timeout:
			fmt.Println("Monitor check completed")
			return
		}
	}
}
