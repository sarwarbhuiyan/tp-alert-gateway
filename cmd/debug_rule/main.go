package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

// Rule represents a simplified alert rule from the API
type Rule struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Status       string `json:"status"`
	ViewName     string `json:"viewName"`
	ResultStream string `json:"resultStream"`
	Query        string `json:"query"`
	LastError    string `json:"lastError,omitempty"`
}

func main() {
	fmt.Println("=== Alert Gateway Debug Tool ===")

	// Get all rules
	rules, err := getRules()
	if err != nil {
		log.Fatalf("Failed to get rules: %v", err)
	}

	fmt.Printf("\nFound %d rules\n", len(rules))

	// Pick a rule for testing
	var selectedRule *Rule
	for i, rule := range rules {
		fmt.Printf("%d) Rule '%s' (status: %s)\n", i+1, rule.Name, rule.Status)
	}

	if len(rules) == 0 {
		log.Fatalf("No rules found")
	}

	// For simplicity, we'll just select the first rule
	selectedRule = &rules[0]
	fmt.Printf("\nSelected rule for testing: %s (ID: %s)\n", selectedRule.Name, selectedRule.ID)
	fmt.Printf("Rule query: %s\n", selectedRule.Query)
	fmt.Printf("Result stream: %s\n", selectedRule.ResultStream)
	fmt.Printf("View name: %s\n", selectedRule.ViewName)

	// Stop the rule if it's running
	fmt.Println("\nStopping the rule if it's running...")
	err = stopRule(selectedRule.ID)
	if err != nil {
		fmt.Printf("Warning: Error stopping rule: %v\n", err)
	}

	// Clean up the materialized view manually with SQL
	fmt.Println("\nCleaning up the materialized view...")
	dropViewSQL := fmt.Sprintf("DROP VIEW IF EXISTS `%s`", selectedRule.ViewName)
	runSQL(dropViewSQL)

	// Clean up the result stream manually with SQL
	fmt.Println("\nCleaning up the result stream...")
	dropStreamSQL := fmt.Sprintf("DROP STREAM IF EXISTS `%s`", selectedRule.ResultStream)
	runSQL(dropStreamSQL)

	// Start the rule
	fmt.Println("\nStarting the rule...")
	err = startRule(selectedRule.ID)
	if err != nil {
		log.Fatalf("Failed to start rule: %v", err)
	}

	// Wait a moment for rule to initialize
	fmt.Println("Waiting for rule to start...")
	time.Sleep(3 * time.Second)

	// Check rule status
	updatedRule, err := getRule(selectedRule.ID)
	if err != nil {
		log.Fatalf("Failed to get updated rule status: %v", err)
	}

	fmt.Printf("Rule status after starting: %s\n", updatedRule.Status)
	if updatedRule.Status == "failed" && updatedRule.LastError != "" {
		fmt.Printf("Rule error: %s\n", updatedRule.LastError)
	}

	// Insert test data manually
	fmt.Println("\nInserting test data that should trigger the rule...")
	insertSQL := "INSERT INTO device_temperatures (device_id, temperature, timestamp) VALUES ('device_1', 35.0, now())"
	runSQL(insertSQL)

	// Check the materialized view directly
	fmt.Printf("\nChecking materialized view directly (%s)...\n", updatedRule.ViewName)
	viewSQL := fmt.Sprintf("SELECT * FROM `%s` LIMIT 10", updatedRule.ViewName)
	runSQL(viewSQL)

	// Check the result stream directly
	fmt.Printf("\nChecking result stream directly (%s)...\n", updatedRule.ResultStream)
	streamSQL := fmt.Sprintf("SELECT * FROM `%s` LIMIT 10", updatedRule.ResultStream)
	runSQL(streamSQL)

	// Wait a moment for alerts to be generated
	fmt.Println("\nWaiting for alerts to be generated...")
	time.Sleep(5 * time.Second)

	// Check for alerts from the API
	fmt.Println("\nChecking for alerts from the API...")
	alerts, err := getAlerts()
	if err != nil {
		fmt.Printf("Warning: Failed to get alerts: %v\n", err)
	} else {
		fmt.Printf("Found %d alerts\n", len(alerts))
		for i, alert := range alerts {
			fmt.Printf("Alert %d: %s\n", i+1, alert)
		}
	}
}

func getRules() ([]Rule, error) {
	resp, err := http.Get("http://localhost:8080/api/rules")
	if err != nil {
		return nil, fmt.Errorf("failed to get rules: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var rules []Rule
	if err := json.Unmarshal(body, &rules); err != nil {
		return nil, fmt.Errorf("failed to parse rules: %w", err)
	}

	return rules, nil
}

func getRule(id string) (*Rule, error) {
	resp, err := http.Get(fmt.Sprintf("http://localhost:8080/api/rules/%s", id))
	if err != nil {
		return nil, fmt.Errorf("failed to get rule: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var rule Rule
	if err := json.Unmarshal(body, &rule); err != nil {
		return nil, fmt.Errorf("failed to parse rule: %w", err)
	}

	return &rule, nil
}

func stopRule(id string) error {
	req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:8080/api/rules/%s/stop", id), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to stop rule: %s", string(body))
	}

	return nil
}

func startRule(id string) error {
	req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:8080/api/rules/%s/start", id), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to start rule: %s", string(body))
	}

	return nil
}

func runSQL(query string) {
	// Use the proton-go-driver CLI to run SQL directly
	cmd := exec.Command("echo", query)
	sqlCmd := exec.Command("proton-go-cli", "--host", "localhost:8464", "--user", "test", "--password", "test", "--database", "default")

	// Pipe the echo output to the SQL command
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Printf("Error creating pipe: %v\n", err)
		return
	}
	sqlCmd.Stdin = pipe

	// Set up output capture
	sqlOutput, err := sqlCmd.StdoutPipe()
	if err != nil {
		fmt.Printf("Error creating output pipe: %v\n", err)
		return
	}

	// Start the commands
	err = cmd.Start()
	if err != nil {
		fmt.Printf("Error starting echo command: %v\n", err)
		return
	}

	err = sqlCmd.Start()
	if err != nil {
		fmt.Printf("Error starting SQL command: %v\n", err)
		return
	}

	// Read output
	outputBytes, err := io.ReadAll(sqlOutput)
	if err != nil {
		fmt.Printf("Error reading SQL output: %v\n", err)
		return
	}

	// Wait for commands to finish
	cmd.Wait()
	sqlCmd.Wait()

	// Print output
	output := string(outputBytes)
	if strings.TrimSpace(output) == "" {
		fmt.Println("No output (command likely succeeded)")
	} else {
		fmt.Println("SQL Output:")
		fmt.Println(output)
	}
}

func getAlerts() ([]string, error) {
	resp, err := http.Get("http://localhost:8080/api/alerts")
	if err != nil {
		return nil, fmt.Errorf("failed to get alerts: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// Format alerts as strings
	var alertStrings []string
	alerts, ok := response["alerts"].([]interface{})
	if ok {
		for _, alert := range alerts {
			alertMap, ok := alert.(map[string]interface{})
			if ok {
				alertJSON, _ := json.MarshalIndent(alertMap, "", "  ")
				alertStrings = append(alertStrings, string(alertJSON))
			}
		}
	}

	return alertStrings, nil
}
