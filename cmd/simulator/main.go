package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	proton "github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/lib/driver"
)

const (
	defaultDeviceCount = 5
	defaultIntervalMs  = 1000 // 1 second
	streamName         = "device_temperatures"
)

// DeviceTemperature represents a temperature reading from a device
type DeviceTemperature struct {
	DeviceID    string    `json:"device_id"`
	Temperature float64   `json:"temperature"`
	Timestamp   time.Time `json:"timestamp"`
}

// RuleRequest represents a request to create a rule
type RuleRequest struct {
	Name            string `json:"name"`
	Description     string `json:"description"`
	Query           string `json:"query"`
	Severity        string `json:"severity"`
	ThrottleMinutes int    `json:"throttleMinutes"`
	SourceStream    string `json:"sourceStream"`
}

// Alert represents an alert returned from the API
type Alert struct {
	ID             string     `json:"id"`
	RuleID         string     `json:"ruleId"`
	RuleName       string     `json:"ruleName"`
	Severity       string     `json:"severity"`
	TriggeredAt    time.Time  `json:"triggeredAt"`
	Data           string     `json:"data"` // JSON string
	Acknowledged   bool       `json:"acknowledged"`
	AcknowledgedAt *time.Time `json:"acknowledgedAt,omitempty"`
	AcknowledgedBy string     `json:"acknowledgedBy,omitempty"`
}

func main() {
	// Initialize random number generator
	rand.Seed(time.Now().UnixNano())

	// Get configuration from environment variables
	alertGatewayURL := getEnv("ALERT_GATEWAY_URL", "http://localhost:8080")
	deviceCount, _ := strconv.Atoi(getEnv("DEVICE_COUNT", fmt.Sprintf("%d", defaultDeviceCount)))
	intervalMs, _ := strconv.Atoi(getEnv("INTERVAL_MS", fmt.Sprintf("%d", defaultIntervalMs)))
	checkAlerts, _ := strconv.ParseBool(getEnv("CHECK_ALERTS", "true"))
	alertCheckIntervalSec, _ := strconv.Atoi(getEnv("ALERT_CHECK_INTERVAL_SEC", "10"))

	// Connect to Timeplus
	conn := connectToTimeplus()

	// Create stream if it doesn't exist
	if err := createTemperatureStream(conn); err != nil {
		logrus.Fatalf("Failed to create stream: %v", err)
	}

	// Create and start sample rules FIRST
	createdRuleIDs, ok := createSampleRules(alertGatewayURL)
	if !ok {
		logrus.Fatal("Failed to create or start sample rules. Exiting simulator.")
	}

	logrus.Infof("Starting data generation with %d devices, sending data every %d ms",
		deviceCount, intervalMs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start alert checking in a separate goroutine if enabled
	if checkAlerts {
		go monitorAlertsForRules(ctx, alertGatewayURL, createdRuleIDs, time.Duration(alertCheckIntervalSec)*time.Second)
	}

	// Start data generation ONLY if rules were set up
	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Generate and send data for each device
			for i := 1; i <= deviceCount; i++ {
				deviceID := fmt.Sprintf("device_%d", i)
				temp := generateTemperature(deviceID)
				if err := sendTemperatureData(ctx, conn, temp); err != nil {
					logrus.Errorf("Error sending data: %v", err)
				}

				// Occasionally generate anomalous temperatures to trigger alerts
				if rand.Intn(50) == 0 {
					anomalyTemp := generateAnomalyTemperature(deviceID)
					if err := sendTemperatureData(ctx, conn, anomalyTemp); err != nil {
						logrus.Errorf("Error sending anomaly data: %v", err)
					} else {
						logrus.Warnf("🔥 Sent anomaly data: %s - %.2f°C (should trigger alert)",
							anomalyTemp.DeviceID, anomalyTemp.Temperature)
					}
				}
			}
		}
	}
}

// getEnv gets an environment variable with a default value
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// connectToTimeplus connects to the Timeplus instance
func connectToTimeplus() driver.Conn {
	// Get Timeplus connection details from environment variables
	tpAddress := getEnv("TIMEPLUS_ADDRESS", "localhost:8464")
	tpUser := getEnv("TIMEPLUS_USER", "test")
	tpPassword := getEnv("TIMEPLUS_PASSWORD", "test123")
	tpWorkspace := getEnv("TIMEPLUS_WORKSPACE", "default")

	logrus.Infof("Connecting simulator to Timeplus at %s (user: %s)", tpAddress, tpUser)

	// Use direct proton.Open instead of proton.OpenDB
	conn, err := proton.Open(&proton.Options{
		Addr: []string{tpAddress},
		Auth: proton.Auth{
			Database: tpWorkspace,
			Username: tpUser,
			Password: tpPassword,
		},
		DialTimeout: 10 * time.Second,
		Compression: &proton.Compression{
			Method: proton.CompressionLZ4,
		},
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Retry connection a few times
	for i := 0; i < 10; i++ {
		err = conn.Ping(ctx)
		if err == nil {
			break
		}
		logrus.Warnf("Failed to connect to Timeplus (attempt %d/10): %v", i+1, err)
		time.Sleep(3 * time.Second)
	}

	if err != nil {
		logrus.Fatalf("Failed to connect to Timeplus: %v", err)
	}

	logrus.Info("Connected to Timeplus")
	return conn
}

// createTemperatureStream creates the device_temperatures stream in Timeplus
func createTemperatureStream(conn driver.Conn) error {
	ctx := context.Background()

	// Check if stream exists using SHOW STREAMS
	query := fmt.Sprintf("SHOW STREAMS LIKE '%s'", streamName)
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("error executing SHOW STREAMS: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		// Stream already exists
		if rows.Err() == nil {
			logrus.Infof("Stream %s already exists", streamName)
			return nil
		} else {
			return fmt.Errorf("error checking rows from SHOW STREAMS: %w", rows.Err())
		}
	}
	if rows.Err() != nil { // Check error after rows.Next() loop is done
		return fmt.Errorf("error after checking rows from SHOW STREAMS: %w", rows.Err())
	}

	// Create the stream
	createSQL := fmt.Sprintf(`
		CREATE STREAM %s (
			device_id string,
			temperature float64,
			timestamp datetime64
		)
	`, streamName)

	// Use Exec instead of ExecContext
	err = conn.Exec(ctx, createSQL)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	logrus.Infof("Created stream: %s", streamName)
	return nil
}

// generateTemperature generates a realistic temperature for a device
func generateTemperature(deviceID string) DeviceTemperature {
	// Base temperature is 20-25°C, with some randomization based on device ID
	deviceNum, _ := strconv.Atoi(deviceID[7:])
	baseTemp := 20.0 + float64(deviceNum%5)

	// Add some noise
	noise := rand.Float64()*2.0 - 1.0 // -1.0 to 1.0

	return DeviceTemperature{
		DeviceID:    deviceID,
		Temperature: baseTemp + noise,
		Timestamp:   time.Now(),
	}
}

// generateAnomalyTemperature creates an anomalous temperature reading to trigger alerts
func generateAnomalyTemperature(deviceID string) DeviceTemperature {
	// For device_1, generate high temperatures (>25°C)
	// For other devices, generate either very high (>30°C) or very low (<19°C) temperatures
	deviceNum, _ := strconv.Atoi(deviceID[7:])
	var temperature float64

	if deviceNum == 1 {
		// For device_1 specific alert
		temperature = 26.0 + rand.Float64()*4.0 // 26-30°C
	} else if rand.Intn(2) == 0 {
		// High temperature alert
		temperature = 31.0 + rand.Float64()*4.0 // 31-35°C
	} else {
		// Low temperature alert
		temperature = 16.0 + rand.Float64()*2.5 // 16-18.5°C
	}

	return DeviceTemperature{
		DeviceID:    deviceID,
		Temperature: temperature,
		Timestamp:   time.Now(),
	}
}

// sendTemperatureData inserts temperature data into Timeplus
func sendTemperatureData(ctx context.Context, conn driver.Conn, temp DeviceTemperature) error {
	// Prepare a batch for insertion
	query := fmt.Sprintf("INSERT INTO %s (device_id, temperature, timestamp)", streamName)
	batch, err := conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}
	defer batch.Abort()

	// Add values to the batch
	err = batch.Append(
		temp.DeviceID,
		temp.Temperature,
		temp.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("failed to append data to batch: %w", err)
	}

	// Send the batch
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// createSampleRules creates and starts sample alert rules
// Returns the created rule IDs and a boolean success indicator
func createSampleRules(alertGatewayURL string) ([]string, bool) {
	rules := []RuleRequest{
		{
			Name:            "High Temperature Alert",
			Description:     "Alert when any device temperature exceeds 30°C",
			Query:           fmt.Sprintf("SELECT * FROM %s WHERE temperature > 30", streamName),
			Severity:        "critical",
			ThrottleMinutes: 1,
			SourceStream:    streamName,
		},
		{
			Name:            "Device 1 Temperature Alert",
			Description:     "Alert when device_1 temperature exceeds 25°C",
			Query:           fmt.Sprintf("SELECT * FROM %s WHERE device_id = 'device_1' AND temperature > 25", streamName),
			Severity:        "warning",
			ThrottleMinutes: 2,
			SourceStream:    streamName,
		},
		{
			Name:            "Low Temperature Alert",
			Description:     "Alert when any device temperature drops below 19°C",
			Query:           fmt.Sprintf("SELECT * FROM %s WHERE temperature < 19", streamName),
			Severity:        "info",
			ThrottleMinutes: 5,
			SourceStream:    streamName,
		},
	}

	client := &http.Client{Timeout: 10 * time.Second}
	createdRuleIDs := []string{}
	allCreated := true

	logrus.Info("Attempting to create sample rules...")
	for _, rule := range rules {
		data, err := json.Marshal(rule)
		if err != nil {
			logrus.Errorf("Failed to marshal rule request for '%s': %v", rule.Name, err)
			allCreated = false
			continue
		}

		resp, err := client.Post(alertGatewayURL+"/api/rules", "application/json", bytes.NewBuffer(data))
		if err != nil {
			logrus.Errorf("Failed POST request to create rule '%s': %v", rule.Name, err)
			allCreated = false
			continue
		}

		// Check response
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			var ruleResp map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&ruleResp); err == nil {
				if ruleID, ok := ruleResp["id"].(string); ok {
					logrus.Infof("Successfully created rule: %s (ID: %s)", rule.Name, ruleID)
					createdRuleIDs = append(createdRuleIDs, ruleID)
				} else {
					logrus.Errorf("Created rule '%s', but response ID is not a string.", rule.Name)
					allCreated = false
				}
			} else {
				logrus.Errorf("Created rule '%s', but failed to decode response: %v", rule.Name, err)
				allCreated = false
			}
		} else {
			logrus.Errorf("Failed to create rule '%s', status: %d", rule.Name, resp.StatusCode)
			allCreated = false
		}
		resp.Body.Close()
	}

	if !allCreated {
		logrus.Warn("Not all rules were created successfully. Skipping start attempts.")
		return createdRuleIDs, false
	}

	// Allow some time for rules to be fully persisted and available for querying
	logrus.Info("Waiting a few seconds for rules to be available...")
	time.Sleep(5 * time.Second)

	// Start sample rules
	logrus.Info("Attempting to start created rules...")
	allStarted := true
	for _, ruleID := range createdRuleIDs {
		startResp, err := client.Post(alertGatewayURL+"/api/rules/"+ruleID+"/start", "application/json", nil)
		if err != nil {
			logrus.Errorf("Failed POST request to start rule ID %s: %v", ruleID, err)
			allStarted = false
			continue
		}

		if startResp.StatusCode >= 200 && startResp.StatusCode < 300 {
			logrus.Infof("Successfully started rule ID: %s", ruleID)
		} else {
			logrus.Errorf("Failed to start rule ID %s, status: %d", ruleID, startResp.StatusCode)
			allStarted = false
		}
		startResp.Body.Close()
	}

	if !allStarted {
		logrus.Error("Failed to start one or more rules.")
		return createdRuleIDs, false
	}

	return createdRuleIDs, true
}

// monitorAlertsForRules checks for alerts generated by the specified rules
func monitorAlertsForRules(ctx context.Context, alertGatewayURL string, ruleIDs []string, interval time.Duration) {
	client := &http.Client{Timeout: 10 * time.Second}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Track which alerts have been seen
	seenAlerts := make(map[string]bool)

	logrus.Info("Starting alert monitoring...")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Check for alerts from all rules
			resp, err := client.Get(alertGatewayURL + "/api/alerts")
			if err != nil {
				logrus.Errorf("Failed to get alerts: %v", err)
				continue
			}

			if resp.StatusCode != http.StatusOK {
				logrus.Errorf("Failed to get alerts, status: %d", resp.StatusCode)
				resp.Body.Close()
				continue
			}

			var alerts []Alert
			if err := json.NewDecoder(resp.Body).Decode(&alerts); err != nil {
				logrus.Errorf("Failed to decode alerts: %v", err)
				resp.Body.Close()
				continue
			}
			resp.Body.Close()

			for _, alert := range alerts {
				// Skip already seen alerts
				if seenAlerts[alert.ID] {
					continue
				}

				seenAlerts[alert.ID] = true

				// Get the alert data
				alertDataURL := fmt.Sprintf("%s/api/alerts/%s/data", alertGatewayURL, alert.ID)
				dataResp, err := client.Get(alertDataURL)
				if err != nil {
					logrus.Errorf("Failed to get alert data for %s: %v", alert.ID, err)
					continue
				}

				if dataResp.StatusCode != http.StatusOK {
					logrus.Errorf("Failed to get alert data, status: %d", dataResp.StatusCode)
					dataResp.Body.Close()
					continue
				}

				var alertData map[string]interface{}
				if err := json.NewDecoder(dataResp.Body).Decode(&alertData); err != nil {
					logrus.Errorf("Failed to decode alert data: %v", err)
					dataResp.Body.Close()
					continue
				}
				dataResp.Body.Close()

				// Display the new alert with pretty formatting
				logrus.Infof("🔔 NEW ALERT DETECTED:\n"+
					"  ID:          %s\n"+
					"  Rule:        %s (%s)\n"+
					"  Severity:    %s\n"+
					"  Triggered:   %s\n"+
					"  Device:      %s\n"+
					"  Temperature: %.2f°C\n",
					alert.ID,
					alert.RuleName, alert.RuleID,
					alert.Severity,
					alert.TriggeredAt.Format(time.RFC3339),
					getValueAsString(alertData, "parsed_data", "device_id"),
					getValueAsFloat(alertData, "parsed_data", "temperature"),
				)

				// Randomly acknowledge some alerts
				if rand.Intn(3) == 0 {
					go acknowledgeAlert(client, alertGatewayURL, alert.ID)
				}
			}
		}
	}
}

// acknowledgeAlert acknowledges an alert with the API
func acknowledgeAlert(client *http.Client, alertGatewayURL, alertID string) {
	ackData := map[string]string{
		"acknowledgedBy": "simulator",
		"comment":        "Auto-acknowledged by simulator",
	}

	data, err := json.Marshal(ackData)
	if err != nil {
		logrus.Errorf("Failed to marshal acknowledge data: %v", err)
		return
	}

	ackURL := fmt.Sprintf("%s/api/alerts/%s/acknowledge", alertGatewayURL, alertID)
	resp, err := client.Post(ackURL, "application/json", bytes.NewBuffer(data))
	if err != nil {
		logrus.Errorf("Failed to acknowledge alert %s: %v", alertID, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		logrus.Infof("✅ Successfully acknowledged alert %s", alertID)
	} else {
		logrus.Errorf("Failed to acknowledge alert %s, status: %d", alertID, resp.StatusCode)
	}
}

// getValueAsString safely extracts a string value from nested maps
func getValueAsString(data map[string]interface{}, keys ...string) string {
	current := data
	for i, key := range keys {
		if i == len(keys)-1 {
			if str, ok := current[key].(string); ok {
				return str
			}
			return ""
		}

		if next, ok := current[key].(map[string]interface{}); ok {
			current = next
		} else {
			return ""
		}
	}
	return ""
}

// getValueAsFloat safely extracts a float value from nested maps
func getValueAsFloat(data map[string]interface{}, keys ...string) float64 {
	current := data
	for i, key := range keys {
		if i == len(keys)-1 {
			switch v := current[key].(type) {
			case float64:
				return v
			case float32:
				return float64(v)
			case int:
				return float64(v)
			case int64:
				return float64(v)
			default:
				return 0
			}
		}

		if next, ok := current[key].(map[string]interface{}); ok {
			current = next
		} else {
			return 0
		}
	}
	return 0
}
