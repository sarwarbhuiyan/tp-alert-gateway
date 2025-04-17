package services

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	proton "github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

// AlertMonitor manages connection to Timeplus for alerts
// This is a simplified version that no longer uses polling Go routines.
// Instead, we use materialized views that write directly to the alert acks stream.
// This approach has several advantages:
// 1. It's more efficient - no need for constant polling
// 2. It's event-driven - updates happen in real-time
// 3. It's more reliable - if the gateway is restarted, all state is preserved in Timeplus
// 4. It's more scalable - we can handle many more rules without increasing load
type AlertMonitor struct {
	conn          proton.Conn
	ruleService   *RuleService
	alertStream   string
	tpAddress     string
	tpUsername    string
	tpPassword    string
	tpDatabase    string
	tpClient      timeplus.TimeplusClient
	serverAddress string
}

// NewAlertMonitor creates a new alert monitor
func NewAlertMonitor(ruleService *RuleService, alertStream, tpAddress, tpUsername, tpPassword, tpDatabase string, tpClient timeplus.TimeplusClient, serverAddress string) *AlertMonitor {
	return &AlertMonitor{
		ruleService:   ruleService,
		alertStream:   alertStream,
		tpAddress:     tpAddress,
		tpUsername:    tpUsername,
		tpPassword:    tpPassword,
		tpDatabase:    tpDatabase,
		tpClient:      tpClient,
		serverAddress: serverAddress,
	}
}

// Start initializes the alert monitor
func (am *AlertMonitor) Start(ctx context.Context) error {
	logrus.Info("Starting Alert Monitor service")
	logrus.Info("Note: This is now using materialized views to write directly to tp_alert_acks_mutable - no polling is performed")

	// Parse address to separate host and port
	hostPort := am.tpAddress

	// Connect to Timeplus using direct proton driver with more robust settings
	opts := &proton.Options{
		Addr: []string{hostPort},
		Auth: proton.Auth{
			Database: am.tpDatabase,
			Username: am.tpUsername,
			Password: am.tpPassword,
		},
		// More robust connection settings
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		Compression: &proton.Compression{
			Method: proton.CompressionLZ4,
		},
	}

	// Create connection
	conn, err := proton.Open(opts)
	if err != nil {
		return fmt.Errorf("failed to connect to Timeplus: %w", err)
	}

	// Test connection with multiple retries
	var pingErr error
	for i := 0; i < 5; i++ {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		pingErr = conn.Ping(pingCtx)
		cancel() // Always cancel context to prevent leaks

		if pingErr == nil {
			break // Success
		}

		logrus.Warnf("Failed to ping Timeplus (attempt %d/5): %v", i+1, pingErr)
		time.Sleep(2 * time.Second)
	}

	if pingErr != nil {
		conn.Close() // Clean up the connection
		return fmt.Errorf("failed to ping Timeplus after multiple attempts: %w", pingErr)
	}

	am.conn = conn
	logrus.Info("Alert Monitor successfully connected to Timeplus")

	return nil
}

// Shutdown stops the monitor and closes connections
func (am *AlertMonitor) Shutdown() {
	logrus.Info("Shutting down Alert Monitor service")

	if am.conn != nil {
		am.conn.Close()
	}
}

// StartMonitoringRule is now a no-op since we use materialized views
// When a rule is created, a materialized view is automatically set up that:
// 1. Reads from the rule's result stream
// 2. Writes to the tp_alert_acks_mutable stream
// 3. Updates in real-time whenever new alerts are triggered
// This approach eliminates the need for polling and scales better for many rules
func (am *AlertMonitor) StartMonitoringRule(ctx context.Context, ruleID string) error {
	logrus.Infof("Rule monitoring for %s is now handled by materialized views (no-op)", ruleID)
	return nil
}

// StopMonitoringRule is now a no-op since we use materialized views
// When a rule is stopped, the corresponding materialized view is dropped
func (am *AlertMonitor) StopMonitoringRule(ruleID string) {
	logrus.Infof("Stopping rule monitoring for %s is now handled by materialized views (no-op)", ruleID)
}

// ensureConnection ensures we have a valid connection to Timeplus
func (am *AlertMonitor) ensureConnection(ctx context.Context) error {
	// Test connection by running a simple query
	_, err := am.tpClient.ExecuteQuery(ctx, "SELECT 1")
	if err != nil {
		logrus.Errorf("Failed to connect to Timeplus: %v", err)
		return err
	}
	logrus.Info("Successfully connected to Timeplus")
	return nil
}
