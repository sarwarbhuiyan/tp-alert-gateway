package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	proton "github.com/timeplus-io/proton-go-driver/v2"
)

func main() {
	logrus.SetLevel(logrus.InfoLevel)
	logrus.Info("Starting cleanup of all Alert Gateway streams and views")

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
		logrus.Fatalf("Failed to connect to Timeplus: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Get all views first, then streams
	viewQuery := "SELECT name from system.tables WHERE engine = 'MaterializedView' or engine = 'View'"
	streamQuery := "SHOW STREAMS"

	// Delete views first since they depend on streams
	rows, err := conn.Query(ctx, viewQuery)
	if err != nil {
		logrus.Fatalf("Failed to query views: %v", err)
	}

	views := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			logrus.Warnf("Failed to scan view name: %v", err)
			continue
		}
		views = append(views, name)
	}
	rows.Close()

	// Delete rule views first
	for _, view := range views {
		if strings.Contains(view, "rule_") && (strings.HasSuffix(view, "_view") || strings.Contains(view, "_view_")) {
			logrus.Infof("Dropping view: %s", view)
			if err := conn.Exec(ctx, fmt.Sprintf("DROP VIEW `%s`", view)); err != nil {
				logrus.Warnf("Failed to drop view %s: %v", view, err)
			}
		}
	}

	// Delete any other views
	for _, view := range views {
		if !strings.Contains(view, "rule_") {
			logrus.Infof("Dropping view: %s", view)
			if err := conn.Exec(ctx, fmt.Sprintf("DROP VIEW `%s`", view)); err != nil {
				logrus.Warnf("Failed to drop view %s: %v", view, err)
			}
		}
	}

	// Now get all streams
	rows, err = conn.Query(ctx, streamQuery)
	if err != nil {
		logrus.Fatalf("Failed to query streams: %v", err)
	}

	streams := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			logrus.Warnf("Failed to scan stream name: %v", err)
			continue
		}
		streams = append(streams, name)
	}
	rows.Close()

	// First delete rule result streams
	for _, stream := range streams {
		if strings.Contains(stream, "rule_") && strings.HasSuffix(stream, "_results") {
			logrus.Infof("Dropping stream: %s", stream)
			if err := conn.Exec(ctx, fmt.Sprintf("DROP STREAM `%s`", stream)); err != nil {
				logrus.Warnf("Failed to drop stream %s: %v", stream, err)
			}
		}
	}

	// Then delete Alert Gateway system streams
	sysStreams := []string{
		"tp_rules",
		"tp_alerts",
		"tp_alert_acks",
		"tp_alert_acks_mutable",
	}

	for _, stream := range sysStreams {
		if contains(streams, stream) {
			logrus.Infof("Dropping stream: %s", stream)
			if err := conn.Exec(ctx, fmt.Sprintf("DROP STREAM `%s`", stream)); err != nil {
				logrus.Warnf("Failed to drop stream %s: %v", stream, err)
			}
		}
	}

	// Delete any remaining test streams
	for _, stream := range streams {
		if strings.Contains(stream, "test_") ||
			strings.Contains(stream, "device_") ||
			strings.Contains(stream, "temperatures") ||
			strings.Contains(stream, "sensor_") {
			logrus.Infof("Dropping stream: %s", stream)
			if err := conn.Exec(ctx, fmt.Sprintf("DROP STREAM `%s`", stream)); err != nil {
				logrus.Warnf("Failed to drop stream %s: %v", stream, err)
			}
		}
	}

	logrus.Info("Cleanup completed")
}

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}
