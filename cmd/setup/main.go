package main

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	proton "github.com/timeplus-io/proton-go-driver/v2"
)

func main() {
	logrus.SetLevel(logrus.InfoLevel)
	logrus.Info("Setting up basic streams for Alert Gateway")

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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create the device_temperatures stream
	query := `
	CREATE STREAM device_temperatures (
		device_id string,
		temperature float,
		timestamp datetime
	)
	`
	logrus.Infof("Creating device_temperatures stream")
	if err := conn.Exec(ctx, query); err != nil {
		logrus.Warnf("Failed to create device_temperatures stream: %v", err)
	} else {
		logrus.Infof("Successfully created device_temperatures stream")
	}

	// Verify streams exist
	rows, err := conn.Query(ctx, "SHOW STREAMS")
	if err != nil {
		logrus.Fatalf("Failed to query streams: %v", err)
	}
	defer rows.Close()

	streams := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			logrus.Warnf("Failed to scan stream name: %v", err)
			continue
		}
		streams = append(streams, name)
	}

	logrus.Infof("Available streams: %v", streams)
	logrus.Info("Setup completed")
}
