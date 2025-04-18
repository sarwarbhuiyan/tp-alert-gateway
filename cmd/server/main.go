package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/sirupsen/logrus"
	httpSwagger "github.com/swaggo/http-swagger"

	"github.com/timeplus-io/tp-alert-gateway/pkg/api"
	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
	"github.com/timeplus-io/tp-alert-gateway/pkg/services"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

// @title Timeplus Alert Gateway API
// @version 1.0
// @description API for managing alerts on Timeplus streams
// @BasePath /api

func main() {
	// Configure Log Level from Environment Variable
	logLevelStr := os.Getenv("LOG_LEVEL")
	switch strings.ToLower(logLevelStr) {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn", "warning":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	case "fatal":
		logrus.SetLevel(logrus.FatalLevel)
	case "panic":
		logrus.SetLevel(logrus.PanicLevel)
	default:
		logrus.SetLevel(logrus.InfoLevel) // Default to Info
	}
	logrus.Infof("Log level set to: %s", logrus.GetLevel().String())

	// Parse command line flags
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logrus.Fatalf("Failed to load config: %v", err)
	}

	// Set up the Timeplus client
	tpClient, err := timeplus.NewClient(&cfg.Timeplus)
	if err != nil {
		logrus.Fatalf("Failed to create Timeplus client: %v", err)
	}

	// Set up required streams with proper schemas
	ctx := context.Background()
	if err := tpClient.SetupStreams(ctx); err != nil {
		logrus.Warnf("Failed to set up streams: %v", err)
	}

	// Initialize services
	ruleService, err := services.NewRuleService(tpClient)
	if err != nil {
		logrus.Fatalf("Failed to create rule service: %v", err)
	}

	// Define the alert stream name
	const AlertStreamName = "tp_alerts"

	// Initialize the alert monitoring service
	// This is now a simplified version without polling goroutines
	alertMonitor := services.NewAlertMonitor(
		ruleService,
		AlertStreamName,
		cfg.Timeplus.Address,
		cfg.Timeplus.Username,
		cfg.Timeplus.Password,
		cfg.Timeplus.Workspace,
		tpClient,
		cfg.Timeplus.Address, // Use the same address for server address
	)

	// Start the alert monitor (only establishes connection, no polling)
	if err := alertMonitor.Start(ctx); err != nil {
		logrus.Fatalf("Failed to start alert monitor: %v", err)
	}
	logrus.Info("Alert monitoring service started")

	// Set up the Echo server
	e := echo.New()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	// API routes
	apiHandler := api.NewAPIHandler(ruleService)
	apiHandler.SetupRoutes(e)

	// Temporary route to list all streams
	e.GET("/debug/streams", func(c echo.Context) error {
		client := ruleService.GetTimeplusClient()
		ctx := context.Background()
		streams, err := client.ListStreams(ctx)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("Failed to list streams: %v", err)})
		}
		return c.JSON(http.StatusOK, streams)
	})

	// Temporary route to view alert acks in the mutable stream
	e.GET("/debug/alert_acks", func(c echo.Context) error {
		client := ruleService.GetTimeplusClient()
		ctx := context.Background()
		query := fmt.Sprintf("SELECT * FROM table(%s) ORDER BY created_at DESC LIMIT 100", timeplus.AlertAcksMutableStream)
		results, err := client.ExecuteQuery(ctx, query)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("Failed to query alert acks: %v", err)})
		}
		return c.JSON(http.StatusOK, results)
	})

	// Temporary route to delete a stream
	e.DELETE("/debug/streams/:name", func(c echo.Context) error {
		streamName := c.Param("name")
		client := ruleService.GetTimeplusClient()
		ctx := context.Background()

		// First try to delete as a view
		err := client.DeleteMaterializedView(ctx, streamName)
		if err != nil {
			logrus.Warnf("Failed to delete as view: %v", err)
		}

		// Then try to delete as a stream
		err = client.DeleteStream(ctx, streamName)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": fmt.Sprintf("Failed to delete stream: %v", err),
			})
		}

		return c.JSON(http.StatusOK, map[string]string{
			"message": fmt.Sprintf("Stream %s deleted successfully", streamName),
		})
	})

	// Swagger documentation
	e.GET("/swagger/*", echo.WrapHandler(httpSwagger.Handler()))

	// Static files for UI
	e.Static("/", "./ui/build")

	// Create HTTP server
	// Use PORT environment variable if available, otherwise use config
	port := os.Getenv("PORT")
	if port == "" {
		port = cfg.Server.Port
	}

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      e,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start the server in a goroutine
	go func() {
		logrus.Infof("Starting server on port %s", port)
		if err := e.StartServer(server); err != nil && err != http.ErrServerClosed {
			logrus.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logrus.Info("Shutting down server...")

	// Shutdown alert monitor
	alertMonitor.Shutdown()
	logrus.Info("Alert monitor shutdown complete")

	// Create a deadline for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Server.ShutdownTimeout)*time.Second)
	defer cancel()

	// Shutdown the server
	if err := e.Shutdown(ctx); err != nil {
		logrus.Fatalf("Server forced to shutdown: %v", err)
	}

	logrus.Info("Server exited properly")
}
