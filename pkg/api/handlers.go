package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"

	"github.com/timeplus-io/tp-alert-gateway/pkg/models"
	"github.com/timeplus-io/tp-alert-gateway/pkg/services"
)

// APIHandler handles HTTP API requests
type APIHandler struct {
	ruleService *services.RuleService
}

// NewAPIHandler creates a new API handler
func NewAPIHandler(ruleService *services.RuleService) *APIHandler {
	return &APIHandler{
		ruleService: ruleService,
	}
}

// GetRules returns all rules
func (h *APIHandler) GetRules(c echo.Context) error {
	rules, err := h.ruleService.GetRules()
	if err != nil {
		logrus.Errorf("Error getting rules: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to get rules"})
	}
	return c.JSON(http.StatusOK, rules)
}

// GetRule returns a rule by ID
func (h *APIHandler) GetRule(c echo.Context) error {
	id := c.Param("id")
	rule, err := h.ruleService.GetRule(id)
	if err != nil {
		logrus.Errorf("Error getting rule %s: %v", id, err)
		return c.JSON(http.StatusNotFound, map[string]string{"error": fmt.Sprintf("Rule with ID %s not found", id)})
	}
	return c.JSON(http.StatusOK, rule)
}

// GetAlertRawData returns the raw parsed data field of an alert
func (h *APIHandler) GetAlertRawData(c echo.Context) error {
	id := c.Param("id")
	alert, err := h.ruleService.GetAlert(id)
	if err != nil {
		logrus.Errorf("Error getting alert %s: %v", id, err)
		return c.JSON(http.StatusNotFound, map[string]string{"error": fmt.Sprintf("Alert with ID %s not found", id)})
	}

	// Parse the data field (which is a JSON string) into a map
	var dataMap map[string]interface{}
	if err := json.Unmarshal([]byte(alert.Data), &dataMap); err != nil {
		logrus.Errorf("Error parsing alert data JSON: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "Failed to parse alert data",
			"data":  alert.Data,
		})
	}

	// Return the parsed data
	return c.JSON(http.StatusOK, map[string]interface{}{
		"alert_id":     alert.ID,
		"rule_id":      alert.RuleID,
		"rule_name":    alert.RuleName,
		"triggered_at": alert.TriggeredAt,
		"raw_data":     alert.Data,
		"parsed_data":  dataMap,
	})
}

// CreateRule creates a new rule
func (h *APIHandler) CreateRule(c echo.Context) error {
	var req models.CreateRuleRequest
	if err := c.Bind(&req); err != nil {
		logrus.Errorf("Error binding create rule request: %v", err)
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	// Validate request
	if req.Name == "" || req.Query == "" || req.SourceStream == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Name, query, and source_stream are required"})
	}

	// Create rule
	rule, err := h.ruleService.CreateRule(c.Request().Context(), &req)
	if err != nil {
		logrus.Errorf("Error creating rule: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("Failed to create rule: %v", err)})
	}

	return c.JSON(http.StatusCreated, rule)
}

// UpdateRule updates a rule
func (h *APIHandler) UpdateRule(c echo.Context) error {
	id := c.Param("id")
	var req models.UpdateRuleRequest
	if err := c.Bind(&req); err != nil {
		logrus.Errorf("Error binding update rule request: %v", err)
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	// Update rule
	rule, err := h.ruleService.UpdateRule(c.Request().Context(), id, &req)
	if err != nil {
		logrus.Errorf("Error updating rule %s: %v", id, err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("Failed to update rule: %v", err)})
	}

	return c.JSON(http.StatusOK, rule)
}

// DeleteRule deletes a rule
func (h *APIHandler) DeleteRule(c echo.Context) error {
	id := c.Param("id")
	err := h.ruleService.DeleteRule(c.Request().Context(), id)
	if err != nil {
		logrus.Errorf("Error deleting rule %s: %v", id, err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("Failed to delete rule: %v", err)})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": "Rule deleted successfully"})
}

// StartRule starts a rule
func (h *APIHandler) StartRule(c echo.Context) error {
	id := c.Param("id")
	err := h.ruleService.StartRule(c.Request().Context(), id)
	if err != nil {
		logrus.Errorf("Error starting rule %s: %v", id, err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("Failed to start rule: %v", err)})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": "Rule started successfully"})
}

// StopRule stops a rule
func (h *APIHandler) StopRule(c echo.Context) error {
	id := c.Param("id")
	err := h.ruleService.StopRule(c.Request().Context(), id)
	if err != nil {
		logrus.Errorf("Error stopping rule %s: %v", id, err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("Failed to stop rule: %v", err)})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": "Rule stopped successfully"})
}

// GetAlerts returns all alerts, optionally filtered by rule ID
func (h *APIHandler) GetAlerts(c echo.Context) error {
	ruleID := c.QueryParam("rule_id")
	alerts, err := h.ruleService.GetAlerts(ruleID)
	if err != nil {
		logrus.Errorf("Error getting alerts: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to get alerts"})
	}
	return c.JSON(http.StatusOK, alerts)
}

// GetAlert returns an alert by ID
func (h *APIHandler) GetAlert(c echo.Context) error {
	id := c.Param("id")
	alert, err := h.ruleService.GetAlert(id)
	if err != nil {
		logrus.Errorf("Error getting alert %s: %v", id, err)
		return c.JSON(http.StatusNotFound, map[string]string{"error": fmt.Sprintf("Alert with ID %s not found", id)})
	}
	return c.JSON(http.StatusOK, alert)
}

// AcknowledgeAlert acknowledges an alert
func (h *APIHandler) AcknowledgeAlert(c echo.Context) error {
	id := c.Param("id")
	var req struct {
		AcknowledgedBy string `json:"acknowledged_by"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request format"})
	}

	err := h.ruleService.AcknowledgeAlert(id, req.AcknowledgedBy)
	if err != nil {
		logrus.Errorf("Error acknowledging alert %s: %v", id, err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("Failed to acknowledge alert: %v", err)})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": "Alert acknowledged successfully"})
}

// GetAlertsByTimeRange returns alerts within a specified time range
func (h *APIHandler) GetAlertsByTimeRange(c echo.Context) error {
	ruleID := c.QueryParam("rule_id")
	startTimeStr := c.QueryParam("start_time")
	endTimeStr := c.QueryParam("end_time")

	// Parse time parameters
	var startTime, endTime time.Time
	var err error

	if startTimeStr != "" {
		startTime, err = time.Parse(time.RFC3339, startTimeStr)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid start_time format"})
		}
	} else {
		// Default to 24 hours ago if not specified
		startTime = time.Now().Add(-24 * time.Hour)
	}

	if endTimeStr != "" {
		endTime, err = time.Parse(time.RFC3339, endTimeStr)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid end_time format"})
		}
	} else {
		// Default to now if not specified
		endTime = time.Now()
	}

	alerts, err := h.ruleService.GetAlertsByTimeRange(ruleID, startTime, endTime)
	if err != nil {
		logrus.Errorf("Error getting alerts by time range: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to get alerts"})
	}

	return c.JSON(http.StatusOK, alerts)
}

// SetupRoutes sets up the API routes
func (h *APIHandler) SetupRoutes(e *echo.Echo) {
	// Rule endpoints
	e.GET("/api/rules", h.GetRules)
	e.GET("/api/rules/:id", h.GetRule)
	e.POST("/api/rules", h.CreateRule)
	e.PUT("/api/rules/:id", h.UpdateRule)
	e.DELETE("/api/rules/:id", h.DeleteRule)
	e.POST("/api/rules/:id/start", h.StartRule)
	e.POST("/api/rules/:id/stop", h.StopRule)

	// Alert endpoints
	e.GET("/api/alerts", h.GetAlerts)
	e.GET("/api/alerts/by-time", h.GetAlertsByTimeRange)
	e.GET("/api/alerts/:id", h.GetAlert)
	e.GET("/api/alerts/:id/data", h.GetAlertRawData)
	e.POST("/api/alerts/:id/acknowledge", h.AcknowledgeAlert)
}
