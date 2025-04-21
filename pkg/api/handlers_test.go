package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timeplus-io/tp-alert-gateway/pkg/models"
	"github.com/timeplus-io/tp-alert-gateway/pkg/services"
	"github.com/timeplus-io/tp-alert-gateway/pkg/timeplus"
)

// setupTestRouter creates a test router with the provided client
func setupTestRouter(client *timeplus.Client) (*echo.Echo, error) {
	e := echo.New()
	ruleService, err := services.NewRuleService(client)
	if err != nil {
		return nil, err
	}
	handler := NewAPIHandler(ruleService)
	handler.SetupRoutes(e)
	return e, nil
}

// TestCreateRule tests the rule creation endpoint
func TestCreateRule(t *testing.T) {
	// Create a mock client
	mockClient := &timeplus.Client{} // In a real test, you'd use a mock implementation

	router, err := setupTestRouter(mockClient)
	require.NoError(t, err)

	tests := []struct {
		name       string
		rule       models.CreateRuleRequest
		wantStatus int
	}{
		{
			name: "valid rule",
			rule: models.CreateRuleRequest{
				Name:        "Test Rule",
				Description: "Test Description",
				Query:       "SELECT * FROM network_logs WHERE status_code >= 500",
				Severity:    "critical",
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "invalid rule - missing name",
			rule: models.CreateRuleRequest{
				Description: "Test Description",
				Query:       "SELECT * FROM network_logs",
				Severity:    "critical",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert rule to JSON
			jsonData, err := json.Marshal(tt.rule)
			require.NoError(t, err)

			// Create request
			req := httptest.NewRequest(http.MethodPost, "/api/rules", bytes.NewBuffer(jsonData))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()

			// Perform request
			router.ServeHTTP(rec, req)

			// Check status code
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusCreated {
				// Parse response
				var response models.Rule
				err = json.Unmarshal(rec.Body.Bytes(), &response)
				assert.NoError(t, err)
				assert.NotEmpty(t, response.ID)
				assert.Equal(t, tt.rule.Name, response.Name)
				assert.Equal(t, tt.rule.Description, response.Description)
				assert.Equal(t, tt.rule.Query, response.Query)
				assert.Equal(t, tt.rule.Severity, response.Severity)
			}
		})
	}
}

// TestAcknowledgeAlert tests the alert acknowledgment endpoint
func TestAcknowledgeAlert(t *testing.T) {
	// Create a mock client
	mockClient := &timeplus.Client{} // In a real test, you'd use a mock implementation

	router, err := setupTestRouter(mockClient)
	require.NoError(t, err)

	tests := []struct {
		name       string
		ruleID     string
		entityID   string
		wantStatus int
	}{
		{
			name:       "valid acknowledgment",
			ruleID:     "test-rule-id",
			entityID:   "test-entity-id",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid rule ID",
			ruleID:     "",
			entityID:   "test-entity-id",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request body
			body := map[string]string{
				"entity_id": tt.entityID,
			}
			jsonData, err := json.Marshal(body)
			require.NoError(t, err)

			// Create request
			req := httptest.NewRequest(http.MethodPost, "/api/rules/"+tt.ruleID+"/acknowledge", bytes.NewBuffer(jsonData))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()

			// Perform request
			router.ServeHTTP(rec, req)

			// Check status code
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestGetRule tests the rule retrieval endpoint
func TestGetRule(t *testing.T) {
	// Create a mock client
	mockClient := &timeplus.Client{} // In a real test, you'd use a mock implementation

	router, err := setupTestRouter(mockClient)
	require.NoError(t, err)

	tests := []struct {
		name       string
		ruleID     string
		wantStatus int
	}{
		{
			name:       "valid rule ID",
			ruleID:     "test-rule-id",
			wantStatus: http.StatusOK,
		},
		{
			name:       "non-existent rule",
			ruleID:     "non-existent-id",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request
			req := httptest.NewRequest(http.MethodGet, "/api/rules/"+tt.ruleID, nil)
			rec := httptest.NewRecorder()

			// Perform request
			router.ServeHTTP(rec, req)

			// Check status code
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				// Parse response
				var response models.Rule
				err := json.Unmarshal(rec.Body.Bytes(), &response)
				assert.NoError(t, err)
				assert.Equal(t, tt.ruleID, response.ID)
			}
		})
	}
}
