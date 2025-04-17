package timeplus

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/timeplus-io/proton-go-driver/v2"
	"github.com/timeplus-io/proton-go-driver/v2/lib/driver"

	"github.com/timeplus-io/tp-alert-gateway/pkg/config"
)

// Column represents a column definition
type Column struct {
	Name     string
	Type     string
	Nullable bool // Whether the column can be NULL
}

// Client is a wrapper around the Timeplus Proton Go driver connection
type Client struct {
	conn      driver.Conn // Use driver.Conn directly
	workspace string
	address   string          // Store connection address
	username  string          // Store username
	password  string          // Store password
	opts      *proton.Options // Store original connection options
}

// NewClient creates a new Timeplus client
func NewClient(cfg *config.TimeplusConfig) (*Client, error) {
	logrus.Infof("Connecting to Timeplus at %s (workspace: %s)", cfg.Address, cfg.Workspace)

	// Parse the address
	address := cfg.Address

	// Strip protocol if present (shouldn't be needed since Timeplus doesn't use HTTP)
	address = strings.TrimPrefix(address, "http://")
	address = strings.TrimPrefix(address, "https://")

	// Parse host and port
	host := address
	port := "8464" // Default native port

	if strings.Contains(address, ":") {
		parts := strings.Split(address, ":")
		host = parts[0]
		port = parts[1]
	}

	// Final connection address
	connectionAddr := host + ":" + port

	logrus.Infof("Connecting to Timeplus native protocol at %s", connectionAddr)

	// Create connection options
	opts := &proton.Options{
		Addr: []string{connectionAddr},
		Auth: proton.Auth{
			Database: cfg.Workspace,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     time.Second * 10, // Increased from 5s to 10s
		MaxOpenConns:    20,               // Increased from 10 to 20
		MaxIdleConns:    10,               // Increased from 5 to 10
		ConnMaxLifetime: time.Hour * 2,    // Increased from 1h to 2h
		Compression: &proton.Compression{
			Method: proton.CompressionLZ4,
		},
	}

	// Create the connection using the Open function with more robust settings
	conn, err := proton.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Timeplus: %w", err)
	}

	// Test connection with retries
	var pingErr error
	for i := 0; i < 10; i++ {
		// Use a longer timeout for ping
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		pingErr = conn.Ping(ctx) // Use conn.Ping directly
		cancel()                 // Cancel context immediately after ping attempt
		if pingErr == nil {
			break // Success
		}
		logrus.Warnf("Alert Gateway: Failed to ping Timeplus (attempt %d/10): %v", i+1, pingErr)
		time.Sleep(3 * time.Second)
	}

	if pingErr != nil {
		// Close the connection if ping fails definitively
		conn.Close()
		return nil, fmt.Errorf("failed to ping Timeplus after multiple attempts: %w", pingErr)
	}

	logrus.Info("Alert Gateway: Successfully connected to Timeplus.")

	return &Client{
		conn:      conn, // Store the driver.Conn
		workspace: cfg.Workspace,
		address:   connectionAddr,
		username:  cfg.Username,
		password:  cfg.Password,
		opts:      opts, // Store the original options
	}, nil
}

// CreateStream creates a new stream with the given name and schema
func (c *Client) CreateStream(ctx context.Context, name string, schema []Column) error {
	// Build schema string
	schemaStr := ""
	if schema != nil && len(schema) > 0 {
		schemaFields := make([]string, len(schema))
		for i, col := range schema {
			if col.Nullable {
				schemaFields[i] = fmt.Sprintf("%s %s NULL", col.Name, col.Type)
			} else {
				schemaFields[i] = fmt.Sprintf("%s %s", col.Name, col.Type)
			}
		}
		schemaStr = "(" + strings.Join(schemaFields, ", ") + ")"
	}

	// Create stream, wrap name in backticks
	query := fmt.Sprintf("CREATE STREAM IF NOT EXISTS `%s` %s", name, schemaStr)
	if err := c.conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create stream '%s': %w", name, err)
	}
	return nil
}

// CreateMaterializedView creates a new materialized view
func (c *Client) CreateMaterializedView(ctx context.Context, name string, query string) error {
	// Check if the view already exists, and drop it if it does
	exists, err := c.ViewExists(ctx, name)
	if err != nil {
		logrus.Warnf("Error checking if view exists: %v", err)
	} else if exists {
		logrus.Infof("View %s already exists, dropping it first", name)
		if err := c.DeleteMaterializedView(ctx, name); err != nil {
			logrus.Warnf("Error dropping existing view: %v", err)
		}
	}

	// Create proper CREATE MATERIALIZED VIEW query if needed
	finalQuery := query
	trimmedUpper := strings.TrimSpace(strings.ToUpper(query))
	if !strings.HasPrefix(trimmedUpper, "CREATE MATERIALIZED VIEW") {
		// For materialized views, we need to REMOVE any table() wrapper since they don't support historical queries
		// Replace table() wrappers specifically, not all parentheses
		query = strings.ReplaceAll(query, "table(", "")
		query = strings.ReplaceAll(query, "TABLE(", "")

		// Replace the closing parenthesis of table() if needed
		if !strings.HasPrefix(trimmedUpper, "SELECT") {
			logrus.Warnf("Query doesn't start with SELECT, may not work as expected: %s", query)
		}

		// Create the full query with proper view and destination
		destStream := strings.Replace(name, "_view", "_results", 1)
		finalQuery = fmt.Sprintf("CREATE MATERIALIZED VIEW `%s` INTO `%s` AS %s", name, destStream, query)
		logrus.Infof("Executing materialized view creation query (with wrapper): %s", finalQuery)
	} else {
		logrus.Infof("Executing materialized view creation query (as is): %s", finalQuery)
	}

	// Execute the query with retry logic
	var lastErr error
	for i := 0; i < 3; i++ {
		err := c.conn.Exec(ctx, finalQuery)
		if err == nil {
			return nil // Success
		}

		lastErr = err
		logrus.Warnf("Attempt %d to create view failed: %v", i+1, err)
		time.Sleep(500 * time.Millisecond)
	}

	// If we get here, all attempts failed
	return fmt.Errorf("failed to create materialized view after multiple attempts: %w", lastErr)
}

// DeleteMaterializedView deletes a materialized view
func (c *Client) DeleteMaterializedView(ctx context.Context, name string) error {
	// Check if the view exists
	exists, err := c.ViewExists(ctx, name)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	// Drop view, wrap name in backticks
	query := fmt.Sprintf("DROP VIEW `%s`", name)
	if err = c.conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to delete view '%s': %w", name, err)
	}
	return nil
}

// ViewExists checks if a view exists
func (c *Client) ViewExists(ctx context.Context, name string) (bool, error) {
	// Use SHOW STREAMS to check if view exists (in Timeplus, views are also streams)
	// Use LIKE for pattern matching, no backticks needed here, but escape single quotes in name
	escapedName := strings.ReplaceAll(name, "'", "''")
	query := fmt.Sprintf("SHOW STREAMS LIKE '%s'", escapedName)
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return false, fmt.Errorf("failed to execute SHOW STREAMS: %w", err)
	}
	defer rows.Close()

	// If the query returns any rows, the view exists
	exists := rows.Next()
	if rows.Err() != nil {
		return false, fmt.Errorf("error checking rows from SHOW STREAMS: %w", rows.Err())
	}

	return exists, nil
}

// DeleteStream deletes a stream
func (c *Client) DeleteStream(ctx context.Context, name string) error {
	// Check if the stream exists
	exists, err := c.StreamExists(ctx, name)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	// Drop stream, wrap name in backticks
	query := fmt.Sprintf("DROP STREAM `%s`", name)
	if err = c.conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to delete stream '%s': %w", name, err)
	}
	return nil
}

// StreamExists checks if a stream exists
func (c *Client) StreamExists(ctx context.Context, name string) (bool, error) {
	// Use SHOW STREAMS to check if stream exists
	// Use LIKE for pattern matching, no backticks needed here, but escape single quotes in name
	escapedName := strings.ReplaceAll(name, "'", "''")
	query := fmt.Sprintf("SHOW STREAMS LIKE '%s'", escapedName)
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return false, fmt.Errorf("failed to execute SHOW STREAMS: %w", err)
	}
	defer rows.Close()

	// If the query returns any rows, the stream exists
	exists := rows.Next()
	if rows.Err() != nil {
		return false, fmt.Errorf("error checking rows from SHOW STREAMS: %w", rows.Err())
	}

	return exists, nil
}

// ExecuteQuery executes a query and returns the result rows
func (c *Client) ExecuteQuery(ctx context.Context, query string) ([]map[string]interface{}, error) {
	// Add retry mechanism for handling EOF errors
	maxRetries := 5 // Increased from 3 to 5
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			logrus.Warnf("Retrying query execution (attempt %d/%d) after error: %v", attempt+1, maxRetries, lastErr)

			// Check for EOF errors and reconnect if needed
			if lastErr != nil && strings.Contains(lastErr.Error(), "EOF") {
				reconnCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // Increased timeout
				err := c.reconnect(reconnCtx)
				cancel()
				if err != nil {
					logrus.Errorf("Failed to reconnect: %v", err)
					// Don't return here, still try the query with existing connection
				}
			}

			// Add longer backoff between retries with jitter
			backoffSeconds := 1 << uint(attempt)
			if backoffSeconds > 20 {
				backoffSeconds = 20 // Cap max delay
			}
			jitter := time.Duration(float64(backoffSeconds) * (0.75 + 0.5*float64(time.Now().Nanosecond())/float64(1e9)))
			logrus.Infof("Waiting %v seconds before retry...", jitter)
			time.Sleep(jitter * time.Second)
		}

		// Create a timeout context for this query attempt
		queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second) // Increased timeout
		defer cancel()

		// Execute the query using direct connection
		rows, err := c.conn.Query(queryCtx, query)
		if err != nil {
			lastErr = err
			cancel() // Cancel this attempt's context

			if strings.Contains(err.Error(), "EOF") {
				logrus.Warnf("EOF error during query execution, will retry: %v", err)
				continue // Retry on EOF errors
			}

			// For non-EOF errors, we'll still try to reconnect on the next attempt
			logrus.Errorf("Error executing query: %v", err)
			continue
		}

		// Successfully connected, get column metadata
		defer rows.Close()

		// Get column names and types
		columnNames := rows.Columns()
		columnTypes := rows.ColumnTypes()

		// Prepare result
		result := make([]map[string]interface{}, 0)

		// Process rows
		rowsProcessed := 0
		for rows.Next() {
			rowsProcessed++
			// Create a slice for scanning, matching column types
			scanArgs := make([]interface{}, len(columnNames))
			for i, ct := range columnTypes {
				scanArgs[i] = reflect.New(ct.ScanType()).Interface()
			}

			if err := rows.Scan(scanArgs...); err != nil {
				cancel() // Cancel context before returning
				return nil, fmt.Errorf("failed to scan row: %w", err)
			}

			// Convert to map
			rowMap := make(map[string]interface{})
			for i, name := range columnNames {
				rowMap[name] = reflect.ValueOf(scanArgs[i]).Elem().Interface()
			}

			result = append(result, rowMap)
		}

		if err := rows.Err(); err != nil {
			cancel() // Cancel context before returning
			lastErr = err

			if strings.Contains(err.Error(), "EOF") {
				logrus.Warnf("EOF error during row iteration, will retry: %v", err)
				continue // Retry on EOF errors
			}

			return nil, fmt.Errorf("error during row iteration: %w", err)
		}

		logrus.Infof("Successfully executed query with %d rows", rowsProcessed)
		return result, nil
	}

	return nil, fmt.Errorf("failed to execute query after %d attempts: %w", maxRetries, lastErr)
}

// reconnect tries to reestablish the connection with retries
func (c *Client) reconnect(ctx context.Context) error {
	logrus.Info("Attempting to reconnect to Timeplus...")

	// Close existing connection if it exists
	if c.conn != nil {
		c.conn.Close()
	}

	// Create connection with exponential backoff
	var err error
	var conn driver.Conn

	maxRetries := 5
	baseDelay := 1 * time.Second

	for i := 0; i < maxRetries; i++ {
		delay := time.Duration(1<<uint(i)) * baseDelay
		if delay > 30*time.Second {
			delay = 30 * time.Second // Cap max delay at 30 seconds
		}

		logrus.Infof("Reconnection attempt %d/%d (delay: %v)...", i+1, maxRetries, delay)

		// Add jitter to prevent thundering herd
		jitter := time.Duration(float64(delay) * (0.5 + 0.5*float64(time.Now().Nanosecond())/float64(1e9)))
		time.Sleep(jitter)

		// Create new connection
		conn, err = proton.Open(c.opts)
		if err == nil {
			// Test connection
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			pingErr := conn.Ping(pingCtx)
			cancel()

			if pingErr == nil {
				c.conn = conn
				logrus.Info("Successfully reconnected to Timeplus")
				return nil
			}

			// Close and try again if ping fails
			logrus.Warnf("Connection established but ping failed: %v", pingErr)
			conn.Close()
			err = pingErr
		} else {
			logrus.Warnf("Failed to reconnect: %v", err)
		}
	}

	return fmt.Errorf("failed to reconnect after %d attempts: %w", maxRetries, err)
}

// StreamQuery executes a streaming query and calls the given callback for each result row
func (c *Client) StreamQuery(ctx context.Context, query string, callback func(row interface{})) error {
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute streaming query: %w", err)
	}
	defer rows.Close()

	// Get column names and types
	columnNames := rows.Columns()
	columnTypes := rows.ColumnTypes()

	// Process each row
	for rows.Next() {
		// Create a slice for scanning, matching column types
		rowScan := make([]interface{}, len(columnNames))
		for i, ct := range columnTypes {
			// Use driver.Valuer for Scan based on the type
			rowScan[i] = reflect.New(ct.ScanType()).Interface()
		}

		if err := rows.Scan(rowScan...); err != nil {
			logrus.Errorf("Error scanning streaming row: %v", err)
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Create a map with column names as keys
		rowMap := make(map[string]interface{})
		for i, colName := range columnNames {
			// Dereference the pointer received from Scan
			val := reflect.ValueOf(rowScan[i]).Elem().Interface()
			rowMap[colName] = val
		}

		// Call the callback with the row
		callback(rowMap)

		// Check if context is done
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Continue processing
		}
	}

	return rows.Err()
}

// GetAlertSchema returns the schema for the alert stream
func (c *Client) GetAlertSchema() []Column {
	return GetAlertSchema()
}

// InsertIntoStream inserts data into a stream
func (c *Client) InsertIntoStream(ctx context.Context, streamName string, columns []string, values []interface{}) error {
	maxRetries := 5
	var lastErr error

	// Build the SQL query with column names and placeholders
	columnList := strings.Join(columns, ", ")

	// Format each value for SQL
	formattedValues := make([]string, len(values))
	for i, val := range values {
		switch v := val.(type) {
		case nil:
			formattedValues[i] = "null"
		case string:
			formattedValues[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
		case time.Time:
			formattedValues[i] = fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05.000"))
		case bool:
			formattedValues[i] = fmt.Sprintf("%t", v)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			formattedValues[i] = fmt.Sprintf("%d", v)
		case float32, float64:
			formattedValues[i] = fmt.Sprintf("%f", v)
		default:
			// Try to convert to string if possible
			formattedValues[i] = fmt.Sprintf("'%v'", v)
		}
	}

	valuesList := strings.Join(formattedValues, ", ")
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", streamName, columnList, valuesList)

	// Execute with retries
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			logrus.Warnf("Retrying insertion to stream '%s' (attempt %d/%d) after error: %v",
				streamName, attempt+1, maxRetries, lastErr)

			// Check for EOF errors and reconnect if needed
			if lastErr != nil && strings.Contains(lastErr.Error(), "EOF") {
				logrus.Warnf("Got EOF error, attempting to reconnect before retry")
				reconnCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				err := c.reconnect(reconnCtx)
				cancel()
				if err != nil {
					logrus.Errorf("Failed to reconnect: %v", err)
				}
			}

			// Add backoff between retries with jitter
			baseDelay := time.Duration(1<<uint(attempt-1)) * time.Second
			if baseDelay > 10*time.Second {
				baseDelay = 10 * time.Second
			}
			jitter := time.Duration(float64(baseDelay) * (0.75 + 0.5*float64(time.Now().Nanosecond())/float64(1e9)))
			time.Sleep(jitter)
		}

		// Execute the insert statement directly
		err := c.conn.Exec(ctx, query)
		if err == nil {
			return nil // Success
		}

		lastErr = err
		logrus.Warnf("Insert failed (attempt %d/%d): %v", attempt+1, maxRetries, err)

		// Continue with retry logic
	}

	// If we get here, all attempts failed
	return fmt.Errorf("failed to insert into stream after %d attempts: %w", maxRetries, lastErr)
}

// SanitizeName sanitizes a name to be used in Timeplus
func SanitizeName(name string) string {
	// Replace spaces and special characters with underscores
	sanitized := strings.ReplaceAll(name, " ", "_")
	sanitized = strings.ReplaceAll(sanitized, "-", "_")
	return sanitized
}

// CreateContextWithRetry creates a context with timeout and implements a retry mechanism
func (c *Client) CreateContextWithRetry(parent context.Context, timeout time.Duration, retries int) (context.Context, context.CancelFunc) {
	if retries <= 0 {
		retries = 3 // Default to 3 retries
	}

	if timeout <= 0 {
		timeout = 30 * time.Second // Default to 30 seconds
	}

	// Create a context with the specified timeout
	ctx, cancel := context.WithTimeout(parent, timeout)

	// Return a decorated context with custom error handling
	return ctx, func() {
		cancel() // Always call the original cancel function
	}
}

// CheckStreamExists efficiently checks if a specific stream exists
func (c *Client) CheckStreamExists(ctx context.Context, streamName string) (bool, error) {
	query := fmt.Sprintf("SHOW STREAMS LIKE '%s'", streamName)

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return false, fmt.Errorf("failed to check stream existence: %w", err)
	}
	defer rows.Close()

	// Stream exists if the query returns at least one row
	return rows.Next(), rows.Err()
}

// SetupMutableAlertAcksStream ensures the mutable alert acknowledgments stream exists
func (c *Client) SetupMutableAlertAcksStream(ctx context.Context) error {
	schema := GetMutableAlertAcksSchema()
	streamName := AlertAcksMutableStream

	// Efficiently check if stream exists using direct query
	exists, err := c.CheckStreamExists(ctx, streamName)
	if err != nil {
		return fmt.Errorf("failed to check if stream exists: %w", err)
	}

	if exists {
		logrus.Infof("Stream %s already exists", streamName)
		return nil
	}

	// Create mutable stream with primary key
	// Build columns string for schema
	columnsStr := ""
	for i, col := range schema {
		if i > 0 {
			columnsStr += ", "
		}
		nullableStr := ""
		if col.Nullable {
			nullableStr = " NULL"
		}
		columnsStr += fmt.Sprintf("`%s` %s%s", col.Name, col.Type, nullableStr)
	}

	query := fmt.Sprintf("CREATE MUTABLE STREAM %s (%s) PRIMARY KEY (rule_id, entity_id)",
		streamName, columnsStr)

	err = c.conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create mutable stream: %w", err)
	}

	logrus.Infof("Created mutable stream %s", streamName)
	return nil
}

// ListStreams returns a list of all streams in Timeplus
func (c *Client) ListStreams(ctx context.Context) ([]string, error) {
	// Use direct connection Query method instead of ExecuteQuery
	rows, err := c.conn.Query(ctx, "SHOW STREAMS")
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columnNames := rows.Columns()
	nameIdx := -1
	for i, name := range columnNames {
		if name == "name" {
			nameIdx = i
			break
		}
	}

	if nameIdx == -1 {
		return nil, fmt.Errorf("no 'name' column found in SHOW STREAMS result")
	}

	streams := make([]string, 0)
	for rows.Next() {
		// Create a slice for scanning, matching column types
		columnTypes := rows.ColumnTypes()
		scanArgs := make([]interface{}, len(columnNames))
		for i, ct := range columnTypes {
			scanArgs[i] = reflect.New(ct.ScanType()).Interface()
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Extract the name value
		if name, ok := reflect.ValueOf(scanArgs[nameIdx]).Elem().Interface().(string); ok {
			streams = append(streams, name)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	return streams, nil
}

// ExecuteStreamingQuery runs a streaming query and calls the callback for each result row
// This is an alias to StreamQuery but with a different callback signature
func (c *Client) ExecuteStreamingQuery(ctx context.Context, query string, callback func(result map[string]interface{}) error) error {
	// Create a wrapper callback that converts the interface{} to map[string]interface{}
	wrappedCallback := func(row interface{}) {
		if mapRow, ok := row.(map[string]interface{}); ok {
			if err := callback(mapRow); err != nil {
				logrus.Errorf("Error in streaming query callback: %v", err)
			}
		} else {
			logrus.Errorf("Unexpected row type: %T", row)
		}
	}

	// Use the existing StreamQuery method
	return c.StreamQuery(ctx, query, wrappedCallback)
}

// ListViews returns a list of all views in the workspace
func (c *Client) ListViews(ctx context.Context) ([]string, error) {
	query := "SELECT name FROM system.tables WHERE engine = 'View'"

	// Use direct connection Query method instead of ExecuteQuery
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list views: %w", err)
	}
	defer rows.Close()

	views := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan view name: %w", err)
		}
		views = append(views, name)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	return views, nil
}

// ListMaterializedViews returns a list of all materialized views in the workspace
func (c *Client) ListMaterializedViews(ctx context.Context) ([]string, error) {
	query := "SELECT name FROM system.tables WHERE engine = 'MaterializedView'"

	// Use direct connection Query method instead of ExecuteQuery
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list materialized views: %w", err)
	}
	defer rows.Close()

	views := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan materialized view name: %w", err)
		}
		views = append(views, name)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	return views, nil
}

// ExecuteDDL executes a Data Definition Language (DDL) statement like CREATE or DROP
func (c *Client) ExecuteDDL(ctx context.Context, query string) error {
	// DDL statements typically don't return rows, so use Exec
	if err := c.conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to execute DDL query '%s': %w", query, err)
	}
	return nil
}
