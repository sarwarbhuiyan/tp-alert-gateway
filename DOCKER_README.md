# Timeplus Alert Gateway - Docker Setup

This setup allows you to quickly test the Timeplus Alert Gateway with simulated device temperature data.

## Components

1. **Timeplus Proton** - The streaming database engine
2. **Alert Gateway** - RESTful API for managing alert rules and notifications  
3. **Data Simulator** - Generates random device temperature readings

## Running with Docker Compose

```bash
# Build and start all services
docker-compose up --build

# In a separate terminal, you can check logs for each service
docker-compose logs -f alert-gateway
docker-compose logs -f data-simulator

# Stop all services
docker-compose down
```

## Accessing Services

- **Timeplus Proton UI**: http://localhost:3218
  - Username: `default`
  - Password: `tp-alert-demo`

- **Alert Gateway API**: http://localhost:8080
  - API Endpoints: See README.md for full API reference

## Simulated Data

The data simulator generates temperature readings from multiple devices with values around 20-25°C. 
Occasionally, anomalies will be generated with temperature spikes to trigger alerts.

## Pre-configured Alert Rules

The simulator automatically creates the following alert rules:

1. **High Temperature Alert** - Triggers when any device temperature exceeds 30°C
2. **Device 1 Temperature Alert** - Triggers when device_1 temperature exceeds 25°C
3. **Low Temperature Alert** - Triggers when any device temperature drops below 19°C

## Testing the System

1. Access alerts in a specific time range:
   ```
   curl "http://localhost:8080/api/alerts/timerange?start=2023-01-01T00:00:00Z"
   ```

2. View all rules:
   ```
   curl http://localhost:8080/api/rules
   ```

3. View alerts for a specific rule:
   ```
   curl http://localhost:8080/api/rules/{rule-id}/alerts
   ```

4. Acknowledge an alert:
   ```
   curl -X POST -H "Content-Type: application/json" \
     -d '{"acknowledgedBy":"admin"}' \
     http://localhost:8080/api/alerts/{alert-id}/acknowledge
   ```

## Notification Stream

All alerts are written to a stream named `tp_alerts` in Timeplus. You can query this stream in the Timeplus UI:

```sql
SELECT * FROM tp_alerts
``` 