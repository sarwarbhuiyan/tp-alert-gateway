#!/bin/bash
# Get all rule IDs
RULE_IDS=$(curl -s http://localhost:8081/api/rules | jq -r '.[].id')

# Delete each rule
for id in $RULE_IDS; do
  echo "Deleting rule: $id"
  curl -s -X DELETE http://localhost:8081/api/rules/$id
  echo
  sleep 0.5
done 