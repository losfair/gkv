#!/bin/bash

set -e

key="$1"
value="$2"

if [[ -z "$key" || -z "$value" ]]; then
  echo "Usage: $0 <key> <value>"
  exit 1
fi

if [ -z "$API_URL" ]; then
  API_URL="http://localhost:6300"
fi

keyB64=$(echo -n "$key" | base64 -w 0)
valueB64=$(echo -n "$value" | base64 -w 0)

curl -f -s -X POST -H "Content-Type: application/json" -d "{\"key\": \"$keyB64\", \"value\": \"$valueB64\"}" \
  "$API_URL/data/set"

echo ""
