#!/bin/bash

set -e

key="$1"

if [ -z "$key" ]; then
  echo "Usage: $0 <key>"
  exit 1
fi

if [ -z "$API_URL" ]; then
  API_URL="http://localhost:6300"
fi

keyB64=$(echo -n "$key" | base64)

curl -f -s -X POST -H "Content-Type: application/json" -d "{\"key\": \"$keyB64\"}" \
  "$API_URL/data/get" | jq -r '(.value // "") | @base64d'
