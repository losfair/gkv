#!/bin/bash

set -e

start="$1"
end="$2"
limit="$3"

if [[ -z "$start" || -z "$end" || -z "$limit" ]]; then
  echo "Usage: $0 <start> <end> <limit>"
  exit 1
fi

if [ -z "$API_URL" ]; then
  API_URL="http://localhost:6300"
fi

startB64=$(echo -n "$start" | base64)
endB64=$(echo -n "$end" | base64)

curl -f -s -X POST -H "Content-Type: application/json" -d "{\"start\": \"$startB64\", \"end\": \"$endB64\", \"limit\": $limit}" \
  "$API_URL/data/list" | jq -r '.entries | map_values((.key | @base64d) + " -> " + (.value | @base64d)) | .[]'
