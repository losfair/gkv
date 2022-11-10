#!/bin/bash

set -e

startB64='' # empty
endB64='/w==' # \xff

if [ -z "$API_URL" ]; then
  API_URL="http://localhost:6300"
fi

# infinite loop
while true; do
  # get all keys
  output=`curl -f -s -X POST -H "Content-Type: application/json" -d "{\"start\": \"$startB64\", \"end\": \"$endB64\", \"limit\": 100}" "$API_URL/data/list"`
  lastKey=`echo "$output" | jq -r '(.entries | last).key'`
  echo "$output" | jq '.entries[]'
  if [ "$lastKey" == "null" ]; then
    break
  fi
  cursorHex=`echo "$lastKey" | base64 -d | od -t x1 -An | tr -d ' '`00
  startB64=`echo "$cursorHex" | xxd -r -p | base64 -w 0`
done
