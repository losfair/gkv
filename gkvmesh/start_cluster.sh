#!/bin/bash

set -eo pipefail

JAR="target/scala-3.2.0/gkvmesh-assembly-0.1.0-SNAPSHOT.jar"

function prepend() { while read line; do echo "${1} ${line}"; done; }

function startJar() {
  local prepend_text="$1"
  shift
  java $@ -jar "$JAR" | prepend $prepend_text
}

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

# \x02gkvmesh-dev-cluster-1\x00
startJar node-1 \
  --enable-preview -Dgkvmesh.meshserver.port=6201 -Dgkvmesh.httpapi.port=6301 \
  -Dgkvmesh.tkv.prefixHex=02676b766d6573682d6465762d636c75737465722d3100 &

# \x02gkvmesh-dev-cluster-2\x00
startJar node-2 \
  --enable-preview -Dgkvmesh.meshserver.port=6202 -Dgkvmesh.httpapi.port=6302 \
  -Dgkvmesh.tkv.prefixHex=02676b766d6573682d6465762d636c75737465722d3200 &

wait -n
