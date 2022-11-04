#!/bin/bash

set -eo pipefail

JAR="target/scala-3.2.0/gkvmesh-assembly-0.1.0-SNAPSHOT.jar"
JVM_ARGS="--enable-preview -XX:+UseZGC -Xmx4G -XX:+UseLargePages"
#JVM_ARGS="--enable-preview"

function prepend() { while read line; do echo "${1} ${line}"; done; }

function startJar() {
  local prepend_text="$1"
  shift
  java $JVM_ARGS $@ -jar "$JAR" | prepend $prepend_text
}

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

# \x02gkvmesh-dev-cluster-1\x00
startJar node-1 \
  -Dgkvmesh.meshserver.port=6201 -Dgkvmesh.httpapi.port=6301 \
  -Dgkvmesh.log.localLevel=info \
  -Dgkvmesh.tkv.prefixHex=02676b766d6573682d6465762d636c75737465722d3100 &

# \x02gkvmesh-dev-cluster-2\x00
startJar node-2 \
  -Dgkvmesh.meshserver.port=6202 -Dgkvmesh.httpapi.port=6302 \
  -Dgkvmesh.log.localLevel=info \
  -Dgkvmesh.tkv.prefixHex=02676b766d6573682d6465762d636c75737465722d3200 &

wait -n
