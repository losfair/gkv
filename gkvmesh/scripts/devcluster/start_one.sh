#!/bin/bash

set -eo pipefail

cd ../..

echo "$@"

JAR="target/scala-3.2.0/gkvmesh-assembly-0.1.0-SNAPSHOT.jar"
JVM_ARGS="--enable-preview -XX:+UseZGC -Xms1G -Xmx1G -XX:+UseLargePages -Xlog:gc"
exec java $JVM_ARGS $@ -jar "$JAR"
