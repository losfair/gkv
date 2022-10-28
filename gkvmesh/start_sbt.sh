#!/bin/bash

export JAVA_OPTS="--enable-preview "

JAVA_OPTS+="-Dgkvmesh.meshserver.port=6200 "
JAVA_OPTS+="-Dgkvmesh.tkv.prefixHex=02676b766d6573682d64657600 " # \x02gkvmesh-dev\x00

exec sbt
