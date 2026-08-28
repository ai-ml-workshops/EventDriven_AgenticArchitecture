#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${DIR}/java"

echo "Building and Running Java Event-Driven Agentic Pipeline..."
mvn compile exec:java -Dexec.mainClass="com.aiml.eventdriven.PipelineApplication" -Dexec.args="$*"
