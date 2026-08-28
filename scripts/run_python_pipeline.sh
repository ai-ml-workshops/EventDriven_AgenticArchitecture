#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="${DIR}:${PYTHONPATH}"

echo "Starting Python Event-Driven Agentic Pipeline..."
python3 -m python.src.pipeline_runner "$@"
