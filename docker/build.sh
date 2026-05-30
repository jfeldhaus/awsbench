#!/usr/bin/env bash
set -euo pipefail
set -x

docker build \
    -f "$(dirname "$0")/Dockerfile" \
    -t awsbench:latest \
    "$(dirname "$0")/.."
