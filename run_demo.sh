#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python -m telemetry_align demo --output-root demo

