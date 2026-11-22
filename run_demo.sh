#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")"
python3 -m blockgemm generate --out samples --force
python3 -m blockgemm demo --manifest samples/manifest.json --out runs/demo
