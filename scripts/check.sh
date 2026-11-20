#!/usr/bin/env sh
set -eu
ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"
python3 -m sqlplan_engine init
python3 -m unittest discover -v
python3 -m sqlplan_engine demo

