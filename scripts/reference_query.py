#!/usr/bin/env python3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sparse_index.cli import main


if __name__ == "__main__":
    raise SystemExit(main(["reference-query", *sys.argv[1:]]))
