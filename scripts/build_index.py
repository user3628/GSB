#!/usr/bin/env python3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sparse_index.cli import main


if __name__ == "__main__":
    arguments = sys.argv[1:]
    commands = {"build", "status", "query", "reference-query", "verify", "serve"}
    if not any(argument in commands for argument in arguments):
        arguments = ["build", *arguments]
    raise SystemExit(main(arguments))
