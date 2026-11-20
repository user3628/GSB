#!/usr/bin/env python3
"""Generate the deterministic local demo workspace."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from conv_workbench.samples import generate_demo_workspace


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="demo_workspace", help="destination workspace directory")
    parser.add_argument("--force", action="store_true", help="replace existing workspace contents")
    args = parser.parse_args()
    result = generate_demo_workspace(args.root, force=args.force)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

