#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gradsync.samples import build_sample_manifest, write_sample_manifest  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate deterministic local gradient samples")
    parser.add_argument("--output", default=str(ROOT / "data" / "samples.json"))
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    output = Path(args.output)
    expected = json.dumps(build_sample_manifest(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if args.check:
        if not output.exists():
            print(f"missing sample manifest: {output}")
            return 1
        actual = output.read_text(encoding="utf-8")
        if actual != expected:
            print(f"sample manifest differs: {output}")
            return 1
        print(f"sample manifest is deterministic: {output}")
        return 0
    write_sample_manifest(output)
    print(f"wrote deterministic sample manifest: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

