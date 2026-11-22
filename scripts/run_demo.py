from __future__ import annotations

import argparse
import json
from pathlib import Path

from density_trace.demo import run_demo


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="artifacts/demo")
    parser.add_argument("--keep-existing", action="store_true")
    args = parser.parse_args()
    summary = run_demo(Path(args.out), overwrite=not args.keep_existing)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
