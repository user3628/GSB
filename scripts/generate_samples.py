from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mv_workbench.samples import write_samples  # noqa: E402


def main() -> int:
    written = write_samples(ROOT / "samples")
    for filename in sorted(written):
        print(f"{filename} {written[filename]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
