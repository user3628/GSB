from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from image_workbench.demo_data import generate_demo_data

if __name__ == "__main__":
    print(json.dumps(generate_demo_data(ROOT / "data"), ensure_ascii=False, indent=2, allow_nan=False))

