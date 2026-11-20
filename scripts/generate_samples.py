#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

def main() -> None:
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    from ree.sample_data import generate_samples

    summary = generate_samples(root)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
