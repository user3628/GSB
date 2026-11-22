"""为 samples 目录下的整份快照回填确定性 SHA-256 摘要。

用法： python tools/fill_checksums.py
摘要只覆盖 version / generated_at / resources，与 graph_bench.canonical 保持一致。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from graph_bench.canonical import snapshot_digest

SAMPLES = ROOT / "samples"


def main() -> None:
    for path in sorted(SAMPLES.glob("snapshot_*.json")):
        if "bad_checksum" in path.name:
            # 该样本故意保留错误摘要，用于验证 E_SNAPSHOT_INVALID。
            continue
        raw = json.loads(path.read_text(encoding="utf-8"))
        if "resources" not in raw:
            continue
        checksum = snapshot_digest(raw["version"], raw["generated_at"], raw["resources"])
        raw["checksum"] = checksum
        path.write_text(json.dumps(raw, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"{path.name}: {checksum}")


if __name__ == "__main__":
    main()
