from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
AS_OF_MS = 1_767_229_200_000
CURRENT_BIN = SAMPLES / "windows" / "current-v2.bin"
CURRENT_JSONL = SAMPLES / "windows" / "current-v2.jsonl"
LEGACY_BIN = SAMPLES / "windows" / "legacy-v1.bin"
LEGACY_JSONL = SAMPLES / "windows" / "legacy-v1.jsonl"
FUTURE_BIN = SAMPLES / "windows" / "future-v3-required.bin"
FUTURE_JSONL = SAMPLES / "windows" / "future-v3-required.jsonl"
EVENTS = SAMPLES / "events" / "requests.jsonl"
OVERWRITE = SAMPLES / "conflicts" / "merge-overwrite.jsonl"
REJECT = SAMPLES / "conflicts" / "merge-reject.jsonl"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_samples_summary() -> dict:
    return json.loads((SAMPLES / "samples.json").read_text(encoding="utf-8"))


def metric_map(result: dict) -> dict[str, dict]:
    return {str(item["window_ms"]): item for item in result["windows"]}
