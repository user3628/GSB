#!/usr/bin/env python3
from __future__ import annotations

import copy
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config import load_config
from app.crash import CrashController
from app.engine import WindowEngine
from app.persistence import SNAPSHOT_FORMAT, checksum_for_snapshot


SAMPLES = ROOT / "samples"
BUILD_DIR = ROOT / "artifacts" / "sample-snapshot-build"
SAMPLE_VERSION = 100
FIXED_CREATED_AT = "2026-01-01T00:00:00.000+00:00"


def main() -> int:
    shutil.rmtree(BUILD_DIR, ignore_errors=True)
    config = load_config(SAMPLES / "config.json")
    engine = WindowEngine(
        config=config,
        runtime_dir=BUILD_DIR / "runtime",
        crash=CrashController(),
    )
    phase = json.loads((SAMPLES / "phase1.json").read_text(encoding="utf-8"))
    for batch in phase["batches"]:
        engine.submit_batch(batch["batch_id"], batch["events"])

    body = copy.deepcopy(engine._state)
    body["version"] = SAMPLE_VERSION
    body["last_intent_id"] = "sample-snapshot-fixed-intent"
    checksum = checksum_for_snapshot(SAMPLE_VERSION, config.fingerprint, body)
    envelope = {
        "format": SNAPSHOT_FORMAT,
        "version": SAMPLE_VERSION,
        "created_at": FIXED_CREATED_AT,
        "config_fingerprint": config.fingerprint,
        "checksum_algorithm": "sha256",
        "checksum": checksum,
        "intent_id": "sample-snapshot-fixed-intent",
        "batch_id": None,
        "body": body,
    }
    output = SAMPLES / "snapshot_sample.json"
    output.write_text(
        json.dumps(envelope, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    # Verify that the produced fixture round-trips through the same validator used by the API.
    engine.store.load_external(output)
    print(
        json.dumps(
            {
                "file": str(output),
                "version": SAMPLE_VERSION,
                "checksum": checksum,
                "window_count": len(body["windows"]),
                "late_queue_length": len(body["late_queue"]),
                "config_fingerprint": config.fingerprint,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
