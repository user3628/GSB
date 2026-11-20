#!/usr/bin/env python3
"""Re-run the default demo and compare against the committed fixed summary."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from stream_tts.engine import StreamingTTSEngine


def main() -> int:
    expected = json.loads((ROOT / "data/demo_reference_summary.json").read_text(encoding="utf-8"))
    engine = StreamingTTSEngine(
        workspace=ROOT,
        config_path=ROOT / "config/default.yaml",
        run_id="main",
    )
    outcome = engine.run()
    summary = json.loads(engine.summary_path.read_text(encoding="utf-8"))
    checks = {
        "status_complete": outcome.status == "complete",
        "aggregate_sha256": summary["aggregate_sha256"] == expected["aggregate_sha256"],
        "trace_sha256": summary["trace_sha256"] == expected["trace_sha256"],
        "trace_records": summary["trace_records"] == expected["trace_records"],
        "config_version": summary["config_version"] == expected["config_version"],
        "degraded_utterances": summary["degraded_utterances"] == expected["degraded_utterances"],
    }
    expected_utts = {item["utt_id"]: item for item in expected["utterances"]}
    for actual in summary["utterances"]:
        wanted = expected_utts[actual["utt_id"]]
        for field in (
            "wav_sha256",
            "pcm_sha256",
            "acoustic_sha256",
            "markers_sha256",
            "frame_count",
            "sample_count_per_channel",
            "block_count",
            "degraded",
            "reference_match",
        ):
            checks[f"{actual['utt_id']}.{field}"] = actual[field] == wanted[field]
    failed = [name for name, ok in checks.items() if not ok]
    print(
        json.dumps(
            {
                "ok": not failed,
                "checks": checks,
                "failed": failed,
                "aggregate_sha256": summary["aggregate_sha256"],
                "trace_sha256": summary["trace_sha256"],
                "summary_path": str(engine.summary_path),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
