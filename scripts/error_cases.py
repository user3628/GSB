#!/usr/bin/env python3
"""Trigger the three required local error/degradation cases."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from stream_tts.config import SplitConfig, load_config
from stream_tts.engine import StreamingTTSEngine
from stream_tts.errors import TTSError
from stream_tts.frontend import PhonemeTable, front_end


def emit_error(exc: TTSError) -> dict:
    return exc.to_dict()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("case", choices=["invalid-split", "empty", "unknown", "over-budget"])
    parser.add_argument("--first-chunk-min-chars", type=int, default=-1)
    parser.add_argument("--subsequent-chunk-chars", type=int, default=18)
    args = parser.parse_args()

    config = load_config(ROOT / "config/default.yaml", workspace=ROOT)
    table = PhonemeTable.load(config.resolved_path("phoneme_table"))
    if args.case == "invalid-split":
        split = SplitConfig(
            first_chunk_min_chars=args.first_chunk_min_chars,
            subsequent_chunk_chars=args.subsequent_chunk_chars,
            punctuation_forced=True,
            max_budget_splits=2,
        )
        try:
            front_end(
                utt_id="injected_invalid_split",
                text="你好，世界。",
                language="zh",
                split=split,
                phoneme_table=table,
            )
        except TTSError as exc:
            print(json.dumps(emit_error(exc), ensure_ascii=False, indent=2, sort_keys=True))
            return 0
    elif args.case in {"empty", "unknown"}:
        text = "  \t  " if args.case == "empty" else "你好，龘。"
        try:
            front_end(
                utt_id=f"injected_{args.case}",
                text=text,
                language="zh",
                split=config.split,
                phoneme_table=table,
            )
        except TTSError as exc:
            print(json.dumps(emit_error(exc), ensure_ascii=False, indent=2, sort_keys=True))
            return 0
    elif args.case == "over-budget":
        engine = StreamingTTSEngine(
            workspace=ROOT,
            config_path=ROOT / "config/default.yaml",
            run_id="error_cases_over_budget",
        )
        outcome = engine.run()
        selected = [
            item for item in outcome.utterances if item["utt_id"] == "utt_budget_stress_03"
        ]
        print(json.dumps({"ok": True, "status": outcome.status, "utterances": selected}, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    raise SystemExit("case did not produce the expected local behavior")


if __name__ == "__main__":
    raise SystemExit(main())
