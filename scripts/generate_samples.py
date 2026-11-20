#!/usr/bin/env python3
"""Generate deterministic two-hour JSONL fixtures for local verification."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE = datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
MINUTES = 120


def build_events(
    *,
    service: str,
    events_per_minute: int,
    bad_every: int,
) -> list[dict]:
    events: list[dict] = []
    interval_ms = 60_000 / events_per_minute
    total = MINUTES * events_per_minute
    for index in range(total):
        timestamp = BASE + timedelta(milliseconds=index * interval_ms)
        events.append(
            {
                "event_id": f"{service}-{index:07d}",
                "timestamp": timestamp.isoformat(timespec="milliseconds").replace(
                    "+00:00", "Z"
                ),
                "service": service,
                "sli_type": "availability",
                "good": index % bad_every != 0,
            }
        )
    return events


def write_jsonl(path: Path, events: list[dict]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        for item in events:
            handle.write(
                json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                + "\n"
            )
    temporary.replace(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return digest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "samples",
    )
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    definitions = {
        "events_small.jsonl": {
            "service": "checkout-small",
            "events_per_minute": 50,
            "bad_every": 200,
        },
        "events_large.jsonl": {
            "service": "checkout-large",
            "events_per_minute": 500,
            "bad_every": 100,
        },
    }
    manifest = {
        "generator": "scripts/generate_samples.py",
        "seed": "implicit index-based deterministic generation; no RNG",
        "start": BASE.isoformat().replace("+00:00", "Z"),
        "end": (BASE + timedelta(minutes=MINUTES)).isoformat().replace("+00:00", "Z"),
        "duration_minutes": MINUTES,
        "files": {},
    }
    for filename, definition in definitions.items():
        events = build_events(**definition)
        path = args.out_dir / filename
        digest = write_jsonl(path, events)
        bad = sum(1 for item in events if not item["good"])
        manifest["files"][filename] = {
            **definition,
            "event_count": len(events),
            "bad_events": bad,
            "sli": (len(events) - bad) / len(events),
            "sha256": digest,
            "bytes": path.stat().st_size,
        }
        print(f"wrote {path} events={len(events)} bad={bad} sha256={digest}")
    manifest_path = args.out_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
