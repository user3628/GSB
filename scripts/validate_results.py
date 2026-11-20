#!/usr/bin/env python3
"""Validate persisted shards, rebuild global statistics and detect stale data."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from detpipe.errors import PipeError
from detpipe.validator import report_bytes, validate_run, write_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate pipeline results")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--report", default=None)
    parser.add_argument("--expect-bytes", default=None,
                        help="path to a canonical stats file to compare against")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        report = validate_run(args.workspace)
    except (FileNotFoundError, PipeError) as exc:
        payload = exc.to_dict() if isinstance(exc, PipeError) else {"message": str(exc)}
        print(json.dumps({"error": payload}, ensure_ascii=False, indent=2))
        return 2
    if args.report:
        write_report(report, args.report)
    digest = hashlib.sha256(report_bytes(report)).hexdigest()
    printable = {key: report[key] for key in (
        "ok", "manifest_fingerprint", "shards_total", "shards_present",
        "chain_contiguous", "missing_shard_indices", "invalid_shards",
        "unprocessed_shards", "global_statistics",
    )}
    printable["stats_sha256"] = digest
    print(json.dumps(printable, ensure_ascii=False, indent=2))
    if args.expect_bytes:
        expected = Path(args.expect_bytes).read_bytes()
        actual = report_bytes(report)
        if expected != actual:
            print(json.dumps({"error": "global statistics byte mismatch"}), file=sys.stderr)
            return 4
    if not report["ok"]:
        return 5
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
