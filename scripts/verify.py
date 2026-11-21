#!/usr/bin/env python3
"""Run the test suite and deterministic demo, then record actual results."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from blocklab.demo import run_demo  # noqa: E402
from blocklab.util import atomic_write_json, digest_json  # noqa: E402


def main() -> int:
    test_command = [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"]
    tests = subprocess.run(test_command, cwd=ROOT, text=True, capture_output=True, check=False)
    combined = (tests.stdout + "\n" + tests.stderr).strip()
    visible_lines = [line for line in combined.splitlines() if not line.startswith("[http]")]
    match = re.search(r"Ran (\d+) tests", combined)
    tests_run = int(match.group(1)) if match else None
    demo = run_demo(
        root=ROOT,
        workdir="demo_work",
        report_path=ROOT / "artifacts" / "demo_report.json",
    )
    report = {
        "schema": "blocklab.verification.v1",
        "python": sys.version.split()[0],
        "tests": {
            "command": "python -m unittest discover -s tests -v",
            "returncode": tests.returncode,
            "tests_run": tests_run,
            "status": "passed" if tests.returncode == 0 else "failed",
            "output_tail": visible_lines[-16:],
        },
        "demo": {
            "command": "python scripts/demo.py",
            "ok": demo["ok"],
            "fixed_demo_digest": demo["fixed_demo_digest"],
            "clean_summaries": demo["clean_summaries"],
            "recovery": demo["recovery"],
            "invalidation_scopes": demo["invalidation_scopes"],
            "business_codes": demo["business_codes"],
            "report_path": demo["report_path"],
        },
    }
    report["ok"] = tests.returncode == 0 and demo["ok"]
    report["verification_digest"] = digest_json(report)
    atomic_write_json(ROOT / "artifacts" / "verification.json", report)
    print(json.dumps({"ok": report["ok"], "verification_digest": report["verification_digest"], "tests_run": tests_run, "demo_digest": demo["fixed_demo_digest"]}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

