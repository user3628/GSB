#!/usr/bin/env python3
"""Run the test suite and acceptance demo, then persist a verification report."""

from __future__ import annotations

import io
import json
import platform
import sys
import time
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def main() -> int:
    from ctr_cvr_workbench.demo import run_demo
    from ctr_cvr_workbench.util import atomic_write_json

    started = time.monotonic()
    suite = unittest.defaultTestLoader.discover("tests", top_level_dir=".")
    stream = io.StringIO()
    runner = unittest.TextTestRunner(stream=stream, verbosity=2)
    result = runner.run(suite)
    tests_ok = result.wasSuccessful()
    demo = run_demo("artifacts/demo", config_path="configs/default.yaml")
    report = {
        "format": "ctr-cvr-verification-report-v1",
        "ok": bool(tests_ok and demo.get("ok")),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "operating_system": platform.system(),
            "actual_runtime_is_windows": platform.system() == "Windows",
            "windows_compatible_paths_and_processes": True,
            "gpu_required": False,
            "network_required": False,
            "container_required": False,
        },
        "tests": {
            "command": "python3 -m unittest discover -v",
            "run": result.testsRun,
            "failures": len(result.failures),
            "errors": len(result.errors),
            "skipped": len(result.skipped),
            "ok": tests_ok,
            "output": stream.getvalue(),
        },
        "demo": demo,
        "demo_report_path": "artifacts/demo/report.json",
    }
    report_path = Path("artifacts/verification_report.json")
    atomic_write_json(report_path, report)
    print(json.dumps({
        "ok": report["ok"],
        "report_path": report_path.as_posix(),
        "tests": report["tests"],
        "demo_summary_digest": demo.get("summary_digest"),
        "demo_error_codes": demo.get("error_codes"),
        "elapsed_seconds": report["elapsed_seconds"],
    }, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
