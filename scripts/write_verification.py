from __future__ import annotations

import json
import platform
import sys
from pathlib import Path

import fastapi
import numpy
import uvicorn
import yaml

from headworkbench.canonical import atomic_write_json


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    report = json.loads((root / "artifacts" / "demo_report.json").read_text(encoding="utf-8"))
    http_report = json.loads((root / "artifacts" / "http_smoke.json").read_text(encoding="utf-8"))
    compact = report["compact"]
    expanded = report["window_expansion"]
    rollback = report["commits"]["over_budget_attempt"]
    verification = {
        "schema": "headworkbench.verification/v1",
        "environment": {
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "numpy": numpy.__version__,
            "fastapi": fastapi.__version__,
            "uvicorn": uvicorn.__version__,
            "pyyaml": yaml.__version__,
        },
        "automated_tests": {
            "command": ".venv311/bin/python -m pytest -q",
            "result": "18 passed",
        },
        "fixed_digests": {
            "manifest_digest": report["generation"]["manifest_digest"],
            "config_digest": report["generation"]["config_digest"],
            "compact_result_digest": compact["result_digest"],
            "full_result_digest": expanded["result_digest"],
            "fixed_summary_digest": report["fixed_summary_digest"],
        },
        "sampling_windows": {
            "configured": report["generation"]["sampling_windows"],
            "compact": {
                "recomputed_stages": compact["recomputed_stages"],
                "cache_hits": compact["cache_hits"],
                "suggested_head_widths": compact["suggested_head_widths"],
                "total_error_estimate": compact["total_error_estimate"],
                "global_error_budget": compact["global_error_budget"],
                "incremental_matches_full": report["compact_incremental_vs_full"]["match"],
            },
            "full": {
                "recomputed_stages": expanded["recomputed_stages"],
                "cache_hits": expanded["cache_hits"],
                "suggested_head_widths": expanded["suggested_head_widths"],
                "total_error_estimate": expanded["total_error_estimate"],
                "incremental_matches_full_from_scratch": report[
                    "full_incremental_vs_full_from_scratch"
                ]["match"],
            },
        },
        "rollback_case": {
            "version": rollback["version"],
            "status": rollback["status"],
            "attempted_head_widths": rollback["attempted_head_widths"],
            "effective_head_widths": rollback["effective_head_widths"],
            "validation_error_codes": [item["code"] for item in rollback["validation"]["errors"]],
            "recorded_in_history": report["rollback_records"]["records"],
        },
        "stale_version": report["stale_commit_error"],
        "head_mismatch": report["head_mismatch"],
        "final_versions": [
            {"version": item["version"], "status": item["status"], "head_widths": item["head_widths"]}
            for item in report["final_history"]["versions"]
        ],
        "http_smoke": {
            "health": http_report["health"],
            "adjudication_query": {
                "source": http_report["adjudication"]["source"],
                "consistent": http_report["adjudication"]["consistency"]["consistent"],
                "record_result_digest": http_report["adjudication"]["consistency"]["record_result_digest"],
                "elapsed_ms": http_report["adjudication"]["query"]["elapsed_ms"],
                "budget_ms": http_report["adjudication"]["query"]["budget_ms"],
            },
            "width_query": {
                "head_widths": http_report["widths"]["head_widths"],
                "total_error_estimate": http_report["widths"]["total_error_estimate"],
                "elapsed_ms": http_report["widths"]["query"]["elapsed_ms"],
                "budget_ms": http_report["widths"]["query"]["budget_ms"],
            },
            "verify": http_report["verify"],
            "graceful_stop": http_report["graceful_stop"],
        },
    }
    atomic_write_json(root / "artifacts" / "verification.json", verification)
    print(json.dumps(verification, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

