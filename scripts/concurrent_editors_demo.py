#!/usr/bin/env python3
"""Run editor_a.py and editor_b.py concurrently against one service."""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.demo_common import (  # noqa: E402
    reset_directory,
    start_server,
    stop_handle,
)


def launch_concurrent_editors(base_url: str, runtime_dir: Path, run_id: str | None = None) -> dict:
    import os
    import subprocess

    run_id = run_id or uuid.uuid4().hex[:12]
    reset_directory(runtime_dir)
    env = dict(os.environ)
    env.update(
        {
            "MVCC_BASE_URL": base_url,
            "MVCC_RUNTIME_DIR": str(runtime_dir),
            "MVCC_RUN_ID": run_id,
        }
    )
    processes = {
        "A": subprocess.Popen(
            [sys.executable, str(ROOT / "scripts" / "editor_a.py")],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        ),
        "B": subprocess.Popen(
            [sys.executable, str(ROOT / "scripts" / "editor_b.py")],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        ),
    }
    outputs: dict[str, dict] = {}
    errors: dict[str, str] = {}
    for name, process in processes.items():
        try:
            stdout, stderr = process.communicate(timeout=12.0)
        except Exception:
            process.kill()
            stdout, stderr = process.communicate()
            errors[name] = "timeout"
        if process.returncode != 0:
            errors[name] = f"exit={process.returncode}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
            continue
        lines = [line for line in stdout.splitlines() if line.strip()]
        try:
            outputs[name] = json.loads(lines[-1])
        except (IndexError, json.JSONDecodeError):
            errors[name] = f"invalid JSON output\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    if errors:
        raise RuntimeError("concurrent editors failed: " + json.dumps(errors, ensure_ascii=False))
    a = outputs["A"]
    b = outputs["B"]
    if b["error"]["code"] != "E_EDIT_STALE":
        raise RuntimeError("editor B did not receive E_EDIT_STALE")
    if a["saved"]["new_row_version"] <= a["opened"]["row_version"]:
        raise RuntimeError("editor A did not create a newer row version")
    if b["latest_editable"]["row_version"] != a["saved"]["new_row_version"]:
        raise RuntimeError("stale response did not include editor A's latest version")
    return {
        "ok": True,
        "run_id": run_id,
        "editor_a": a,
        "editor_b": b,
        "assertion": "B opened the same version before A committed; B save was rejected with E_EDIT_STALE and latest version returned.",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--runtime-dir", default=str(ROOT / "runtime" / "concurrent_editors"))
    args = parser.parse_args()
    data_dir = Path(args.runtime_dir) / "data"
    reset_directory(data_dir)
    handle = start_server(data_dir, ROOT / "samples" / "demo_params.json", args.port)
    base_url = f"http://{handle['info']['host']}:{handle['info']['port']}"
    try:
        result = launch_concurrent_editors(base_url, Path(args.runtime_dir) / "barrier")
    finally:
        stop_handle(handle, force=False)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

