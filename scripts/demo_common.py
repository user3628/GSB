"""Process helpers shared by the reproducible demo scripts."""

from __future__ import annotations

import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
SERVER_SCRIPT = ROOT / "scripts" / "run_server.py"


def reset_directory(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def start_server(
    data_dir: Path,
    params_path: Path | None = None,
    port: int = 0,
    quiet: bool = True,
) -> dict[str, Any]:
    data_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(SERVER_SCRIPT),
        "--root",
        str(ROOT),
        "--data-dir",
        str(data_dir),
        "--port",
        str(port),
    ]
    if params_path is not None:
        cmd.extend(["--params", str(params_path)])
    if quiet:
        cmd.append("--quiet")
    process = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    stdout_queue: queue.Queue[str | None] = queue.Queue()
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []

    def read_stdout() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            stdout_lines.append(line.rstrip("\n"))
            stdout_queue.put(line)
        stdout_queue.put(None)

    def read_stderr() -> None:
        assert process.stderr is not None
        for line in process.stderr:
            stderr_lines.append(line.rstrip("\n"))

    threading.Thread(target=read_stdout, daemon=True).start()
    threading.Thread(target=read_stderr, daemon=True).start()

    ready_info: dict[str, Any] | None = None
    deadline = time.monotonic() + 12.0
    while time.monotonic() < deadline:
        if process.poll() is not None:
            break
        try:
            line = stdout_queue.get(timeout=0.1)
        except queue.Empty:
            continue
        if line is None:
            break
        if line.startswith("SERVER_READY "):
            ready_info = json.loads(line[len("SERVER_READY ") :])
            break
    if ready_info is None:
        stop_server(process, force=True)
        raise RuntimeError(
            "server failed to become ready\nSTDOUT:\n"
            + "\n".join(stdout_lines)
            + "\nSTDERR:\n"
            + "\n".join(stderr_lines)
        )
    return {
        "process": process,
        "info": ready_info,
        "stdout_lines": stdout_lines,
        "stderr_lines": stderr_lines,
    }


def stop_server(process: subprocess.Popen[str], force: bool = False, timeout: float = 6.0) -> int | None:
    if process.poll() is not None:
        return process.returncode
    if force:
        process.kill()
    else:
        process.terminate()
    try:
        return process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        return process.wait(timeout=timeout)


def stop_handle(handle: dict[str, Any], force: bool = False) -> int | None:
    process = handle["process"]
    return stop_server(process, force=force)


def wait_until(predicate: Callable[[], bool], timeout: float = 3.0, interval: float = 0.01) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def run_editor_process(
    script_name: str,
    base_url: str,
    runtime_dir: Path,
    run_id: str,
    timeout: float = 10.0,
) -> dict[str, Any]:
    script = ROOT / "scripts" / script_name
    env = dict(os.environ)
    env.update(
        {
            "MVCC_BASE_URL": base_url,
            "MVCC_RUNTIME_DIR": str(runtime_dir),
            "MVCC_RUN_ID": run_id,
        }
    )
    completed = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(ROOT),
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=timeout,
    )
    output_lines = [line for line in completed.stdout.splitlines() if line.strip()]
    if completed.returncode != 0:
        raise RuntimeError(
            f"{script_name} failed with {completed.returncode}\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
        )
    try:
        return json.loads(output_lines[-1])
    except (IndexError, json.JSONDecodeError) as error:
        raise RuntimeError(
            f"{script_name} did not emit a JSON result\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
        ) from error

