from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from transcript_workbench.workspace import Workspace
from tests.conftest import first_word


def test_sigkill_after_committed_edit_recovers_latest_snapshot(boot_workspace):
    state = boot_workspace.load_state()
    word = first_word(state)
    ready = boot_workspace.root / "child-ready.json"
    script = f"""
import json, time
from transcript_workbench.workspace import Workspace
ws = Workspace({str(boot_workspace.root)!r})
result = ws.edit([
    {{'op': 'update_word', 'entry_id': {word.entry_id!r}, 'text': 'committed-before-kill'}}
], expected_version=1)
open({str(ready)!r}, 'w', encoding='utf-8').write(json.dumps(result))
time.sleep(60)
"""
    process = subprocess.Popen(
        [sys.executable, "-c", script],
        cwd=str(Path(__file__).resolve().parents[1]),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 15
    while not ready.exists() and time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise AssertionError(f"child exited early: {stdout}\n{stderr}")
        time.sleep(0.02)
    assert ready.exists()
    if os.name == "nt":
        process.terminate()
    else:
        os.kill(process.pid, signal.SIGKILL)
    process.wait(timeout=10)
    child_result = json.loads(ready.read_text(encoding="utf-8"))
    assert child_result["version"] == 2
    recovered = Workspace(boot_workspace.root).recover()
    assert recovered["version"] == 2
    assert recovered["replayed_versions"] == []
    assert recovered["word_count"] == len(boot_workspace.load_state().words)
    assert boot_workspace.load_state().version == 2
