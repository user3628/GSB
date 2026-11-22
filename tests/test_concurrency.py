from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from multiscale_fusion.engine import FusionEngine

from .conftest import ROOT, popen_cli


def test_multiple_workers_publish_one_continuous_chain(workspace: Path) -> None:
    processes = [
        popen_cli("run", "--workspace", str(workspace), "--task", "demo_fpn")
        for _ in range(3)
    ]
    outputs = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=60)
        assert process.returncode == 0, stderr
        outputs.append(json.loads(stdout))
    engine = FusionEngine(workspace)
    state = engine.store("demo_fpn").scan()
    assert [record["sequence"] for record in state.records] == [1, 2, 3]
    assert state.head_sequence == 3
    assert engine.verify_checkpoints("demo_fpn")["classification"] == "normal"
    assert engine.verify_incremental("demo_fpn")["ok"] is True
    status = engine.status("demo_fpn")
    assert all(item["current"] for item in status["layers"].values())
    assert status["sensitivity"]["current"] is True
    head_sequences = {item["head_sequence"] for item in outputs}
    assert head_sequences == {3}
