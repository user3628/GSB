from __future__ import annotations

import shutil
from pathlib import Path

from sumengine.engine import SummaryEngine


REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLES = REPO_ROOT / "samples"


def workspace_with_samples(root: Path, name: str = "workspace") -> SummaryEngine:
    workspace = root / name
    (workspace / "samples").mkdir(parents=True, exist_ok=True)
    for file_name in ("documents.jsonl", "config.yaml"):
        shutil.copy2(SAMPLES / file_name, workspace / "samples" / file_name)
    return SummaryEngine(workspace)
