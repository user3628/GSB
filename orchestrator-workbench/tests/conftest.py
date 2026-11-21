from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest

from app.service import OrchestrationService
from app.store import Store


@pytest.fixture()
def service(tmp_path):
    store = Store(tmp_path / "workbench.sqlite3")
    yield OrchestrationService(store)
    store.close()


@pytest.fixture()
def samples_dir():
    return ROOT / "samples"
