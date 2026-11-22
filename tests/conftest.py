from __future__ import annotations

from pathlib import Path

import pytest

from leasefair.clock import FixedClock
from leasefair.service import LeaseService

ROOT = Path(__file__).resolve().parent.parent
SAMPLE_PATH = ROOT / "data" / "fixed_lease_samples.json"
ORIGIN_MS = 1_700_000_000_000


@pytest.fixture
def clock() -> FixedClock:
    return FixedClock(ORIGIN_MS)


@pytest.fixture
def service(tmp_path: Path, clock: FixedClock) -> LeaseService:
    return LeaseService(tmp_path / "lease.sqlite", clock=clock)

