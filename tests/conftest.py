from __future__ import annotations

from pathlib import Path

import pytest

from slo_budget.config import Settings
from slo_budget.engine import SLIEngine


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    result = Settings(
        base_dir=tmp_path,
        data_dir=tmp_path / "data",
        db_path=tmp_path / "data" / "slo.db",
        windows=(300, 1800, 3600),
        slo_target=0.999,
        minimum_samples=20,
        exact_event_limit=500,
        late_tolerance_seconds=600,
        warning_burn_rate=2.0,
        critical_burn_rate=6.0,
    )
    result.validate()
    return result


@pytest.fixture
def engine(settings: Settings) -> SLIEngine:
    instance = SLIEngine(settings)
    instance.recover(reason="test-startup")
    return instance


def event(
    timestamp_ms: int,
    *,
    service: str = "checkout",
    sli_type: str = "availability",
    good: bool = True,
    event_id: str | None = None,
) -> dict:
    value = {
        "timestamp": timestamp_ms,
        "service": service,
        "sli_type": sli_type,
        "good": good,
    }
    if event_id is not None:
        value["event_id"] = event_id
    return value


def window_events(
    count: int,
    start_ms: int,
    *,
    service: str = "checkout",
    sli_type: str = "availability",
    bad_every: int = 0,
    span_ms: int = 240_000,
) -> list[dict]:
    step = max(1, span_ms // max(1, count))
    return [
        event(
            start_ms + index * step,
            service=service,
            sli_type=sli_type,
            good=not (bad_every and index % bad_every == 0),
        )
        for index in range(count)
    ]
