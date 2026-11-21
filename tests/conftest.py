from __future__ import annotations

from pathlib import Path

import pytest

from prep_pipeline.config import Settings
from prep_pipeline.service import PipelineService

from .helpers import DATA_DIR, make_settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return make_settings(tmp_path)


@pytest.fixture
def service(settings: Settings) -> PipelineService:
    instance = PipelineService(settings)
    instance.initialize(recover=False)
    return instance


@pytest.fixture
def bootstrapped_service(service: PipelineService) -> PipelineService:
    service.bootstrap_policies(
        [DATA_DIR / "policy_v1.json", DATA_DIR / "policy_v2.json"]
    )
    return service
