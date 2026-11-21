from __future__ import annotations

from pathlib import Path

import pytest

from cabin_voice.audio import generate_demo_dataset
from cabin_voice.service import CabinService


@pytest.fixture(scope="session")
def demo_dataset(tmp_path_factory: pytest.TempPathFactory) -> dict[str, str]:
    root = tmp_path_factory.mktemp("dataset")
    return generate_demo_dataset(root, seed=20261122)


@pytest.fixture()
def service(tmp_path: Path, demo_dataset: dict[str, str]) -> CabinService:
    app_service = CabinService(tmp_path / "workspace")
    app_service.import_manifest(demo_dataset["manifest_path"], reset=True)
    return app_service


@pytest.fixture()
def ready_service(service: CabinService) -> CabinService:
    service.run(verify=True)
    return app_service_with_state(service)


def app_service_with_state(service: CabinService) -> CabinService:
    return service
