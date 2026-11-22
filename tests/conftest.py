from __future__ import annotations

from dataclasses import dataclass

import pytest

from tests.support import (
    free_port,
    start_upstreams,
    start_workbench,
    stop_process,
)


@dataclass(frozen=True)
class LocalServices:
    host: str
    backend_port: int
    alt_backend_port: int
    egress_port: int
    process: object


@pytest.fixture(scope="session")
def local_services(tmp_path_factory: pytest.TempPathFactory) -> LocalServices:
    host = "127.0.0.1"
    backend_port = free_port(host)
    alt_backend_port = free_port(host)
    egress_port = free_port(host)
    log_path = tmp_path_factory.mktemp("services") / "upstreams.log"
    process = start_upstreams(
        host=host,
        backend_port=backend_port,
        egress_port=egress_port,
        alt_backend_port=alt_backend_port,
        log_path=log_path,
    )
    services = LocalServices(
        host=host,
        backend_port=backend_port,
        alt_backend_port=alt_backend_port,
        egress_port=egress_port,
        process=process,
    )
    yield services
    stop_process(process, force=True)
    handle = getattr(process, "_test_log_handle", None)
    if handle is not None:
        handle.close()


@pytest.fixture(scope="session")
def live_workbench(
    tmp_path_factory: pytest.TempPathFactory, local_services: LocalServices
) -> dict[str, object]:
    host = local_services.host
    port = free_port(host)
    db_path = tmp_path_factory.mktemp("workbench") / "live.sqlite3"
    log_path = tmp_path_factory.mktemp("workbench") / "workbench.log"
    process = start_workbench(
        host=host,
        port=port,
        db_path=db_path,
        enable_failpoints=False,
        log_path=log_path,
    )
    value = {
        "base_url": f"http://{host}:{port}",
        "host": host,
        "port": port,
        "db_path": db_path,
        "process": process,
        "services": local_services,
    }
    yield value
    stop_process(process, force=True)
    handle = getattr(process, "_test_log_handle", None)
    if handle is not None:
        handle.close()
