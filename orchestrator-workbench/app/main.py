"""Uvicorn entrypoint: loopback-only HTTP on a fixed port."""
from __future__ import annotations

import os
from pathlib import Path

from app.http_api import create_app
from app.service import OrchestrationService
from app.store import Store

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "workbench.sqlite3"


def build_service(db_path: str | os.PathLike[str] | None = None) -> OrchestrationService:
    resolved = Path(db_path) if db_path else Path(os.environ.get("WORKBENCH_DB", DEFAULT_DB))
    store = Store(resolved)
    recovery = store.recover()
    if recovery.recovered:
        print(f"[startup] recovered {len(recovery.aborted_intents)} unfinished intent(s)")
    return OrchestrationService(store)


app = create_app(build_service())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=DEFAULT_HOST, port=DEFAULT_PORT, log_level="info")
