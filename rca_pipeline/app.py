"""FastAPI application factory and uvicorn entrypoint (loopback only)."""
from __future__ import annotations

from fastapi import FastAPI

from .api import router
from .config import get_settings
from .service import Pipeline


def create_app(db_path: str | None = None, *, run_recovery: bool = True) -> FastAPI:
    app = FastAPI(title="Local SLI RCA Pipeline", version="1.0.0")
    settings = get_settings(db_path)
    pipeline = Pipeline(settings)
    if run_recovery:
        pipeline.run_startup_recovery()
    app.state.pipeline = pipeline
    app.include_router(router, prefix="/api/v1")
    return app


def main() -> None:
    import uvicorn

    uvicorn.run(create_app(), host="127.0.0.1", port=8000, log_level="info")


if __name__ == "__main__":
    main()
