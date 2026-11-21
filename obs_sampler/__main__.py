"""Run the HTTP server with environment-selected database and log paths."""

from __future__ import annotations

import os

import uvicorn

from .api import create_app


def main() -> None:
    app = create_app()
    uvicorn.run(
        app,
        host=os.environ.get("OBS_HOST", "127.0.0.1"),
        port=int(os.environ.get("OBS_PORT", "8000")),
        reload=False,
        workers=1,
    )


if __name__ == "__main__":
    main()
