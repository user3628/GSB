"""Replication lag governance data editor and adjudication service."""

from typing import Any

__all__ = ["create_app"]
__version__ = "1.0.0"


def create_app(*args: Any, **kwargs: Any):
    # Keep package import side-effect free so `python -m replication_guard --db ...`
    # can set the database path before the FastAPI module is imported.
    from .api import create_app as factory

    return factory(*args, **kwargs)

