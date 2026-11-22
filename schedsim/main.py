"""Uvicorn entry point: ``uvicorn schedsim.main:app``."""

from .api import create_app

app = create_app()

__all__ = ["app"]

