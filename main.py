"""Uvicorn entry point: ``uvicorn main:app``."""

from obs_sampler.api import create_app

app = create_app()
