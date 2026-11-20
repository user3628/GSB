"""Uvicorn entry point: ``uvicorn app:app --workers 1``."""

from slo_budget.api import create_app

app = create_app()
