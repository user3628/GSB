"""Shared test helpers."""

from __future__ import annotations

import tempfile
from pathlib import Path

from mvcc_engine.config import load_settings


ROOT = Path(__file__).resolve().parents[1]


def make_settings(data_dir: Path, **overrides: object):
    settings = load_settings(
        root=ROOT,
        params_path=ROOT / "samples" / "demo_params.json",
        data_dir=data_dir,
        port=0,
        host="127.0.0.1",
    )
    return settings.with_overrides(**overrides)


def temporary_data_dir() -> tempfile.TemporaryDirectory[str]:
    return tempfile.TemporaryDirectory(prefix="mvcc-test-")

