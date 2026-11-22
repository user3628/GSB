from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from upload_index.service import UploadIndexService


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def part(seq: int, text: str, size: int | None = None) -> dict[str, object]:
    return {
        "seq": seq,
        "size": len(text) if size is None else size,
        "sha256": sha256(text),
        "arrival_ms": 1000 + seq,
    }


@pytest.fixture
def service(tmp_path: Path):
    instance = UploadIndexService(tmp_path / "index.db")
    instance.initialize()
    try:
        yield instance
    finally:
        instance.shutdown()
        instance.close()

