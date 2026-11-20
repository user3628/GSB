from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

from dialogue_engine.serialization import atomic_write_json, read_json


def clone_data(source: Path, destination: Path) -> Path:
    shutil.copytree(source, destination)
    return destination


def mutate_manifest(data_dir: Path, mutator: object, *, filename: str = "manifest.json") -> None:
    path = data_dir / filename
    manifest = read_json(path)
    assert isinstance(manifest, dict)
    assert callable(mutator)
    mutator(manifest)
    atomic_write_json(path, manifest)


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree_digests(root: Path, pattern: str = "*.json") -> dict[str, str]:
    return {
        str(path.relative_to(root)): file_digest(path)
        for path in sorted(root.rglob(pattern))
    }


def dump_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)

