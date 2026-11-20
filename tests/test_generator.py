from __future__ import annotations

import hashlib
from pathlib import Path

from storypipe.generator import generate_samples


def _digest_files(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_generated_samples_rebuild_identically(tmp_path: Path) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first = generate_samples(first_root)
    second = generate_samples(second_root)
    assert first == second
    assert first["set_digest"].startswith("sha256:")
    assert _digest_files(first_root) == _digest_files(second_root)
    assert first["scales"]["small"]["shot_count"] == 5
    assert first["scales"]["large"]["shot_count"] == 36
