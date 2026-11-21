"""Dependency-free PEP 517 backend for the pure-Python component wheel."""
from __future__ import annotations

import base64
import hashlib
import io
import tarfile
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent
NAME = "contract_elements"
DIST = "contract_elements"
VERSION = "1.0.0"


def _metadata() -> str:
    return (
        "Metadata-Version: 2.4\n"
        "Name: contract-elements\n"
        f"Version: {VERSION}\n"
        "Summary: Deterministic local contract clause element extraction component\n"
        "Requires-Python: >=3.10\n"
        "Requires-Dist: numpy>=2.0,<3\n"
        "\n"
    )


def _wheel_metadata() -> str:
    return (
        "Wheel-Version: 1.0\n"
        "Generator: contract-elements-build-backend\n"
        "Root-Is-Purelib: true\n"
        "Tag: py3-none-any\n"
        "\n"
    )


def _entry_points() -> str:
    return "[console_scripts]\ncontract-elements = contract_elements.cli:main\n"


def _hash(data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode("ascii")
    return f"sha256={digest}"


def _zip_write(archive: zipfile.ZipFile, name: str, data: bytes) -> None:
    info = zipfile.ZipInfo(name, date_time=(2025, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    archive.writestr(info, data)


def _wheel_bytes() -> bytes:
    files: list[tuple[str, bytes]] = []
    for path in sorted((ROOT / "contract_elements").glob("*.py")):
        files.append((f"contract_elements/{path.name}", path.read_bytes()))
    dist_info = f"{DIST}-{VERSION}.dist-info"
    files.extend(
        [
            (f"{dist_info}/METADATA", _metadata().encode("utf-8")),
            (f"{dist_info}/WHEEL", _wheel_metadata().encode("utf-8")),
            (f"{dist_info}/entry_points.txt", _entry_points().encode("utf-8")),
        ]
    )
    record_lines = [f"{name},{_hash(data)},{len(data)}" for name, data in files]
    record_lines.append(f"{dist_info}/RECORD,,")
    files.append((f"{dist_info}/RECORD", ("\n".join(record_lines) + "\n").encode("utf-8")))
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in files:
            _zip_write(archive, name, data)
    return buffer.getvalue()


def get_requires_for_build_wheel(config_settings=None):
    return []


def get_requires_for_build_sdist(config_settings=None):
    return []


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    dist_info = Path(metadata_directory) / f"{DIST}-{VERSION}.dist-info"
    dist_info.mkdir(parents=True, exist_ok=True)
    (dist_info / "METADATA").write_text(_metadata(), encoding="utf-8")
    (dist_info / "WHEEL").write_text(_wheel_metadata(), encoding="utf-8")
    (dist_info / "entry_points.txt").write_text(_entry_points(), encoding="utf-8")
    return dist_info.name


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    filename = f"{DIST}-{VERSION}-py3-none-any.whl"
    Path(wheel_directory).mkdir(parents=True, exist_ok=True)
    (Path(wheel_directory) / filename).write_bytes(_wheel_bytes())
    return filename


def build_sdist(sdist_directory, config_settings=None):
    filename = f"{DIST}-{VERSION}.tar.gz"
    target = Path(sdist_directory) / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(target, "w:gz") as archive:
        for relative in ["pyproject.toml", "build_backend.py", "README.md"]:
            path = ROOT / relative
            if path.exists():
                archive.add(path, arcname=f"{DIST}-{VERSION}/{relative}")
        for path in sorted((ROOT / "contract_elements").glob("*.py")):
            archive.add(path, arcname=f"{DIST}-{VERSION}/contract_elements/{path.name}")
    return filename

