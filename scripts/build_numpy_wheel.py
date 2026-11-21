#!/usr/bin/env python3
"""Repack the preinstalled offline NumPy distribution into a local wheel.

The task environment provides NumPy already, but no network or wheel cache. This
script creates a deterministic installable wheel from that local installation so
the project virtual environment can be built with ``pip --no-index`` only.
"""
from __future__ import annotations

import importlib.metadata
import sys
import sysconfig
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    distribution = importlib.metadata.distribution("numpy")
    version = distribution.version
    python_tag = f"cp{sys.version_info.major}{sys.version_info.minor}"
    platform = sysconfig.get_platform().replace("-", "_").replace(".", "_")
    if "linux" in platform:
        platform = "manylinux_2_27_x86_64"
    wheel_name = f"numpy-{version}-{python_tag}-{python_tag}-{platform}.whl"
    target = ROOT / "vendor" / wheel_name
    target.parent.mkdir(parents=True, exist_ok=True)
    files = []
    for file in distribution.files or []:
        path = Path(distribution.locate_file(file))
        if not path.is_file() or "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        arcname = str(file)
        if arcname.startswith(("numpy/", "numpy.libs/", f"numpy-{version}.dist-info/")):
            files.append((path, arcname))
    files.sort(key=lambda item: item[1])
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path, arcname in files:
            info = zipfile.ZipInfo(arcname, date_time=(2025, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = (path.stat().st_mode & 0xFFFF) << 16
            archive.writestr(info, path.read_bytes())
    print(target)


if __name__ == "__main__":
    main()

