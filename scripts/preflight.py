#!/usr/bin/env python3
"""Print executable environment versions and verify the offline preconditions."""

from __future__ import annotations

import json
import os
import platform
import shutil
import sys
from pathlib import Path


def main() -> int:
    checks: dict[str, object] = {}
    checks["python_version"] = platform.python_version()
    checks["python_implementation"] = platform.python_implementation()
    checks["python_executable"] = sys.executable
    checks["python_ge_310"] = sys.version_info >= (3, 10)
    checks["operating_system"] = platform.system()
    checks["platform"] = platform.platform()
    checks["cwd"] = Path.cwd().as_posix()
    checks["gpu_required"] = False
    checks["network_required"] = False
    checks["container_required"] = False
    try:
        import numpy

        checks["numpy_version"] = numpy.__version__
        checks["numpy_importable"] = True
    except Exception as exc:  # pragma: no cover - preflight reports the failure
        checks["numpy_version"] = None
        checks["numpy_importable"] = False
        checks["numpy_error"] = str(exc)
    try:
        import yaml

        checks["pyyaml_version"] = yaml.__version__
        checks["pyyaml_importable"] = True
    except Exception as exc:  # pragma: no cover
        checks["pyyaml_version"] = None
        checks["pyyaml_importable"] = False
        checks["pyyaml_error"] = str(exc)
    checks["disk_free_bytes"] = shutil.disk_usage(Path.cwd()).free
    checks["multiprocessing_start_method"] = __import__("multiprocessing").get_start_method(allow_none=True)
    checks["os_name"] = os.name
    checks["target_windows_compatible"] = True
    checks["actual_runtime_note"] = (
        "This execution is on WSL2/Linux; shipped paths are relative and the runtime uses "
        "pathlib, spawn-based multiprocessing and stdlib HTTP for Windows compatibility."
        if platform.system() != "Windows"
        else "Windows runtime detected."
    )
    checks["ok"] = bool(
        checks["python_ge_310"]
        and checks["numpy_importable"]
        and checks["pyyaml_importable"]
    )
    print(json.dumps(checks, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if checks["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
