from __future__ import annotations

import json
import platform
import sqlite3
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    result = {
        "python_version": platform.python_version(),
        "python_executable": sys.executable,
        "python_supported": sys.version_info >= (3, 11),
        "platform": platform.platform(),
        "sqlite_version": sqlite3.sqlite_version,
        "project_root": str(root),
        "dependencies": {},
    }
    try:
        import numpy
    except ImportError as exc:
        result["dependencies"]["numpy"] = f"missing: {exc}"
    else:
        result["dependencies"]["numpy"] = numpy.__version__
    try:
        import requests
    except ImportError as exc:
        result["dependencies"]["requests"] = f"missing: {exc}"
    else:
        result["dependencies"]["requests"] = requests.__version__
    result["ok"] = bool(
        result["python_supported"]
        and all(not str(value).startswith("missing:") for value in result["dependencies"].values())
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
