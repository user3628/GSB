from __future__ import annotations

import importlib
import platform
import sys
from importlib.metadata import PackageNotFoundError, version


REQUIRED_MODULES = ("fastapi", "uvicorn", "numpy", "pytest", "httpx")
PACKAGE_NAMES = {
    "fastapi": "fastapi",
    "uvicorn": "uvicorn",
    "numpy": "numpy",
    "pytest": "pytest",
    "httpx": "httpx",
}


def main() -> int:
    print(f"Python: {sys.version.split()[0]} ({platform.platform()})")
    if sys.version_info < (3, 11):
        print("ERROR: Python 3.11 or newer is required.")
        return 1
    failures: list[str] = []
    for module_name in REQUIRED_MODULES:
        try:
            importlib.import_module(module_name)
            package_version = version(PACKAGE_NAMES[module_name])
            print(f"OK     {module_name:8s} {package_version}")
        except (ImportError, PackageNotFoundError) as exc:
            print(f"MISSING {module_name}: {exc}")
            failures.append(module_name)
    if failures:
        print("Install dependencies with: python -m pip install -r requirements.txt")
        return 1
    print("Environment checks passed. Use scripts/check_ports.py before startup.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
