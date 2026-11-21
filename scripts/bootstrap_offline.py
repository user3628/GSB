#!/usr/bin/env python3
"""Create the local virtual environment without contacting a package index."""

from __future__ import annotations

import argparse
import subprocess
import sys
import venv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENV = ROOT / ".venv"
REQUIREMENTS = ROOT / "requirements.txt"
MINIMUM = (3, 11)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--wheelhouse",
        type=Path,
        help="Optional directory containing offline wheel files.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Recreate .venv. The default refuses to overwrite an existing environment.",
    )
    args = parser.parse_args()

    if sys.version_info < MINIMUM:
        raise SystemExit(f"Python {MINIMUM[0]}.{MINIMUM[1]}+ is required, got {sys.version}")
    if VENV.exists() and not args.replace:
        print(f"Using existing virtual environment: {VENV}")
    else:
        if VENV.exists():
            import shutil

            shutil.rmtree(VENV)
        # The execution environment provides the pinned packages in its local
        # system site-packages. system_site_packages keeps setup fully offline.
        builder = venv.EnvBuilder(with_pip=True, system_site_packages=True)
        builder.create(VENV)
        print(f"Created virtual environment: {VENV}")

    python = VENV / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    pip_arguments = [str(python), "-m", "pip", "install", "--no-index", "--disable-pip-version-check"]
    if args.wheelhouse is not None:
        pip_arguments.extend(["--find-links", str(args.wheelhouse.resolve())])
    pip_arguments.extend(["-r", str(REQUIREMENTS)])
    subprocess.run(pip_arguments, check=True)
    subprocess.run(
        [
            str(python),
            "-c",
            (
                "import fastapi, uvicorn, numpy, pytest, sqlite3, sys; "
                "print('python', sys.version.split()[0]); "
                "print('fastapi', fastapi.__version__); "
                "print('uvicorn', uvicorn.__version__); "
                "print('numpy', numpy.__version__); "
                "print('pytest', pytest.__version__); "
                "print('sqlite', sqlite3.sqlite_version)"
            ),
        ],
        check=True,
    )


if __name__ == "__main__":
    main()
