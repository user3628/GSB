#!/usr/bin/env python3
"""Check the locked runtime dependencies before local Windows execution."""

from __future__ import annotations

import sys


def main() -> int:
    import numpy
    import PIL
    import pytest

    print(
        {
            "python": sys.version.split()[0],
            "numpy": numpy.__version__,
            "Pillow": PIL.__version__,
            "pytest": pytest.__version__,
        }
    )
    if sys.version_info[:2] < (3, 11):
        raise SystemExit("Python 3.11 or newer is required")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

