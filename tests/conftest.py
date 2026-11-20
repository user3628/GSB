import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)


@pytest.fixture()
def tiny_workspace(tmp_path):
    return str(tmp_path / "ws")


@pytest.fixture()
def data_dir(tmp_path):
    return str(tmp_path / "data")
