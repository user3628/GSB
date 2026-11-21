import os
import shutil
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from generate_samples import main as generate_main


@pytest.fixture()
def work_dir(tmp_path):
    target = tmp_path / "work"
    generate_main(str(target))
    return str(target)
