import os
import shutil
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pkq.config import Config
from tools.gen_sample import make_snapshot


@pytest.fixture(scope="session")
def sample_root(tmp_path_factory):
    root = str(tmp_path_factory.mktemp("sample"))
    make_snapshot(os.path.join(root, "snapshot_v1"), 1)
    make_snapshot(os.path.join(root, "snapshot_v2"), 2)
    make_snapshot(os.path.join(root, "snapshot_corrupt"), 3, corrupt_block=5)
    yield root
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture()
def cfg():
    return Config()


@pytest.fixture()
def small_cfg():
    return Config(max_concurrent_tasks=2, task_memory_budget_bytes=4096,
                  round_cost_budget=10_000, scan_queue_capacity=8,
                  max_page_size=128, default_page_size=16)
