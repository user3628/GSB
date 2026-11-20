"""Shared test helpers."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from rerank_bench.generator import DEFAULT_SEED, generate  # noqa: E402
from rerank_bench.pipeline import Workspace  # noqa: E402


class WorkspaceTestCase(unittest.TestCase):
    """Base class providing a temp dir with generated sample data."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="rerank-bench-test-")
        self.data_dir = os.path.join(self.tmp, "data")
        generate(self.data_dir, seed=DEFAULT_SEED, num_shards=4)
        self.ws_root = os.path.join(self.tmp, "workspace")
        self.ws = Workspace(self.ws_root)
        self.ws.import_data(self.data_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)
