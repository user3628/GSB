from __future__ import annotations

import unittest

from dimcluster.envcheck import environment_check

from .helpers import workspace


class EnvironmentTests(unittest.TestCase):
    def test_atomic_replace_lock_and_loopback(self) -> None:
        root = workspace()
        result = environment_check(root)
        self.assertTrue(result["ok"])
        self.assertTrue(result["workspace"]["atomic_replace_probe"])
        self.assertTrue(result["workspace"]["file_lock_probe"])
        self.assertTrue(result["http"]["loopback_bind_probe"])
        self.assertTrue(result["python"]["meets_3_11"])


if __name__ == "__main__":
    unittest.main()
