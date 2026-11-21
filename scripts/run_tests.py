"""Convenience launcher: runs the whole unittest suite from the repo root."""
import os
import sys
import unittest


if __name__ == "__main__":
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    sys.path.insert(0, repo_root)
    loader = unittest.TestLoader()
    suite = loader.discover(os.path.join(repo_root, "tests"))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
