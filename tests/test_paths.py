from pathlib import Path

from prefalign.core.config import Config
from prefalign.core.paths import resolve, to_windows


def test_to_windows_uses_backslashes():
    assert to_windows("workspace/state/manifest.json") == "workspace\\state\\manifest.json"
    assert to_windows("workspace\\state") == "workspace\\state"


def test_resolve_relative_under_root():
    native = resolve("workspace\\state\\manifest.json", Path("/proj"))
    assert native == Path("/proj/workspace/state/manifest.json")


def test_resolve_drive_path_on_posix():
    native = resolve("C:\\data\\shards", Path("/proj"))
    assert native == Path("/proj/data/shards")


def test_config_storage_paths_are_windows_style(workspace):
    cfg = Config.load(workspace / "config" / "default.yaml", workspace)
    for key, value in cfg.raw["storage"].items():
        assert "/" not in value, f"{key} must use Windows backslash paths"
    assert cfg.path("shards_dir") == workspace / "data" / "shards"
