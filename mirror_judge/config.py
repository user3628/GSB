from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parent.parent


def _is_loopback_host(host: str | None) -> bool:
    return (host or "").lower() in {"127.0.0.1", "localhost", "::1"}


@dataclass(frozen=True)
class Settings:
    db_path: Path
    replay_target_url: str
    test_hooks_enabled: bool = False
    crash_point: str = ""

    @classmethod
    def from_env(cls) -> "Settings":
        db_path = Path(os.environ.get("MIRROR_DB_PATH", ROOT / "runtime" / "mirror_judge.db"))
        target = os.environ.get("MIRROR_REPLAY_TARGET", "http://127.0.0.1:8765/_test/replay")
        parsed = urlsplit(target)
        if parsed.scheme != "http" or not _is_loopback_host(parsed.hostname):
            raise ValueError("MIRROR_REPLAY_TARGET must be an http URL on a loopback address")
        enabled = os.environ.get("MIRROR_ENABLE_TEST_HOOKS", "0") == "1"
        return cls(
            db_path=db_path,
            replay_target_url=target,
            test_hooks_enabled=enabled,
            crash_point=os.environ.get("MIRROR_CRASH_POINT", "before_commit"),
        )

    def ensure_directories(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
