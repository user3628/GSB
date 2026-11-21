from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Settings:
    root: Path
    data_dir: Path
    artifact_dir: Path
    db_path: Path
    staging_dir: Path
    published_dir: Path
    verification_dir: Path
    host: str = "127.0.0.1"
    port: int = 8767
    batch_wait_timeout_seconds: float = 120.0
    policy_lock_timeout_seconds: float = 120.0
    poll_interval_seconds: float = 0.05
    test_stage_delay_seconds: float = 0.0

    @classmethod
    def from_env(cls, root: str | Path | None = None) -> "Settings":
        base = Path(root or os.environ.get("PREP_ROOT", Path.cwd())).resolve()
        artifact_dir = Path(os.environ.get("PREP_ARTIFACT_DIR", base / "artifacts")).resolve()
        db_path = Path(os.environ.get("PREP_DB_PATH", artifact_dir / "pipeline.sqlite3")).resolve()
        return cls(
            root=base,
            data_dir=Path(os.environ.get("PREP_DATA_DIR", base / "data")).resolve(),
            artifact_dir=artifact_dir,
            db_path=db_path,
            staging_dir=artifact_dir / "staging",
            published_dir=artifact_dir / "batches",
            verification_dir=artifact_dir / "verification",
            host=os.environ.get("PREP_HOST", "127.0.0.1"),
            port=int(os.environ.get("PREP_PORT", "8767")),
            batch_wait_timeout_seconds=float(
                os.environ.get("PREP_BATCH_WAIT_TIMEOUT_SECONDS", "120")
            ),
            policy_lock_timeout_seconds=float(
                os.environ.get("PREP_POLICY_LOCK_TIMEOUT_SECONDS", "120")
            ),
            test_stage_delay_seconds=float(os.environ.get("PREP_TEST_STAGE_DELAY_SECONDS", "0")),
        )

    def ensure_directories(self) -> None:
        for directory in (
            self.root,
            self.data_dir,
            self.artifact_dir,
            self.db_path.parent,
            self.staging_dir,
            self.published_dir,
            self.verification_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
