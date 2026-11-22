"""WSGI entry point: ``flask --app app run`` or ``python -m quota_scheduler serve``."""

from __future__ import annotations

import os
from pathlib import Path

from quota_scheduler.api import create_app


app = create_app(
    os.environ.get("QUOTA_SCHEDULER_DB", str(Path.cwd() / "quota_scheduler.db")),
    sample_dir=os.environ.get("QUOTA_SCHEDULER_SAMPLES", "sample_data"),
    bootstrap=os.environ.get("QUOTA_SCHEDULER_BOOTSTRAP", "1") == "1",
    load_requests=os.environ.get("QUOTA_SCHEDULER_LOAD_REQUESTS", "0") == "1",
    auto_admit=os.environ.get("QUOTA_SCHEDULER_AUTO_ADMIT", "0") == "1",
    recover_on_start=os.environ.get("QUOTA_SCHEDULER_RECOVER_ON_START", "1") == "1",
    abort_running_on_start=os.environ.get(
        "QUOTA_SCHEDULER_ABORT_RUNNING_ON_START", "1"
    ) == "1",
)

