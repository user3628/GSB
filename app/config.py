from __future__ import annotations

import json
import os
from pathlib import Path

from app.models import GatewaySettings


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = ROOT / "var" / "gateway.sqlite3"
DEFAULT_BACKENDS_PATH = ROOT / "samples" / "backends.json"


def load_settings(path: str | Path | None = None) -> GatewaySettings:
    settings_path = Path(path or os.environ.get("GATEWAY_BACKENDS_FILE", DEFAULT_BACKENDS_PATH))
    with settings_path.open("r", encoding="utf-8") as handle:
        return GatewaySettings.model_validate(json.load(handle))


def database_path(path: str | Path | None = None) -> Path:
    return Path(path or os.environ.get("GATEWAY_DB", DEFAULT_DB_PATH))
