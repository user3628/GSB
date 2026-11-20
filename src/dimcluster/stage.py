from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ArtifactBundle:
    artifacts: dict[str, bytes]
    metadata: dict[str, Any]
