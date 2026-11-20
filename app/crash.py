from __future__ import annotations

import os
import sys
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CrashController:
    """Deterministic process-abort hook used by recovery integration tests.

    Production runs leave the point unset. Tests set DWE_CRASH_POINT and
    optionally DWE_CRASH_BATCH_ID / DWE_CRASH_KIND. The process exits with
    code 137, so no Python cleanup, shutdown handler, or response is produced.
    """

    point: str | None = None
    batch_id: str | None = None
    kind: str | None = None
    exit_code: int = 137

    @classmethod
    def from_env(cls) -> "CrashController":
        return cls(
            point=os.environ.get("DWE_CRASH_POINT") or None,
            batch_id=os.environ.get("DWE_CRASH_BATCH_ID") or None,
            kind=os.environ.get("DWE_CRASH_KIND") or None,
        )

    def maybe_crash(self, point: str, *, batch_id: str | None = None, kind: str | None = None) -> None:
        if not self.point or point != self.point:
            return
        if self.batch_id is not None and batch_id != self.batch_id:
            return
        if self.kind is not None and kind != self.kind:
            return
        try:
            sys.stderr.write(f"[dwe] deterministic hard crash at {point}; intent={batch_id or '-'}\n")
            sys.stderr.flush()
            sys.stdout.flush()
        finally:
            os._exit(self.exit_code)
