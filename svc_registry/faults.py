from __future__ import annotations

import os
import signal


def maybe_crash(point: str) -> None:
    """Simulate a forced process termination for recovery tests.

    This is deliberately opt-in.  Normal server processes ignore the hook
    unless REGISTRY_FAULT_POINT is set to the exact checkpoint name.
    """

    if os.environ.get("REGISTRY_FAULT_POINT") != point:
        return
    if os.name == "nt":
        os._exit(137)
    os.kill(os.getpid(), signal.SIGKILL)
