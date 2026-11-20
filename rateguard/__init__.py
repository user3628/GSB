"""RateGuard: deterministic adaptive limiting and circuit breaking."""

from .config import GuardConfig
from .engine import Engine
from .errors import BusinessError

__all__ = ["BusinessError", "Engine", "GuardConfig", "__version__"]
__version__ = "1.0.0"

