"""LeaseFair: deterministic local lease-lock middleware."""

from .clock import Clock, FixedClock
from .service import LeaseService

__all__ = ["Clock", "FixedClock", "LeaseService"]
__version__ = "0.1.0"

