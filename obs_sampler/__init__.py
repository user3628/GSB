"""Deterministic, crash-recoverable sampling coordinator for local telemetry."""

from .errors import BusinessError
from .policy import Policy, PolicyError
from .service import SamplerService

__all__ = ["BusinessError", "Policy", "PolicyError", "SamplerService"]
__version__ = "1.0.0"
