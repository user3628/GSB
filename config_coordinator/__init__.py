"""Multi-cluster configuration distribution coordinator.

The package deliberately keeps the domain layer independent from FastAPI.  The
HTTP application is a thin adapter around :class:`Coordinator`; the same
component can be embedded in another service or exercised from a CLI.
"""

from .api import create_app
from .coordinator import Coordinator
from .errors import DomainError
from .models import (
    ComponentDeclaration,
    DispatchRequest,
    IncrementRequest,
    Snapshot,
    SnapshotSubmitRequest,
    TransitionRequest,
)

__all__ = [
    "ComponentDeclaration",
    "Coordinator",
    "DispatchRequest",
    "DomainError",
    "IncrementRequest",
    "Snapshot",
    "SnapshotSubmitRequest",
    "TransitionRequest",
    "create_app",
]

