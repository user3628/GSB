"""Local service registry middleware.

The package deliberately keeps format handling, normalization, persistence,
and probe execution in separate modules.  The public entry point is
:class:`svc_registry.service.RegistryService`.
"""

from .errors import BusinessError
from .models import NormalizedProbeResult, NormalizedRegistration

__all__ = [
    "BusinessError",
    "NormalizedProbeResult",
    "NormalizedRegistration",
]

__version__ = "1.0.0"
