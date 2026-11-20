"""Local columnar vectorized expression execution engine."""

from .errors import BusinessError, PlanError
from .model import Dataset, DatasetRegistry, DatasetSpec

__all__ = [
    "BusinessError",
    "Dataset",
    "DatasetRegistry",
    "DatasetSpec",
    "PlanError",
]

