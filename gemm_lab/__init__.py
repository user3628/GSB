"""Blocked GEMM/BMM operator library with deterministic validation.

The public API is intentionally small.  Use :func:`gemm`, :func:`bmm`, and
:class:`OperatorWorkspace` for library integration, or ``python -m gemm_lab``
for the command line interface.
"""

from .api import ComputeResult, OperatorWorkspace, bmm, gemm, select_plan
from .blocking import BlockPlan, PlanSelection
from .errors import BusinessCode, GemmError
from .kernel import SumStrategy

__all__ = [
    "BlockPlan",
    "BusinessCode",
    "ComputeResult",
    "GemmError",
    "OperatorWorkspace",
    "PlanSelection",
    "SumStrategy",
    "bmm",
    "gemm",
    "select_plan",
]

__version__ = "1.0.0"

