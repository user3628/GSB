"""Local deterministic generative-image editing pipeline.

The public entry points are :mod:`gspipe.cli` and :mod:`gspipe.pipeline`.
No model, GPU, network access, or container runtime is required.
"""

from __future__ import annotations

__version__ = "1.0.0"

from .errors import PipelineError  # noqa: E402

__all__ = ["PipelineError", "__version__"]
