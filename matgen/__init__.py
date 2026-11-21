"""Local deterministic texture/material generation component."""

from .config import ALGORITHM_VERSION, DEFAULT_CONFIG, load_config, normalize_config
from .diagnostics import build_diagnostics
from .errors import (
    E_DIM_MISMATCH,
    E_EMPTY_PROMPT,
    E_INVALID_CONFIG,
    E_INVALID_RECIPE,
    E_INVALID_STAGE,
    E_IO,
    E_NUMERIC,
    E_OCTAVE_RANGE,
    E_PALETTE_INVALID,
    E_STALE_CHECKPOINT,
    MatgenError,
)
from .noise import compose_noise
from .palette import build_palette_artifacts, load_palette
from .pipeline import (
    clear_stop,
    generate_all,
    get_error_report,
    get_material_result,
    get_status,
    get_versions,
    load_recipes,
    request_stop,
)
from .prompt import generate_control_vector


__all__ = [
    "ALGORITHM_VERSION",
    "DEFAULT_CONFIG",
    "E_DIM_MISMATCH",
    "E_EMPTY_PROMPT",
    "E_INVALID_CONFIG",
    "E_INVALID_RECIPE",
    "E_INVALID_STAGE",
    "E_IO",
    "E_NUMERIC",
    "E_OCTAVE_RANGE",
    "E_PALETTE_INVALID",
    "E_STALE_CHECKPOINT",
    "MatgenError",
    "build_diagnostics",
    "build_palette_artifacts",
    "clear_stop",
    "compose_noise",
    "generate_all",
    "generate_control_vector",
    "get_error_report",
    "get_material_result",
    "get_status",
    "get_versions",
    "load_config",
    "load_palette",
    "load_recipes",
    "normalize_config",
    "request_stop",
]

