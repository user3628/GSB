"""Loopback recursive DNS middleware used by local network diagnostics."""

from .codec import (
    ParsedRequest,
    QueryKey,
    canonical_name,
    parse_request,
    parse_response,
    render_response,
)
from .errors import BusinessError, E_MALFORMED_MESSAGE
from .model import Record, Resolution

__all__ = [
    "BusinessError",
    "E_MALFORMED_MESSAGE",
    "ParsedRequest",
    "QueryKey",
    "Record",
    "Resolution",
    "canonical_name",
    "parse_request",
    "parse_response",
    "render_response",
]

__version__ = "1.0.0"
