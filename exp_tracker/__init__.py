"""Local training experiment tracking component.

A self-contained, dependency-light experiment tracker supporting two
interchangeable persistence formats (a compact binary format and sharded
JSON-lines), version negotiation, derived metric/parameter caches, atomic
snapshot replacement, single-writer concurrency and crash recovery.
"""

from .errors import TrackerError, ErrorCode

__all__ = ["TrackerError", "ErrorCode"]
__version__ = "1.0.0"
