"""LLM pretraining data mixing & dedup pipeline.

Stages: normalize -> dedup -> mix, with persistent checkpoints,
crash recovery, version-based invalidation and concurrent-safe publish.
"""

__version__ = "1.0.0"
