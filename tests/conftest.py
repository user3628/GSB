from __future__ import annotations

from pathlib import Path

import pytest

from qos_workbench.models import Policy, PolicySnapshot


@pytest.fixture
def snapshot() -> PolicySnapshot:
    timestamp = 1_700_000_000_123_456
    policies = (
        Policy("wan", 100_000_000, 200_000_000, 262_144, 524_288, 0, "all", None, timestamp),
        Policy("voice", 2_000_000, 4_000_000, 4_096, 8_192, 1, "udp/5060", "wan", timestamp),
        Policy("video", 10_000_000, 20_000_000, 32_768, 65_536, 2, "udp/5004", "wan", timestamp),
        Policy("bulk", 20_000_000, 40_000_000, 65_536, 131_072, 7, "tcp/443", "wan", timestamp),
    )
    return PolicySnapshot(
        format_version=3,
        snapshot_no=1,
        generated_at_us=timestamp,
        policies=tuple(sorted(policies, key=lambda policy: policy.queue_id)),
    )


@pytest.fixture
def small_snapshot() -> PolicySnapshot:
    return PolicySnapshot(
        format_version=3,
        snapshot_no=1,
        generated_at_us=1_700_000_000_000_000,
        policies=(
            Policy(
                queue_id="q",
                cir_bps=8_000,
                pir_bps=16_000,
                cbs_bytes=1_000,
                pbs_bytes=2_000,
                priority=1,
                match_rule="all",
                parent_queue_id=None,
                timestamp_us=1_700_000_000_000_000,
            ),
        ),
    )


@pytest.fixture
def sample_paths() -> Path:
    return Path(__file__).resolve().parent.parent / "samples"

