from __future__ import annotations

from capacity_workbench.randomness import SplitMix64
from capacity_workbench.workloads import WorkloadRegistry, generate_sample


def test_same_seed_generates_same_samples_and_distributions_differ() -> None:
    registry = WorkloadRegistry.load()
    first = [
        generate_sample(registry.get("stable"), 4, nonce)
        for nonce in range(20)
    ]
    second = [
        generate_sample(registry.get("stable"), 4, nonce)
        for nonce in range(20)
    ]
    assert first == second

    lognormal = generate_sample(registry.get("stable"), 1, 99)
    normal = generate_sample(registry.get("normal_tail"), 1, 99)
    uniform = generate_sample(registry.get("timeout_mode"), 1, 99)
    assert {lognormal.kind, normal.kind, uniform.kind} == {
        "lognormal",
        "normal",
        "uniform",
    }
    assert all(sample.latency_ms > 0 for sample in (lognormal, normal, uniform))


def test_splitmix64_is_reproducible() -> None:
    first = SplitMix64(42)
    second = SplitMix64(42)
    assert [first.random() for _ in range(8)] == [
        second.random() for _ in range(8)
    ]
    first = SplitMix64(42)
    second = SplitMix64(42)
    assert [first.next_u64() for _ in range(8)] == [
        second.next_u64() for _ in range(8)
    ]

