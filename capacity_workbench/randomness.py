from __future__ import annotations

import math


_MASK64 = (1 << 64) - 1


class SplitMix64:
    """Small, explicitly specified PRNG so generated samples are reproducible."""

    def __init__(self, seed: int) -> None:
        self._state = seed & _MASK64
        self._gauss_spare: float | None = None

    def next_u64(self) -> int:
        self._state = (self._state + 0x9E3779B97F4A7C15) & _MASK64
        z = self._state
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _MASK64
        return (z ^ (z >> 31)) & _MASK64

    def random(self) -> float:
        # 53 random bits, matching the conventional [0, 1) double resolution.
        return (self.next_u64() >> 11) * (1.0 / (1 << 53))

    def gauss(self) -> float:
        if self._gauss_spare is not None:
            value = self._gauss_spare
            self._gauss_spare = None
            return value
        u1 = max(self.random(), 1e-15)
        u2 = self.random()
        radius = math.sqrt(-2.0 * math.log(u1))
        angle = 2.0 * math.pi * u2
        self._gauss_spare = radius * math.sin(angle)
        return radius * math.cos(angle)


def derive_sample_nonce(
    seed: int, stage_index: int, attempt: int, request_index: int
) -> int:
    mixed = (
        (seed & _MASK64)
        ^ (((stage_index + 1) * 0xD1B54A32D192ED03) & _MASK64)
        ^ (((attempt + 1) * 0x94D049BB133111EB) & _MASK64)
        ^ (((request_index + 1) * 0x9E3779B97F4A7C15) & _MASK64)
    )
    return SplitMix64(mixed).next_u64()

