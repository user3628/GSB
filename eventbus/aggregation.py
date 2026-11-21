from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(slots=True)
class NeumaierSum:
    """Deterministic compensated sum used by the aggregate pipeline."""

    total: float = 0.0
    compensation: float = 0.0

    def add(self, value: float | None) -> None:
        x = 0.0 if value is None else float(value)
        t = self.total + x
        if abs(self.total) >= abs(x):
            self.compensation += (self.total - t) + x
        else:
            self.compensation += (x - t) + self.total
        self.total = t

    @property
    def value(self) -> float:
        return self.total + self.compensation


@dataclass(slots=True)
class Aggregate:
    key: str
    count: int = 0
    total: float = 0.0
    compensation: float = 0.0

    def add(self, measure: float | None) -> None:
        accumulator = NeumaierSum(self.total, self.compensation)
        accumulator.add(measure)
        self.total = accumulator.total
        self.compensation = accumulator.compensation
        self.count += 1

    @property
    def value(self) -> float:
        return self.total + self.compensation

    def as_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "count": self.count,
            "sum": self.value,
            "compensation": self.compensation,
        }


def aggregate_rows(rows: Iterable[tuple[str, float | None]]) -> dict[str, Aggregate]:
    result: dict[str, Aggregate] = {}
    for key, measure in rows:
        result.setdefault(key, Aggregate(key=key)).add(measure)
    return result


def ordered_aggregates(rows: Iterable[tuple[str, float | None]]) -> list[dict[str, object]]:
    return [result.as_dict() for _, result in sorted(aggregate_rows(rows).items())]


def combine_aggregates(groups: Sequence[Sequence[tuple[str, float | None]]]) -> list[dict[str, object]]:
    """Reduce the same event multiset split into arbitrary batches."""

    flattened = [row for batch in groups for row in batch]
    return ordered_aggregates(flattened)
