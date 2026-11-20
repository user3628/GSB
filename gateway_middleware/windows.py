from __future__ import annotations

import bisect
import hashlib
import math
from array import array
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from .config import Settings
from .models import MAX_STORED_INT, EventInput, WindowEstimate

EXACT_METHOD = "exact"
RESERVOIR_METHOD = "deterministic_hash_reservoir"
SAMPLING_DOMAIN = b"gw-window-v1"


@dataclass(slots=True, frozen=True)
class WindowSpec:
    name: str
    length_ms: int
    step_ms: int
    exact_limit: int


@dataclass(slots=True)
class SeriesItem:
    timestamp_ms: int
    weight: int
    request_id: str
    event_seq: int
    existing: bool
    admitted: bool
    global_seq: int
    event: EventInput | None = None
    sort_key: tuple[int, int, int, str] | None = None
    index: int = -1
    sample_hash: int = 0


class Fenwick:
    __slots__ = ("values", "size")

    def __init__(self, values: Iterable[int]) -> None:
        data = list(values)
        self.size = len(data)
        self.values = array("q", data)
        for index in range(1, self.size + 1):
            parent = index + (index & -index)
            if parent <= self.size:
                self.values[parent - 1] += self.values[index - 1]

    def add(self, index: int, delta: int) -> None:
        if index < 0 or index >= self.size:
            raise IndexError("fenwick index out of range")
        current = index + 1
        while current <= self.size:
            self.values[current - 1] += delta
            current += current & -current

    def prefix(self, end_exclusive: int) -> int:
        if end_exclusive <= 0:
            return 0
        if end_exclusive > self.size:
            end_exclusive = self.size
        total = 0
        current = end_exclusive
        while current > 0:
            total += self.values[current - 1]
            current -= current & -current
        return total

    def range_sum(self, start: int, end_exclusive: int) -> int:
        return self.prefix(end_exclusive) - self.prefix(start)


@dataclass(slots=True, frozen=True)
class SamplingPolicy:
    method: str
    sample_rate_num: int
    sample_rate_den: int
    exact_limit: int
    max_window_events: int

    @property
    def sample_rate(self) -> float:
        return self.sample_rate_num / self.sample_rate_den


def _sampling_denominator(max_events: int, exact_limit: int) -> int:
    if max_events <= exact_limit:
        return 1
    denominator = 1
    while max_events > exact_limit * denominator:
        denominator <<= 1
        if denominator > (1 << 62):
            break
    return denominator


def _max_window_events(timestamps: Sequence[int], length_ms: int) -> int:
    maximum = 0
    left = 0
    for right, timestamp in enumerate(timestamps):
        while left <= right and timestamps[left] <= timestamp - length_ms:
            left += 1
        maximum = max(maximum, right - left + 1)
    return maximum


def _sample_hash(secret: str, item: SeriesItem) -> int:
    payload = f"{secret}|{item.request_id}|{item.timestamp_ms}|{item.weight}".encode("utf-8")
    return int.from_bytes(
        hashlib.blake2b(payload, digest_size=8, person=SAMPLING_DOMAIN).digest(),
        "big",
        signed=False,
    )


def _is_sampled(sample_hash: int, denominator: int) -> bool:
    if denominator <= 1:
        return True
    level = denominator.bit_length() - 1
    if level <= 0:
        return True
    if level >= 64:
        return sample_hash == 0
    return (sample_hash >> (64 - level)) == 0


class WindowSeries:
    """One route/client pair with both window views over a canonical event order.

    Prefix arrays make a sliding-window estimate O(log n) for boundary lookup and
    O(1) for integer sums. Exact decisions never use a sampled estimate.
    """

    __slots__ = (
        "route_key",
        "client_id",
        "settings",
        "short_spec",
        "long_spec",
        "items",
        "timestamps",
        "weights",
        "prefix_weight",
        "sample_prefixes",
        "policies",
        "admitted_fenwick",
        "pending_items",
        "secret",
    )

    def __init__(
        self,
        *,
        route_key: str,
        client_id: str,
        existing_rows: Iterable[dict[str, Any]],
        new_events: Iterable[EventInput],
        settings: Settings,
        secret: str,
    ) -> None:
        self.route_key = route_key
        self.client_id = client_id
        self.settings = settings
        self.short_spec = WindowSpec(
            "short", settings.short_length_ms, settings.short_step_ms, settings.short_exact_limit
        )
        self.long_spec = WindowSpec(
            "long", settings.long_length_ms, settings.long_step_ms, settings.long_exact_limit
        )
        self.secret = secret

        items: list[SeriesItem] = []
        for row in existing_rows:
            items.append(
                SeriesItem(
                    timestamp_ms=int(row["timestamp_ms"]),
                    weight=int(row["weight"]),
                    request_id=str(row["request_id"]),
                    event_seq=int(row["event_seq"]),
                    existing=True,
                    admitted=bool(row["admitted"]),
                    global_seq=int(row["global_seq"]),
                )
            )
        pending_items: dict[str, SeriesItem] = {}
        for event in new_events:
            item = SeriesItem(
                timestamp_ms=event.timestamp_ms,
                weight=event.weight,
                request_id=event.request_id,
                event_seq=event.event_seq,
                existing=False,
                admitted=False,
                global_seq=0,
                event=event,
            )
            items.append(item)
            pending_items[event.request_id] = item

        for item in items:
            item.sort_key = (
                item.timestamp_ms,
                0 if item.existing else 1,
                item.global_seq if item.existing else item.event_seq,
                item.request_id,
            )
        items.sort(key=lambda item: item.sort_key)
        for index, item in enumerate(items):
            item.index = index
            item.sample_hash = _sample_hash(secret, item)

        self.items = items
        self.pending_items = pending_items
        self.timestamps = array("q", (item.timestamp_ms for item in items))
        self.weights = array("q", (item.weight for item in items))
        self.prefix_weight = array("q", [0])
        running = 0
        for weight in self.weights:
            running += weight
            self.prefix_weight.append(running)

        max_short = _max_window_events(self.timestamps, self.short_spec.length_ms)
        max_long = _max_window_events(self.timestamps, self.long_spec.length_ms)
        short_den = _sampling_denominator(max_short, self.short_spec.exact_limit)
        long_den = _sampling_denominator(max_long, self.long_spec.exact_limit)
        self.policies = {
            "short": SamplingPolicy(RESERVOIR_METHOD, 1, short_den, self.short_spec.exact_limit, max_short),
            "long": SamplingPolicy(RESERVOIR_METHOD, 1, long_den, self.long_spec.exact_limit, max_long),
        }

        self.sample_prefixes: dict[str, tuple[array, array, array]] = {}
        for spec in (self.short_spec, self.long_spec):
            denominator = self.policies[spec.name].sample_rate_den
            count_prefix = array("q", [0])
            sum_prefix = array("q", [0])
            square_prefix = array("q", [0])
            if denominator == 1:
                # Exact path still keeps these arrays for a uniform O(1) estimator.
                for weight in self.weights:
                    count_prefix.append(count_prefix[-1] + 1)
                    sum_prefix.append(sum_prefix[-1] + weight)
                    square_prefix.append(square_prefix[-1] + weight * weight)
            else:
                for item in items:
                    selected = _is_sampled(item.sample_hash, denominator)
                    count_prefix.append(count_prefix[-1] + (1 if selected else 0))
                    sum_prefix.append(sum_prefix[-1] + (item.weight if selected else 0))
                    square_prefix.append(
                        square_prefix[-1] + (item.weight * item.weight if selected else 0)
                    )
            self.sample_prefixes[spec.name] = (count_prefix, sum_prefix, square_prefix)

        admitted_values = (item.weight if item.existing and item.admitted else 0 for item in items)
        self.admitted_fenwick = Fenwick(admitted_values)

    def _bounds_for_index(self, index: int, length_ms: int) -> tuple[int, int]:
        timestamp = self.timestamps[index]
        left = bisect.bisect_right(self.timestamps, timestamp - length_ms)
        right = index + 1
        return left, right

    def _margin(self, variance: int) -> int:
        if variance <= 0:
            return 0
        scale = self.settings.fixed_point_scale
        root_scaled = math.isqrt(variance * scale * scale)
        numerator = self.settings.confidence_z_ppm * root_scaled
        denominator = scale * scale
        return (numerator + denominator - 1) // denominator

    def _estimate_range(self, *, window_name: str, left: int, right: int) -> WindowEstimate:
        spec = self.short_spec if window_name == "short" else self.long_spec
        event_count = right - left
        exact_weight = self.prefix_weight[right] - self.prefix_weight[left]
        policy = self.policies[window_name]

        if event_count <= spec.exact_limit:
            return WindowEstimate(
                window=window_name,
                event_count=event_count,
                exact_weight=exact_weight,
                estimated_weight=exact_weight,
                confidence_low=exact_weight,
                confidence_high=exact_weight,
                sample_rate_num=1,
                sample_rate_den=1,
                method=EXACT_METHOD,
                exact_limit=spec.exact_limit,
                max_window_events=policy.max_window_events,
                variance=0,
            )

        _, sum_prefix, square_prefix = self.sample_prefixes[window_name]
        sampled_weight = sum_prefix[right] - sum_prefix[left]
        sampled_square = square_prefix[right] - square_prefix[left]
        denominator = policy.sample_rate_den
        estimated_weight = sampled_weight * denominator
        variance = (denominator - 1) * denominator * sampled_square
        margin = self._margin(variance)
        return WindowEstimate(
            window=window_name,
            event_count=event_count,
            exact_weight=exact_weight,
            estimated_weight=estimated_weight,
            confidence_low=max(0, estimated_weight - margin),
            confidence_high=estimated_weight + margin,
            sample_rate_num=1,
            sample_rate_den=denominator,
            method=RESERVOIR_METHOD,
            exact_limit=spec.exact_limit,
            max_window_events=policy.max_window_events,
            variance=min(variance, MAX_STORED_INT),
        )

    def estimate(self, request_id: str, window_name: str) -> WindowEstimate:
        item = self.pending_items.get(request_id)
        if item is None:
            raise KeyError(f"request {request_id!r} is not a pending event in this series")
        spec = self.short_spec if window_name == "short" else self.long_spec
        left, right = self._bounds_for_index(item.index, spec.length_ms)
        return self._estimate_range(window_name=window_name, left=left, right=right)

    def estimate_at(self, timestamp_ms: int, window_name: str) -> WindowEstimate:
        spec = self.short_spec if window_name == "short" else self.long_spec
        left = bisect.bisect_right(self.timestamps, timestamp_ms - spec.length_ms)
        right = bisect.bisect_right(self.timestamps, timestamp_ms)
        return self._estimate_range(window_name=window_name, left=left, right=right)

    def admitted_weight(self, request_id: str, window_name: str) -> int:
        item = self.pending_items.get(request_id)
        if item is None:
            raise KeyError(request_id)
        spec = self.short_spec if window_name == "short" else self.long_spec
        left, right = self._bounds_for_index(item.index, spec.length_ms)
        return self.admitted_fenwick.range_sum(left, right)

    def admitted_at(self, timestamp_ms: int, window_name: str) -> int:
        spec = self.short_spec if window_name == "short" else self.long_spec
        left = bisect.bisect_right(self.timestamps, timestamp_ms - spec.length_ms)
        right = bisect.bisect_right(self.timestamps, timestamp_ms)
        return self.admitted_fenwick.range_sum(left, right)

    def mark_admitted(self, request_id: str) -> None:
        item = self.pending_items[request_id]
        if item.admitted:
            return
        item.admitted = True
        self.admitted_fenwick.add(item.index, item.weight)

    def policy_dict(self, window_name: str) -> dict[str, Any]:
        policy = self.policies[window_name]
        return {
            "method": policy.method,
            "sample_rate": policy.sample_rate,
            "sample_rate_num": policy.sample_rate_num,
            "sample_rate_den": policy.sample_rate_den,
            "exact_limit": policy.exact_limit,
            "max_window_events": policy.max_window_events,
            "confidence_level": 0.99,
            "estimator": "Horvitz-Thompson",
            "trigger_rule": "sample when actual events in the queried window > exact_limit",
        }

    def bucket_start(self, timestamp_ms: int, window_name: str) -> int:
        spec = self.short_spec if window_name == "short" else self.long_spec
        return (timestamp_ms // spec.step_ms) * spec.step_ms


class WindowStateSet:
    def __init__(self, series: dict[tuple[str, str], WindowSeries]) -> None:
        self.series = series

    def get(self, route_key: str, client_id: str) -> WindowSeries:
        return self.series[(route_key, client_id)]


def build_window_state(
    *,
    existing_rows_by_key: dict[tuple[str, str], list[dict[str, Any]]],
    new_events: Iterable[EventInput],
    settings: Settings,
    secret: str,
) -> WindowStateSet:
    new_by_key: dict[tuple[str, str], list[EventInput]] = {}
    for event in new_events:
        new_by_key.setdefault((event.route_key, event.client_id), []).append(event)
    keys = set(existing_rows_by_key) | set(new_by_key)
    series: dict[tuple[str, str], WindowSeries] = {}
    for key in sorted(keys):
        route_key, client_id = key
        series[key] = WindowSeries(
            route_key=route_key,
            client_id=client_id,
            existing_rows=existing_rows_by_key.get(key, ()),
            new_events=new_by_key.get(key, ()),
            settings=settings,
            secret=secret,
        )
    return WindowStateSet(series)
