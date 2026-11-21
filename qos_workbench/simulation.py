from __future__ import annotations

import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping

from .errors import E_SAMPLE_INVALID, BusinessError
from .models import PolicySnapshot, TrafficSample, decimal_ms
from .normalization import TOKEN_SCALE


COLOR_ALIASES = {
    "green": "green",
    "g": "green",
    "0": "green",
    "yellow": "yellow",
    "y": "yellow",
    "1": "yellow",
    "red": "red",
    "r": "red",
    "2": "red",
}


def canonical_color(value: Any, *, index: int) -> str:
    color = str(value).strip().lower()
    canonical = COLOR_ALIASES.get(color)
    if canonical is None:
        raise BusinessError(
            E_SAMPLE_INVALID,
            "sample color must be green, yellow or red",
            {"index": index, "color": str(value)},
        )
    return canonical


def normalize_samples(samples: Iterable[Mapping[str, Any] | TrafficSample]) -> tuple[TrafficSample, ...]:
    result: list[TrafficSample] = []
    last_timestamp_by_queue: dict[str, int] = {}
    for index, raw in enumerate(samples):
        if isinstance(raw, TrafficSample):
            sample = raw
        else:
            timestamp_value = raw.get("timestamp_us", raw.get("timestamp_ms"))
            if timestamp_value is None:
                raise BusinessError(
                    E_SAMPLE_INVALID,
                    "sample is missing timestamp_ms or timestamp_us",
                    {"index": index},
                )
            if "timestamp_us" in raw:
                timestamp_us = int(timestamp_value)
            else:
                timestamp_us = int(Decimal(str(timestamp_value)) * 1000)
            queue_id = raw.get("queue_id")
            packet_value = raw.get("packet_bytes", raw.get("length_bytes", raw.get("packet_length")))
            if packet_value is None:
                raise BusinessError(
                    E_SAMPLE_INVALID,
                    "sample is missing packet_bytes",
                    {"index": index},
                )
            try:
                packet_bytes = int(packet_value)
            except (TypeError, ValueError) as exc:
                raise BusinessError(
                    E_SAMPLE_INVALID,
                    "packet_bytes must be an integer",
                    {"index": index, "value": str(packet_value)},
                ) from exc
            sample = TrafficSample(
                timestamp_us=timestamp_us,
                queue_id=str(queue_id) if queue_id is not None else "",
                packet_bytes=packet_bytes,
                color=canonical_color(raw.get("color", raw.get("mark_color")), index=index),
                source_index=index,
            )
        if not sample.queue_id:
            raise BusinessError(
                E_SAMPLE_INVALID,
                "sample queue_id must not be empty",
                {"index": index},
            )
        if sample.timestamp_us < 0:
            raise BusinessError(
                E_SAMPLE_INVALID,
                "sample timestamp must not be negative",
                {"index": index, "timestamp_us": sample.timestamp_us},
            )
        if sample.packet_bytes <= 0:
            raise BusinessError(
                E_SAMPLE_INVALID,
                "sample packet_bytes must be positive",
                {"index": index, "packet_bytes": sample.packet_bytes},
            )
        previous = last_timestamp_by_queue.get(sample.queue_id)
        if previous is not None and sample.timestamp_us < previous:
            raise BusinessError(
                E_SAMPLE_INVALID,
                "sample timestamps must be non-decreasing within each queue",
                {
                    "index": index,
                    "queue_id": sample.queue_id,
                    "previous_timestamp_us": previous,
                    "timestamp_us": sample.timestamp_us,
                },
            )
        last_timestamp_by_queue[sample.queue_id] = sample.timestamp_us
        result.append(
            TrafficSample(
                timestamp_us=sample.timestamp_us,
                queue_id=sample.queue_id,
                packet_bytes=sample.packet_bytes,
                color=canonical_color(sample.color, index=index),
                source_index=index,
            )
        )
    if not result:
        raise BusinessError(E_SAMPLE_INVALID, "traffic sample must contain at least one packet")
    return tuple(result)


def fixed_units_to_bits(units: int) -> str:
    value = Decimal(units) / Decimal(TOKEN_SCALE)
    return f"{value.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP):.6f}"


def fixed_ratio(numerator: int | Decimal, denominator: int | Decimal, places: int = 6) -> str:
    if denominator == 0:
        return "0." + "0" * places
    quantum = Decimal(1).scaleb(-places)
    value = (Decimal(numerator) / Decimal(denominator)).quantize(
        quantum, rounding=ROUND_HALF_UP
    )
    return f"{value:.{places}f}"


def _drop_reason(arrival_color: str, committed_available: bool, peak_available: bool) -> str:
    if arrival_color == "red":
        return "arrival_red"
    if not peak_available:
        return "peak_bucket_empty"
    if arrival_color == "green" and not committed_available:
        return "committed_bucket_empty_and_peak_exhausted"
    return "peak_bucket_empty"


def simulate_snapshot(
    snapshot: PolicySnapshot,
    samples: Iterable[Mapping[str, Any] | TrafficSample],
) -> dict[str, Any]:
    """Run a deterministic dual token-bucket simulation.

    Token counters are integer micro-bits (1,000,000 sub-units per bit).
    Refill in those units is exactly ``rate_bps * elapsed_us`` because
    ``bps * us / 1e6 seconds * 1e6 sub-units/bit`` simplifies to that product.
    """
    normalized_samples = normalize_samples(samples)
    by_queue = snapshot.by_queue
    unknown = sorted({sample.queue_id for sample in normalized_samples if sample.queue_id not in by_queue})
    if unknown:
        raise BusinessError(
            E_SAMPLE_INVALID,
            "traffic sample references queues absent from the active snapshot",
            {"unknown_queue_ids": unknown},
        )

    states = {
        policy.queue_id: {
            "policy": policy,
            "committed_units": policy.cbs_bytes * 8 * TOKEN_SCALE,
            "peak_units": policy.pbs_bytes * 8 * TOKEN_SCALE,
            "last_timestamp_us": None,
            "stats": {
                "received_packets": 0,
                "received_bytes": 0,
                "sent_packets": 0,
                "sent_bytes": 0,
                "dropped_packets": 0,
                "dropped_bytes": 0,
                "marked_bytes": 0,
                "green_bytes": 0,
                "yellow_bytes": 0,
                "red_bytes": 0,
                "arrival_green_bytes": 0,
                "arrival_yellow_bytes": 0,
                "arrival_red_bytes": 0,
            },
        }
        for policy in snapshot.policies
    }

    packets: list[dict[str, Any]] = []
    for index, sample in enumerate(normalized_samples):
        state = states[sample.queue_id]
        policy = state["policy"]
        committed_capacity = policy.cbs_bytes * 8 * TOKEN_SCALE
        peak_capacity = policy.pbs_bytes * 8 * TOKEN_SCALE
        last_timestamp = state["last_timestamp_us"]
        if last_timestamp is not None:
            elapsed_us = sample.timestamp_us - last_timestamp
            state["committed_units"] = min(
                committed_capacity,
                state["committed_units"] + policy.cir_bps * elapsed_us,
            )
            state["peak_units"] = min(
                peak_capacity,
                state["peak_units"] + policy.pir_bps * elapsed_us,
            )
        state["last_timestamp_us"] = sample.timestamp_us

        committed_before = state["committed_units"]
        peak_before = state["peak_units"]
        packet_cost = sample.packet_bytes * 8 * TOKEN_SCALE
        committed_available = committed_before >= packet_cost
        peak_available = peak_before >= packet_cost

        arrival_color = sample.color
        if arrival_color == "red":
            egress_color = "red"
            action = "drop"
            reason = _drop_reason(arrival_color, committed_available, peak_available)
        elif arrival_color == "green" and committed_available and peak_available:
            state["committed_units"] -= packet_cost
            state["peak_units"] -= packet_cost
            egress_color = "green"
            action = "send"
            reason = "committed_and_peak_tokens_available"
        elif peak_available:
            state["peak_units"] -= packet_cost
            egress_color = "yellow"
            action = "send"
            reason = (
                "committed_tokens_insufficient_peak_tokens_available"
                if arrival_color == "green"
                else "arrival_yellow_peak_tokens_available"
            )
        else:
            egress_color = "red"
            action = "drop"
            reason = _drop_reason(arrival_color, committed_available, peak_available)

        sent_bytes = sample.packet_bytes if action == "send" else 0
        dropped_bytes = sample.packet_bytes - sent_bytes
        marked_bytes = sample.packet_bytes if egress_color in {"yellow", "red"} else 0
        stats = state["stats"]
        stats["received_packets"] += 1
        stats["received_bytes"] += sample.packet_bytes
        stats[f"arrival_{arrival_color}_bytes"] += sample.packet_bytes
        stats[f"{egress_color}_bytes"] += sample.packet_bytes
        stats["marked_bytes"] += marked_bytes
        if action == "send":
            stats["sent_packets"] += 1
            stats["sent_bytes"] += sent_bytes
        else:
            stats["dropped_packets"] += 1
            stats["dropped_bytes"] += dropped_bytes

        packets.append(
            {
                "index": index,
                "timestamp_us": sample.timestamp_us,
                "timestamp_ms": decimal_ms(sample.timestamp_us),
                "queue_id": sample.queue_id,
                "packet_bytes": sample.packet_bytes,
                "arrival_color": arrival_color,
                "egress_color": egress_color,
                "action": action,
                "reason": reason,
                "sent_bytes": sent_bytes,
                "dropped_bytes": dropped_bytes,
                "marked_bytes": marked_bytes,
                "committed_tokens_before_units": committed_before,
                "committed_tokens_after_units": state["committed_units"],
                "peak_tokens_before_units": peak_before,
                "peak_tokens_after_units": state["peak_units"],
                "committed_tokens_before_bits": fixed_units_to_bits(committed_before),
                "committed_tokens_after_bits": fixed_units_to_bits(state["committed_units"]),
                "peak_tokens_before_bits": fixed_units_to_bits(peak_before),
                "peak_tokens_after_bits": fixed_units_to_bits(state["peak_units"]),
            }
        )

    min_timestamp = min(sample.timestamp_us for sample in normalized_samples)
    max_timestamp = max(sample.timestamp_us for sample in normalized_samples)
    duration_us = max_timestamp - min_timestamp
    denominator_us = max(duration_us, 1)
    per_queue: dict[str, Any] = {}
    for queue_id in sorted(states):
        state = states[queue_id]
        policy = state["policy"]
        stats = dict(state["stats"])
        exact_rate = Decimal(stats["sent_bytes"] * 8 * 1_000_000) / Decimal(denominator_us)
        displayed_rate = exact_rate.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
        stats.update(
            {
                "queue_id": queue_id,
                "priority": policy.priority,
                "cir_bps": policy.cir_bps,
                "pir_bps": policy.pir_bps,
                "cbs_bytes": policy.cbs_bytes,
                "pbs_bytes": policy.pbs_bytes,
                "parent_queue_id": policy.parent_queue_id,
                "actual_sent_rate_bps": f"{displayed_rate:.6f}",
                "measurement_window_us": denominator_us,
                "final_committed_tokens_bits": fixed_units_to_bits(state["committed_units"]),
                "final_peak_tokens_bits": fixed_units_to_bits(state["peak_units"]),
            }
        )
        per_queue[queue_id] = stats

    canonical_samples = [
        {
            "timestamp_us": sample.timestamp_us,
            "queue_id": sample.queue_id,
            "packet_bytes": sample.packet_bytes,
            "color": sample.color,
        }
        for sample in normalized_samples
    ]
    sample_sha256 = hashlib.sha256(
        json.dumps(canonical_samples, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "snapshot": {
            "snapshot_no": snapshot.snapshot_no,
            "format_version": snapshot.format_version,
            "generated_at_us": snapshot.generated_at_us,
            "checksum": snapshot.checksum,
        },
        "sample_sha256": sample_sha256,
        "packet_count": len(normalized_samples),
        "measurement_window_us": denominator_us,
        "initial_bucket_policy": "both_buckets_full_at_first_packet_per_queue",
        "token_model": {
            "fixed_point_scale": TOKEN_SCALE,
            "fixed_point_unit": "sub-bit (1 bit = 1,000,000 sub-units)",
            "refill_formula": "token_subunits += rate_bps * elapsed_us, capped at burst_bytes * 8 * 1,000,000",
            "rounding": "no token rounding; integer arithmetic only",
        },
        "packets": packets,
        "per_queue": per_queue,
    }

