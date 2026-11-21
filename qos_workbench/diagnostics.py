from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from .errors import E_REQUEST_INVALID, BusinessError
from .models import PolicySnapshot


def _fixed_decimal(value: Decimal, places: int = 6) -> str:
    quantum = Decimal(1).scaleb(-places)
    return f"{value.quantize(quantum, rounding=ROUND_HALF_UP):.{places}f}"


def build_diagnostics(
    snapshot: PolicySnapshot,
    simulation: dict[str, Any],
    *,
    relative_tolerance: str | Decimal = "0.10",
) -> dict[str, Any]:
    try:
        tolerance = Decimal(str(relative_tolerance))
    except Exception as exc:  # noqa: BLE001 - normalized into a business error
        raise BusinessError(
            E_REQUEST_INVALID,
            "relative_tolerance must be a decimal number",
            {"relative_tolerance": str(relative_tolerance)},
        ) from exc
    if not tolerance.is_finite() or tolerance < 0:
        raise BusinessError(
            E_REQUEST_INVALID,
            "relative_tolerance must be finite and non-negative",
            {"relative_tolerance": str(relative_tolerance)},
        )

    by_queue = snapshot.by_queue
    entries: list[dict[str, Any]] = []
    over: list[str] = []
    under: list[str] = []
    stable: list[str] = []
    no_traffic: list[str] = []
    cumulative_rate_rounding_error = Decimal(0)

    for queue_id in sorted(simulation["per_queue"]):
        stats = simulation["per_queue"][queue_id]
        policy = by_queue[queue_id]
        actual = Decimal(stats["sent_bytes"] * 8 * 1_000_000) / Decimal(
            stats["measurement_window_us"]
        )
        target = Decimal(policy.cir_bps)
        delta = actual - target
        has_traffic = stats.get("received_packets", 0) > 0
        if not has_traffic:
            ratio = None
            within = None
            ratio_display = None
        elif target == 0:
            ratio = Decimal(0) if actual == 0 else None
            within = actual == 0
        else:
            ratio = delta / target
            within = abs(ratio) <= tolerance

        if not has_traffic:
            classification = "no_traffic"
            no_traffic.append(queue_id)
        elif within:
            classification = "stable"
            stable.append(queue_id)
        elif delta > 0:
            classification = "over"
            over.append(queue_id)
        else:
            classification = "under"
            under.append(queue_id)

        actual_display = _fixed_decimal(actual, 6)
        rounded_actual = Decimal(actual_display)
        cumulative_rate_rounding_error += actual - rounded_actual
        if has_traffic:
            ratio_display = None if ratio is None else _fixed_decimal(ratio, 6)
        else:
            ratio_display = None
        entries.append(
            {
                "queue_id": queue_id,
                "priority": policy.priority,
                "target_cir_bps": policy.cir_bps,
                "target_pir_bps": policy.pir_bps,
                "actual_sent_rate_bps": actual_display,
                "deviation_bps": _fixed_decimal(delta, 6),
                "deviation_ratio": ratio_display,
                "relative_tolerance": _fixed_decimal(tolerance, 6),
                "classification": classification,
                "within_relative_tolerance": within,
                "sent_bytes": stats["sent_bytes"],
                "dropped_bytes": stats["dropped_bytes"],
                "marked_bytes": stats["marked_bytes"],
                "green_bytes": stats["green_bytes"],
                "yellow_bytes": stats["yellow_bytes"],
                "red_bytes": stats["red_bytes"],
            }
        )

    return {
        "snapshot": simulation["snapshot"],
        "sample_sha256": simulation["sample_sha256"],
        "measurement_window_us": simulation["measurement_window_us"],
        "comparison": {
            "reference_rate": "normalized CIR in bps",
            "relative_tolerance": _fixed_decimal(tolerance, 6),
            "comparison_method": "abs((actual_cir - target_cir) / target_cir) <= relative_tolerance using Decimal",
            "tie_breaking": "queue_id ascending (lexicographic); tolerance-boundary entries are stable",
        },
        "thresholds": {
            "relative_tolerance": _fixed_decimal(tolerance, 6),
            "over_condition": "deviation_ratio > relative_tolerance",
            "under_condition": "deviation_ratio < -relative_tolerance",
            "stable_condition": "abs(deviation_ratio) <= relative_tolerance",
        },
        "queues": entries,
        "over_shaped_queue_ids": sorted(over),
        "under_shaped_queue_ids": sorted(under),
        "stable_queue_ids": sorted(stable),
        "no_traffic_queue_ids": sorted(no_traffic),
        "precision_contract": {
            "token_arithmetic": "integer micro-bit fixed point; no floating-point token operations",
            "rate_calculation": "exact rational sent_bits / measurement_window, rounded only for presentation",
            "decimal_places": 6,
            "rounding_mode": "ROUND_HALF_UP",
            "cumulative_rate_rounding_error_bps": _fixed_decimal(
                cumulative_rate_rounding_error, 6
            ),
            "cumulative_error_semantics": "sum(exact_actual_rate - displayed_actual_rate) in bps",
        },
    }

