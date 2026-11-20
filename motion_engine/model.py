"""Closed-form, deterministic state evolution and range clamping."""

from __future__ import annotations

import math
from typing import Any

from .manifest import STATE_KEYS, expected_range_for_step


def initial_state_record(series: dict[str, Any]) -> dict[str, float | int]:
    state: dict[str, float | int] = {"step": 0}
    state.update({key: float(series["initial_state"][key]) for key in STATE_KEYS})
    return state


def transition_state(
    series: dict[str, Any],
    previous: dict[str, Any],
    step: int,
    *,
    ranges: dict[str, list[float]] | None = None,
) -> tuple[dict[str, float | int], list[dict[str, Any]]]:
    """Advance exactly one indivisible rolling step.

    Event velocity multipliers are applied before motion, then motion is applied,
    then the velocity increment is damped by ``acceleration_decay``. Deformation is
    a deterministic slow oscillation around the declared initial deformation.
    """

    x = float(previous["x"])
    y = float(previous["y"])
    vx = float(previous["vx"])
    vy = float(previous["vy"])

    event = series["dynamics"].get("direction_event")
    if event is not None and step == event["step"]:
        vx *= float(event["vx_factor"])
        vy *= float(event["vy_factor"])

    x += vx
    y += vy
    decay = float(series["dynamics"]["acceleration_decay"])
    vx *= decay
    vy *= decay

    deformation_config = series["deformation"]
    deformation = float(series["initial_state"]["deformation"]) + float(
        deformation_config["amplitude"]
    ) * math.sin(
        float(deformation_config["frequency"]) * step
        + float(deformation_config["phase"])
    )

    requested = {
        "x": x,
        "y": y,
        "vx": vx,
        "vy": vy,
        "deformation": deformation,
    }
    bounds = ranges if ranges is not None else expected_range_for_step(series, step)
    clamped: dict[str, float] = {}
    clamp_events: list[dict[str, Any]] = []
    for key in STATE_KEYS:
        value = requested[key]
        lower = float(bounds[key][0])
        upper = float(bounds[key][1])
        clamped_value = value
        if value < lower:
            clamped_value = lower
        elif value > upper:
            clamped_value = upper
        if clamped_value != value:
            clamp_events.append(
                {
                    "step": step,
                    "component": key,
                    "requested": value,
                    "clamped": clamped_value,
                    "lower": lower,
                    "upper": upper,
                }
            )
        clamped[key] = clamped_value

    state: dict[str, float | int] = {"step": step}
    state.update(clamped)
    return state, clamp_events
