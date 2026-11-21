from __future__ import annotations

import pytest

from app.errors import BusinessError


def _window(service, batch_id="B-TEST"):
    return service.get_batch(batch_id)["windows"][0]


def test_duplicate_is_idempotent_and_conflict_preserves_first_report(
    service, policy_v1, batch_factory, observation_factory
):
    service.submit_policy(policy_v1)
    service.submit_batch(batch_factory())
    observations = observation_factory()
    first = service.ingest_observations("B-TEST", {"observations": observations})
    assert first["accepted_count"] == 12
    before = _window(service)
    epoch_before = service.get_batch("B-TEST")["window_epoch"]

    duplicate = service.ingest_observations(
        "B-TEST", {"observations": [observations[0]]}
    )
    assert duplicate["accepted_count"] == 0
    assert duplicate["duplicate_count"] == 1
    after = _window(service)
    assert after["version"] == before["version"]
    assert after["sample_count"] == before["sample_count"]
    assert service.get_batch("B-TEST")["window_epoch"] == epoch_before

    conflicting = dict(observations[1])
    conflicting["latency_ms"] += 5
    with pytest.raises(BusinessError) as error:
        service.ingest_observations("B-TEST", {"observations": [conflicting]})
    assert error.value.code == "E_CONFLICTING_REPORT"
    assert error.value.context["sequence"] == 2
    after_conflict = _window(service)
    assert after_conflict["sample_count"] == 12
    assert after_conflict["version"] == before["version"]


def test_late_sample_within_tolerance_before_close_is_accepted_and_after_close_rejected(
    service, policy_v1, batch_factory, observation_factory
):
    service.submit_policy(policy_v1)
    started = 1_000_000
    service.submit_batch(batch_factory(started_at_ms=started))
    service.ingest_observations(
        "B-TEST",
        {"observations": observation_factory(count=12, started_at_ms=started)},
    )
    close_boundary = started + 60_000 + 5_000

    near_close = service.ingest_observations(
        "B-TEST",
        {
            "watermark_ms": close_boundary - 1_000,
            "observations": [
                {
                    "sequence": 20,
                    "timestamp_ms": started + 12_000,
                    "latency_ms": 181,
                    "success": True,
                }
            ],
        },
    )
    assert near_close["accepted_count"] == 1
    assert _window(service)["state"] == "OPEN"

    closed = service.ingest_observations(
        "B-TEST", {"observations": [], "watermark_ms": close_boundary}
    )
    assert closed["windows"][0]["state"] == "CLOSED"
    closed_window = _window(service)

    with pytest.raises(BusinessError) as error:
        service.ingest_observations(
            "B-TEST",
            {
                "watermark_ms": close_boundary,
                "observations": [
                    {
                        "sequence": 21,
                        "timestamp_ms": started + 13_000,
                        "latency_ms": 181,
                        "success": True,
                    }
                ],
            },
        )
    assert error.value.code == "E_LATE_SAMPLE"
    assert error.value.context["window_state"] == "CLOSED"
    after = _window(service)
    assert after["sample_count"] == closed_window["sample_count"] == 13
    assert after["version"] == closed_window["version"]


def test_insufficient_samples_returns_business_error_not_zero_risk(
    service, policy_v1, batch_factory, observation_factory
):
    service.submit_policy(policy_v1)
    service.submit_batch(batch_factory())
    service.ingest_observations(
        "B-TEST", {"observations": observation_factory(count=3)}
    )
    service.ingest_observations("B-TEST", {"watermark_ms": 1_070_000})
    with pytest.raises(BusinessError) as error:
        service.get_risk("B-TEST", mode="FULL")
    assert error.value.code == "E_INSUFFICIENT_SAMPLES"
    assert error.value.context["sample_count"] == 3
    assert error.value.context["min_samples"] == 10


def test_invalid_observation_decimal_uses_observation_error_code(
    service, policy_v1, batch_factory
):
    service.submit_policy(policy_v1)
    service.submit_batch(batch_factory())
    with pytest.raises(BusinessError) as error:
        service.ingest_observations(
            "B-TEST",
            {
                "observations": [
                    {
                        "sequence": 1,
                        "timestamp_ms": 1_000_000,
                        "latency_ms": "not-a-number",
                        "success": True,
                    }
                ]
            },
        )
    assert error.value.code == "E_OBSERVATION_INVALID"
    assert error.value.context["field"] == "observations[0].latency_ms"
