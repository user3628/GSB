from __future__ import annotations

import numpy as np
import pytest

from confighub.errors import ConfigHubError, ErrorCode
from confighub.models import SampleSpec
from confighub.pipeline import compute_pipeline, generate_samples
from confighub.service import snapshot_digest

from .conftest import CONFIG_V1, submit


def test_snapshot_integrity_and_staleness_do_not_change_state(service, config_v1):
    generated_at = "2026-01-01T00:00:00+00:00"
    with pytest.raises(ConfigHubError) as invalid:
        service.submit_snapshot(
            1,
            config_v1,
            digest="0" * 64,
            generated_at=generated_at,
        )
    assert invalid.value.code == ErrorCode.E_SNAPSHOT_INVALID
    assert service.latest_version() == 0
    assert service.storage.query_one("SELECT count(*) AS n FROM snapshots")["n"] == 0

    good = submit(service, 1, config_v1)
    assert good["version"] == 1
    with pytest.raises(ConfigHubError) as stale:
        service.submit_snapshot(
            1,
            config_v1,
            digest=snapshot_digest(1, generated_at, config_v1),
            generated_at=generated_at,
        )
    assert stale.value.code == ErrorCode.E_STALE_SNAPSHOT
    assert service.latest_version() == 1
    assert service.storage.query_one("SELECT count(*) AS n FROM snapshots")["n"] == 1

    bad_digest_v2 = snapshot_digest(2, "2026-01-01T00:00:02+00:00", CONFIG_V1)
    with pytest.raises(ConfigHubError) as invalid_v2:
        service.submit_snapshot(
            2,
            config_v1,
            digest="f" * 64,
            generated_at="2026-01-01T00:00:02+00:00",
        )
    assert invalid_v2.value.code == ErrorCode.E_SNAPSHOT_INVALID
    assert service.latest_version() == 1
    assert service.get_snapshot(2) is None
    assert bad_digest_v2 != "f" * 64


def test_pipeline_is_bitwise_deterministic_and_ignores_global_rng():
    spec = SampleSpec(seed=19, sample_size=32, feature_count=3)
    config = dict(CONFIG_V1)
    np.random.seed(1)
    samples_a = generate_samples(spec)
    result_a = compute_pipeline(spec, 4, config)
    np.random.seed(999)
    samples_b = generate_samples(spec)
    result_b = compute_pipeline(spec, 4, config)
    assert np.array_equal(samples_a, samples_b)
    assert result_a == result_b
    assert result_a["result_digest"] == result_b["result_digest"]

    same_config_new_version = compute_pipeline(spec, 5, config)
    assert same_config_new_version["weighted_sum_digest"] == result_a["weighted_sum_digest"]
    assert same_config_new_version["result_digest"] != result_a["result_digest"]

