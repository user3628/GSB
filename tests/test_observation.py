from fiw.catalog import load_samples
from fiw.models import (
    CONCLUSION_NORMAL,
    CONCLUSION_UNREACHABLE,
    CONCLUSION_UNSTABLE,
)
from fiw.observation import compute_observations


def _trajectory():
    return [
        {"item_id": "it-1", "target": "ep-cache-1",
         "fault_type": "connection_refused", "outcome": "INJECTED"},
        {"item_id": "it-2", "target": "ep-db-1",
         "fault_type": "connection_timeout", "outcome": "TIMEOUT"},
        {"item_id": "it-3", "target": "ep-web-1",
         "fault_type": "corrupt_body", "outcome": "INJECTED"},
        {"item_id": "it-4", "target": "ep-web-2",
         "fault_type": "connection_refused", "outcome": "FAULT_UNSUPPORTED"},
    ]


def test_observation_conclusions_and_propagation(env):
    catalog, _ = load_samples(str(env.endpoints_path), str(env.plan_path))
    obs = compute_observations(catalog, _trajectory())

    assert obs["ep-cache-1"]["conclusion"] == CONCLUSION_UNREACHABLE
    assert obs["ep-db-1"]["conclusion"] == CONCLUSION_UNREACHABLE
    assert obs["ep-web-1"]["conclusion"] == CONCLUSION_UNSTABLE
    # unsupported fault leaves no impact
    assert obs["ep-web-2"]["conclusion"] == CONCLUSION_NORMAL

    api = obs["ep-api-1"]
    # weight 0.4 -> UNSTABLE baseline; upstream ep-db-1 UNREACHABLE -> downgrade
    assert api["conclusion"] == CONCLUSION_UNREACHABLE
    assert api["propagation"], "propagation evidence must be recorded"
    assert api["propagation"][0]["upstream"] == "ep-db-1"
    assert api["propagation"][0]["rule"] == "upstream_unreachable_downgrade"
    assert api["propagation"][0]["from"] == CONCLUSION_UNSTABLE
    assert api["propagation"][0]["to"] == CONCLUSION_UNREACHABLE


def test_no_injection_all_normal_except_low_weight(env):
    catalog, _ = load_samples(str(env.endpoints_path), str(env.plan_path))
    obs = compute_observations(catalog, [])
    assert obs["ep-web-1"]["conclusion"] == CONCLUSION_NORMAL
    assert obs["ep-web-2"]["conclusion"] == CONCLUSION_NORMAL
    assert obs["ep-api-1"]["conclusion"] == CONCLUSION_UNSTABLE  # weight 0.4
