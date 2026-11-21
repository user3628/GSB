import pytest

from evalbench.config import load_config
from evalbench.errors import ConfigInvalidError


def test_v1_upgrade_alias_pct_backfill(config_v1):
    cfg = load_config(config_v1)
    assert cfg.method == "platt"
    assert cfg.n_bins == 10
    assert cfg.expected_positive_rate == (0.0, 1.0)
    notes = {n.field: n for n in cfg.upgrade_notes}
    assert "platt_scaling" in notes["method"].source
    assert "bins_pct=10" in notes["n_bins"].source
    assert notes["expected_positive_rate"].source == "<missing>"


def test_upgraded_v1_reexports_as_v2(config_v1):
    cfg = load_config(config_v1)
    out = cfg.to_v2_dict()
    assert out["config_version"] == 2
    assert out["calibration"]["method"] == "platt"
    assert out["expected_positive_rate"] == [0.0, 1.0]


def test_out_of_range_bins_rejected():
    with pytest.raises(ConfigInvalidError) as exc:
        load_config({"config_version": 2, "n_bins": 0,
                     "calibration": {"method": "platt"},
                     "expected_positive_rate": [0.0, 1.0]})
    assert exc.value.code == "E_CONFIG_INVALID"


def test_unknown_alias_rejected():
    with pytest.raises(ConfigInvalidError):
        load_config("data/inject/config_unknown_alias.json")


def test_bad_interval_rejected():
    with pytest.raises(ConfigInvalidError):
        load_config({"config_version": 2, "n_bins": 10,
                     "calibration": {"method": "platt"},
                     "expected_positive_rate": [0.9, 0.1]})


def test_stage_fingerprints_are_field_scoped(config_v2):
    cfg = load_config(config_v2)
    base = cfg.stage_fingerprint("diagnose")
    changed = load_config({"config_version": 2, "n_bins": 10,
                           "calibration": {"method": "platt"},
                           "reliability_min_samples": 99,
                           "expected_positive_rate": [0.0, 1.0]})
    assert changed.stage_fingerprint("diagnose") != base
    assert changed.stage_fingerprint("calibrate") == cfg.stage_fingerprint("calibrate")
