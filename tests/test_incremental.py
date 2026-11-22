import pytest

from conftest import fresh_pipeline, read_config, write_config

from xfpipe.errors import E_INCREMENTAL_MISMATCH, PipelineError
from xfpipe.fingerprint import canonical_json_bytes


def _mutate(workdir, mutate):
    cfg = read_config(workdir)
    mutate(cfg)
    write_config(workdir, cfg)


def test_reduction_change_rebuilds_reduce_and_summary_only(ran_pipeline):
    _mutate(ran_pipeline.workdir, lambda c: c["reduction"]["candidates"][0].update(target_dim=4))
    ran_pipeline._reload()
    report = ran_pipeline.pipeline.run()
    assert report["reused_stages"] == ["infer", "cross"]
    assert report["recomputed_stages"] == ["reduce", "summary"]
    assert report["incremental_verification"]["status"] == "match"


def test_crossing_change_rebuilds_cross_and_downstream(ran_pipeline):
    _mutate(ran_pipeline.workdir, lambda c: c["crossing"].update(max_order=3))
    ran_pipeline._reload()
    report = ran_pipeline.pipeline.run()
    assert report["reused_stages"] == ["infer"]
    assert report["recomputed_stages"] == ["cross", "reduce", "summary"]
    assert report["incremental_verification"]["status"] == "match"


def test_irrelevant_change_rebuilds_nothing(ran_pipeline):
    _mutate(ran_pipeline.workdir, lambda c: c["cache"].update(capacity=3))
    ran_pipeline._reload()
    report = ran_pipeline.pipeline.run()
    assert report["recomputed_stages"] == []
    assert report["reused_stages"] == ["infer", "cross", "reduce", "summary"]


def test_inference_change_rebuilds_everything(ran_pipeline):
    _mutate(ran_pipeline.workdir, lambda c: c["inference"].update(numeric_bins=4))
    ran_pipeline._reload()
    report = ran_pipeline.pipeline.run()
    assert report["reused_stages"] == []
    assert report["recomputed_stages"] == ["infer", "cross", "reduce", "summary"]


def test_incremental_outputs_match_full_rebuild_byte_for_byte(ran_pipeline):
    workdir = ran_pipeline.workdir
    _mutate(workdir, lambda c: c["crossing"].update(max_crosses=5))
    _mutate(read_config(workdir) and workdir, lambda c: None)  # no-op guard
    ran_pipeline._reload()
    ran_pipeline.pipeline.run()
    result = ran_pipeline.pipeline.verify_against_full_rebuild()
    assert result["status"] == "match"


def test_incremental_mismatch_is_detected(ran_pipeline):
    workdir = ran_pipeline.workdir
    # Corrupt the persisted cross artefact payload, keep meta intact.
    path = ran_pipeline.pipeline._artifact_path("cross")
    raw = open(path, "rb").read().decode("utf-8")
    corrupted = raw.replace('"cardinality":4', '"cardinality":5', 1)
    assert corrupted != raw
    open(path, "wb").write(corrupted.encode("utf-8"))
    with pytest.raises(PipelineError) as exc:
        ran_pipeline.pipeline.verify_against_full_rebuild()
    assert exc.value.code == E_INCREMENTAL_MISMATCH
    assert exc.value.details["stage"] == "cross"
    assert "first_diff_offset" in exc.value.details
