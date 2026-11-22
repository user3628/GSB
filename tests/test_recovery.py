import json
import os

from conftest import fresh_pipeline

from xfpipe.fingerprint import canonical_json_bytes


def _artifact_payloads(pipeline):
    return {
        stage: canonical_json_bytes(pipeline.load_artifact(stage)["artifact"])
        for stage in ("infer", "cross", "reduce", "summary")
    }


def test_stop_and_recovery_matches_full_run(bench):
    workdir = bench.workdir
    # Fresh reference run in a separate workdir with identical inputs.
    ref_dir = os.path.join(workdir, "reference")
    os.makedirs(os.path.join(ref_dir, "data"), exist_ok=True)
    import shutil

    shutil.copyfile(os.path.join(workdir, "data", "sample.csv"),
                    os.path.join(ref_dir, "data", "sample.csv"))
    shutil.copyfile(os.path.join(workdir, "data", "config.json"),
                    os.path.join(ref_dir, "data", "config.json"))
    reference = fresh_pipeline(ref_dir)
    reference.run()

    # Stop between stages, then recover.
    stopped = fresh_pipeline(workdir).run(stop_after_stage="cross")
    assert stopped["status"] == "stopped"
    assert stopped["completed_stages"] == ["infer", "cross"]
    seq_after_stop = fresh_pipeline(workdir).checkpoints.next_seq()
    assert seq_after_stop == 3  # two records published, sequence continuous

    recovered = fresh_pipeline(workdir).recover()
    assert recovered["status"] == "completed"
    assert recovered["recomputed_stages"] == ["reduce", "summary"]

    final = fresh_pipeline(workdir)
    assert _artifact_payloads(final) == _artifact_payloads(reference)
    # Sequence advanced continuously: 4 records, no gaps.
    records = final.checkpoints.records()
    assert [r["seq"] for r in records] == [1, 2, 3, 4]
    assert final.checkpoints.classify_snapshot()["status"] == "normal"


def test_graceful_stop_via_flag(bench):
    bench.pipeline.request_stop()
    report = bench.pipeline.run()
    assert report["status"] == "stopped"
    assert report["completed_stages"] == []
    bench.pipeline.clear_stop()
    report = bench.pipeline.run()
    assert report["status"] == "completed"


def test_journal_survives_for_recovery(ran_pipeline):
    pipeline = ran_pipeline.pipeline
    records = pipeline.checkpoints.records()
    assert len(records) == 4
    for rec in records:
        assert rec["sample_fingerprint"] == pipeline.sample_fingerprint
        assert rec["config_version"] == pipeline.config_version
        assert rec["artifact_digest"]
