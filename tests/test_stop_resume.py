from __future__ import annotations

from pathlib import Path

from conftest import output_file_digests
from storypipe.pipeline import PipelineRunner
from storypipe.workspace import import_sample


def test_stop_during_compose_then_resume_is_byte_identical(
    sample_root: Path, small_run, tmp_path: Path
) -> None:
    workspace = tmp_path / "compose-stop"
    import_sample(sample_root / "small", workspace)
    holder: dict[str, PipelineRunner] = {}

    def observer(stage: str, index: int, shot_id: str) -> None:
        if stage == "compose" and index == 1:
            holder["runner"].request_stop("test compose stop")

    runner = PipelineRunner(workspace, unit_observer=observer)
    holder["runner"] = runner
    stopped = runner.run(fresh=True)
    assert stopped["status"] == "stopped"
    assert stopped["stopped_at_stage"] == "compose"
    assert stopped["completed_units"] >= 2
    assert runner.checkpoints.read("encode") is None
    runner.resume()
    runner.unit_observer = None
    resumed = runner.run()
    assert resumed["ok"]
    baseline = small_run["workspace"]
    assert output_file_digests(workspace) == output_file_digests(baseline)
    assert (
        runner.export_manifest()["deterministic_summary_sha256"]
        == PipelineRunner(baseline).export_manifest()["deterministic_summary_sha256"]
    )


def test_stop_during_encode_then_resume_is_byte_identical(
    sample_root: Path, small_run, tmp_path: Path
) -> None:
    workspace = tmp_path / "encode-stop"
    import_sample(sample_root / "small", workspace)
    holder: dict[str, PipelineRunner] = {}

    def observer(stage: str, index: int, shot_id: str) -> None:
        if stage == "encode" and index == 0:
            holder["runner"].request_stop("test encode stop")

    runner = PipelineRunner(workspace, unit_observer=observer)
    holder["runner"] = runner
    stopped = runner.run(fresh=True)
    assert stopped["status"] == "stopped"
    assert stopped["stopped_at_stage"] == "encode"
    assert runner.checkpoints.read("encode") is None
    assert not (workspace / "stages" / "04_encode" / "output").exists()
    runner.resume()
    runner.unit_observer = None
    resumed = runner.run()
    assert resumed["ok"]
    assert output_file_digests(workspace) == output_file_digests(small_run["workspace"])
