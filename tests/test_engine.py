"""Requirement 3, 4 & 5: parallel determinism, stop/resume, invalidation."""
from __future__ import annotations

import json

import depworkbench.workspace as workspace_mod
from depworkbench.checkpoint import CheckpointStore


def _export(service) -> dict:
    path = service.workspace.root / "export.json"
    service.export(path)
    return json.loads(path.read_text(encoding="utf-8"))


def test_parallel_and_sequential_outputs_identical(imported_service,
                                                   tmp_workspace):
    parallel = imported_service.run()
    assert parallel.status == "completed"
    parallel_export = _export(imported_service)

    ws_seq = workspace_mod.Workspace.create(
        tmp_workspace.parent / "ws_seq")
    seq_service = workspace_mod.WorkbenchService(ws_seq)
    seq_service.import_bundle("samples", "samples/manifest.json",
                              "samples/config.yaml")
    seq_service.run(sequential=True)
    assert _export(seq_service) == parallel_export
    assert seq_service.stats()["stats"] == imported_service.stats()["stats"]


def test_graceful_stop_completes_current_shard_then_resumes(imported_service):
    def stop_after_first(index: int) -> None:
        if index == 0:
            imported_service.stop()

    report = imported_service.run(
        sequential=True, shard_complete_callback=stop_after_first)
    assert report.status == "stopped"
    assert report.stop_requested is True
    prefix = CheckpointStore(
        imported_service.workspace.checkpoint_root).load_consistent_prefix()[0]
    assert [r.shard_index for r in prefix] == [0]

    resumed = imported_service.resume()
    assert resumed.status == "completed"
    assert resumed.reused_checkpoints == [0]
    stats = imported_service.stats()["stats"]
    assert stats["shard_count"] == 3


def test_results_are_globally_sorted(imported_service):
    imported_service.run()
    aggregate = imported_service.stats()
    indices = [s["shard_index"] for s in aggregate["shards"]]
    assert indices == sorted(indices)


def test_input_change_invalidates_from_earliest_affected_shard(
        imported_service):
    imported_service.run()
    shards_dir = imported_service.workspace.shards_dir
    target = shards_dir / "shard_01.txt"
    original = target.read_text(encoding="utf-8").splitlines()
    original[0] = original[0] + " 增量"
    target.write_text("\n".join(original) + "\n", encoding="utf-8")
    from depworkbench.shards import load_shards, write_manifest
    changed = load_shards(shards_dir, shards_dir / "manifest.json")
    write_manifest(shards_dir / "manifest.json", changed)

    pre_status = imported_service.status()
    pre_shard1 = next(s for s in pre_status["shards"] if s["index"] == 1)
    assert pre_shard1["invalidation"] == "INPUT_CHANGED"

    report = imported_service.resume()
    assert report.invalidation_reason == "INPUT_CHANGED"
    assert report.invalidated_shards == [1, 2]
    assert report.reused_checkpoints == [0]
    status = imported_service.status()
    shard1 = next(s for s in status["shards"] if s["index"] == 1)
    assert shard1["invalidation"] is None
    assert shard1["state"] == "checkpointed"


def test_model_change_invalidates_everything(imported_service, ensure_samples):
    imported_service.run()
    config_path = imported_service.workspace.config_path
    raw = json.loads(json.dumps(
        __import__("yaml").safe_load(config_path.read_text(encoding="utf-8"))))
    raw["model"]["weights"]["shift"][0] += 0.125
    import yaml
    config_path.write_text(yaml.safe_dump(raw, allow_unicode=True),
                           encoding="utf-8")
    report = imported_service.resume()
    assert report.invalidation_reason == "MODEL_CHANGED"
    assert report.invalidated_shards == [0, 1, 2]


def test_budget_change_invalidates_decision(imported_service):
    imported_service.run()
    import yaml
    config_path = imported_service.workspace.config_path
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["budget"]["global_total_tokens"] = 130
    raw["budget"]["worker_token_quota"] = 80
    config_path.write_text(yaml.safe_dump(raw, allow_unicode=True),
                           encoding="utf-8")
    report = imported_service.resume()
    assert report.invalidation_reason == "BUDGET_OR_CONFIG_CHANGED"
    plan = imported_service.plan()
    assert plan["global_consumed"] <= 80


def test_structured_error_preserves_completed_shards(ensure_samples,
                                                     tmp_workspace):
    import depworkbench.workspace as workspace_mod
    ws = workspace_mod.Workspace.create(tmp_workspace)
    service = workspace_mod.WorkbenchService(ws)
    service.import_bundle(ensure_samples, ensure_samples / "manifest.json",
                          ensure_samples / "config.yaml")

    def explode_after_first(index: int) -> None:
        if index == 0:
            raise RuntimeError("simulated worker explosion")

    report = service.run(sequential=True,
                         shard_complete_callback=explode_after_first)
    assert report.status == "failed"
    assert report.structured_errors
    prefix = CheckpointStore(ws.checkpoint_root).load_consistent_prefix()[0]
    assert [r.shard_index for r in prefix] == [0]
