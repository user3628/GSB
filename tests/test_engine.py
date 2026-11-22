from __future__ import annotations

import json
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

from posalign.engine import Engine
from posalign.checkpoints import CheckpointStore
from posalign.errors import BusinessError, E_ALIGNMENT_FAILED, E_STALE_MAPPING
from posalign.http_server import start_server_thread
from tests.conftest import copy_sample_inputs, write_jsonl


OUTPUT_NAMES = [
    "migration.json",
    "alignment.json",
    "mapping_candidates.json",
    "mapping_resolved.json",
    "summary.json",
    "stats.json",
    "manifest.json",
    "fixed_digest.txt",
]


def test_full_run_business_outputs(tmp_path, sample_config, sample_shards):
    engine = Engine(tmp_path / "work")
    result = engine.run(sample_shards, config_path=sample_config, reset=True)
    assert result["status"] == "completed"
    assert result["sequence"] == 12
    assert result["mapping_version"] == 1
    output = engine.output_dir
    for name in (
        "migration.json",
        "alignment.json",
        "mapping_candidates.json",
        "mapping_resolved.json",
        "summary.json",
        "stats.json",
        "manifest.json",
        "fixed_digest.txt",
    ):
        assert (output / name).is_file()
    digest = (output / "fixed_digest.txt").read_text(encoding="utf-8").strip()
    assert len(digest) == 64
    assert result["outputs"]["fixed_digest"] == digest
    expected = Path("samples/expected_fixed_digest.txt").read_text(encoding="utf-8").strip()
    assert digest == expected

    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    assert summary["v1"]["unsegmented_count"] == 1
    assert summary["v1"]["unsegmented_sentences"][0]["sent_id"] == "v1-002"
    assert summary["mapping"]["conflicts"] >= 2
    assert summary["alignment"]["insertions"] >= 1
    assert summary["alignment"]["deletions"] >= 1
    assert summary["alignment"]["unsegmented"] == 1

    mapping = {item["old_tag"]: item for item in engine.mapping_resolved()["resolved"]}
    assert mapping["名"]["new_tag"] == "NOUN"
    assert mapping["动"]["new_tag"] == "VERB"
    assert mapping["标题"]["new_tag"] == "PROPN"
    assert mapping["名"]["arbitration"]["rejected_candidates"]
    migration_only = engine.mapping_candidates("整句")
    assert migration_only["candidate_stage"] == "migration"
    assert any(
        diagnostic["code"] in {"alias_missing", "no_candidate"}
        for diagnostic in migration_only["diagnostics"]
    )


def test_interrupted_resume_is_byte_identical_to_full_run(tmp_path, sample_config, sample_shards):
    full = Engine(tmp_path / "full")
    full.run(sample_shards, config_path=sample_config, reset=True)
    resumed = Engine(tmp_path / "resumed")
    stopped = resumed.run(
        sample_shards,
        config_path=sample_config,
        reset=True,
        stop_after_sentences=5,
    )
    assert stopped["status"] == "stopped"
    assert stopped["sequence"] < 12
    resumed.run(sample_shards, config_path=sample_config, clear_stop=True)
    for name in OUTPUT_NAMES:
        assert (full.output_dir / name).read_bytes() == (resumed.output_dir / name).read_bytes(), name
    assert (full.workspace / "mapping" / "current.json").read_bytes() == (
        resumed.workspace / "mapping" / "current.json"
    ).read_bytes()
    assert (full.workspace / "mapping" / "versions" / "00000001.json").read_bytes() == (
        resumed.workspace / "mapping" / "versions" / "00000001.json"
    ).read_bytes()


def test_forced_interruption_after_mapping_publication_recovers_without_new_version(
    tmp_path, sample_config, sample_shards, monkeypatch
):
    reference = Engine(tmp_path / "reference")
    reference.run(sample_shards, config_path=sample_config, reset=True)
    engine = Engine(tmp_path / "interrupted")
    original = CheckpointStore.publish
    interrupted = {"done": False}

    def interrupt_before_adjudicate_checkpoint(self, **kwargs):
        if kwargs.get("stage") == "adjudicate" and not interrupted["done"]:
            interrupted["done"] = True
            raise RuntimeError("simulated hard interruption")
        return original(self, **kwargs)

    monkeypatch.setattr(CheckpointStore, "publish", interrupt_before_adjudicate_checkpoint)
    with pytest.raises(RuntimeError):
        engine.run(sample_shards, config_path=sample_config, reset=True)
    assert (engine.workspace / "mapping" / "current.json").is_file()
    monkeypatch.setattr(CheckpointStore, "publish", original)
    result = engine.run(sample_shards, config_path=sample_config)
    assert result["mapping_version"] == 1
    for name in OUTPUT_NAMES:
        assert (reference.output_dir / name).read_bytes() == (engine.output_dir / name).read_bytes(), name


def test_stop_after_adjudication_boundary_resumes_without_duplicate_mapping_version(
    tmp_path, sample_config, sample_shards, monkeypatch
):
    reference = Engine(tmp_path / "reference")
    reference.run(sample_shards, config_path=sample_config, reset=True)
    engine = Engine(tmp_path / "stopped")
    from posalign import engine as engine_module

    original_adjudicate = engine_module.adjudicate

    def adjudicate_then_request_stop(align_payloads, config):
        result = original_adjudicate(align_payloads, config)
        engine.stop_path.parent.mkdir(parents=True, exist_ok=True)
        engine.stop_path.write_text("stop\n", encoding="utf-8")
        return result

    monkeypatch.setattr(engine_module, "adjudicate", adjudicate_then_request_stop)
    stopped = engine.run(sample_shards, config_path=sample_config, reset=True)
    assert stopped["status"] == "stopped"
    assert (engine.workspace / "mapping" / "current.json").is_file()
    monkeypatch.setattr(engine_module, "adjudicate", original_adjudicate)
    resumed = engine.run(sample_shards, config_path=sample_config, clear_stop=True)
    assert resumed["mapping_version"] == 1
    for name in OUTPUT_NAMES:
        assert (reference.output_dir / name).read_bytes() == (engine.output_dir / name).read_bytes(), name


def test_alignment_failure_writes_no_alignment_checkpoints(tmp_path, sample_config, sample_shards):
    engine = Engine(tmp_path / "work")
    bad = Path("samples/injected_alignment_failed.jsonl").resolve()
    with pytest.raises(BusinessError) as caught:
        engine.run([sample_shards[0], bad], config_path=sample_config, reset=True)
    assert caught.value.code == E_ALIGNMENT_FAILED
    assert caught.value.details["failures"][0]["sent_id"] == "bad-align-001"
    assert caught.value.details["wrote_alignment_stage"] is False
    products = engine.status()["checkpoints"]["products"]
    assert not [name for name in products if name.endswith("/align")]
    assert not [name for name in products if name.endswith("/adjudicate")]
    failure = json.loads(engine.failure_path.read_text(encoding="utf-8"))
    assert failure["error"]["code"] == E_ALIGNMENT_FAILED
    assert failure["new_alignment_checkpoints_written"] is False


def test_incremental_invalidation_reuses_only_upstream(tmp_path):
    config, shards = copy_sample_inputs(tmp_path)
    engine = Engine(tmp_path / "work")
    first = engine.run(shards, config_path=config, reset=True)
    assert first["mapping_version"] == 1

    rows = [json.loads(line) for line in shards[2].read_text(encoding="utf-8").splitlines()]
    rows[0]["new_tags"][0] = "VERB"
    write_jsonl(shards[2], rows)
    second = engine.run(shards, config_path=config)
    assert second["mapping_version"] == 2
    status = engine.status()
    reasons = status["invalidation"]["reasons"]
    assert any(
        item["reason"] == "shard_content_changed" and item["shard"] == "shard_03_v2.jsonl"
        for item in reasons
    )
    reused = {(item["shard"], item["stage"]) for item in status["invalidation"]["reused_products"]}
    assert ("shard_01_v2.jsonl", "parse") in reused
    assert ("shard_01_v2.jsonl", "align") in reused
    assert ("shard_02_v1.jsonl", "align") in reused
    assert ("shard_03_v2.jsonl", "parse") not in reused
    assert ("shard_03_v2.jsonl", "align") not in reused

    text = config.read_text(encoding="utf-8").replace(
        "confidence_threshold: 0.55", "confidence_threshold: 0.70"
    )
    config.write_text(text, encoding="utf-8")
    third = engine.run(shards, config_path=config)
    assert third["mapping_version"] == 3
    status = engine.status()
    reasons = status["invalidation"]["reasons"]
    assert any(
        item["reason"] == "adjudication_priority_or_threshold_changed" for item in reasons
    )
    reused = {(item["shard"], item["stage"]) for item in status["invalidation"]["reused_products"]}
    for shard in ("shard_01_v2.jsonl", "shard_02_v1.jsonl", "shard_03_v2.jsonl"):
        assert (shard, "parse") in reused
        assert (shard, "align") in reused

    text = config.read_text(encoding="utf-8").replace("noun: [NOUN]", "noun: [PROPN]")
    config.write_text(text, encoding="utf-8")
    fourth = engine.run(shards, config_path=config)
    assert fourth["mapping_version"] == 4
    status = engine.status()
    assert any(
        item["reason"] == "old_label_alias_table_changed" for item in status["invalidation"]["reasons"]
    )
    reused = {(item["shard"], item["stage"]) for item in status["invalidation"]["reused_products"]}
    assert not any(stage == "parse" for _shard, stage in reused)
    assert not any(stage == "align" for _shard, stage in reused)


def test_invalidation_marks_mapping_stale_and_blocks_old_queries(tmp_path):
    config, shards = copy_sample_inputs(tmp_path)
    engine = Engine(tmp_path / "work")
    engine.run(shards, config_path=config, reset=True)
    text = config.read_text(encoding="utf-8").replace("min_rate: 0.72", "min_rate: 0.70")
    config.write_text(text, encoding="utf-8")
    engine.stop_path.parent.mkdir(parents=True, exist_ok=True)
    engine.stop_path.write_text("stop\n", encoding="utf-8")
    result = engine.run(shards, config_path=config)
    assert result["status"] == "stopped"
    status = engine.status()
    assert status["mapping"]["stale_marker"] is not None
    assert status["outputs_stale"] is not None
    with pytest.raises(BusinessError) as caught:
        engine.mapping_resolved()
    assert caught.value.code == E_STALE_MAPPING
    assert len(engine.mapping_resolved(allow_stale=True)["resolved"]) > 0

    engine.run(shards, config_path=config, clear_stop=True)
    assert engine.status()["mapping"]["stale_marker"] is None


def test_local_http_api(tmp_path, sample_config, sample_shards):
    engine = Engine(tmp_path / "work")
    engine.run(sample_shards, config_path=sample_config, reset=True)
    server, thread = start_server_thread(engine.workspace)
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with urllib.request.urlopen(base + "/health") as response:
            assert json.loads(response.read())["ok"] is True
        with urllib.request.urlopen(base + "/status") as response:
            assert json.loads(response.read())["checkpoints"]["seq"] == 12
        label = urllib.parse.quote("名")
        with urllib.request.urlopen(base + f"/mapping/candidates?label={label}") as response:
            candidates = json.loads(response.read())["candidates"]
            assert candidates[0]["tag"] == "NOUN"
        with urllib.request.urlopen(base + "/summary") as response:
            assert json.loads(response.read())["v1"]["unsegmented_count"] == 1
        body = json.dumps(
            {
                "shards": [str(path) for path in sample_shards],
                "config": str(sample_config),
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            base + "/run",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request) as response:
            assert json.loads(response.read())["status"] == "up_to_date"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

