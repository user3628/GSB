"""One-command deterministic demonstration of the complete local workflow."""

from __future__ import annotations

import json
import shutil
import sys
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from posalign.canonical import atomic_write_json, read_json  # noqa: E402
from posalign.engine import Engine  # noqa: E402
from posalign.errors import BusinessError, E_ALIGNMENT_FAILED, E_STALE_MAPPING  # noqa: E402
from posalign.http_server import start_server_thread  # noqa: E402
from posalign.mapping_store import MappingStore  # noqa: E402
from scripts.generate_samples import main as generate_samples  # noqa: E402


DEMO = ROOT / "runtime" / "demo"
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


def _fresh(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _copy_inputs(target: Path) -> tuple[Path, list[Path]]:
    _fresh(target)
    config = target / "governance.yaml"
    shutil.copyfile(ROOT / "samples" / "governance.yaml", config)
    shards: list[Path] = []
    for source in sorted((ROOT / "samples").glob("shard_*.jsonl")):
        shard = target / source.name
        shutil.copyfile(source, shard)
        shards.append(shard)
    return config, shards


def _write_changed_shard(path: Path) -> None:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    rows[0]["new_tags"][0] = "VERB"
    path.write_text(
        "".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )


def main() -> None:
    generate_samples()
    _fresh(DEMO)
    inputs = DEMO / "inputs"
    config, shards = _copy_inputs(inputs)

    full = Engine(DEMO / "full")
    full_result = full.run(shards, config_path=config, reset=True)

    resumed = Engine(DEMO / "resumed")
    stopped = resumed.run(
        shards,
        config_path=config,
        reset=True,
        stop_after_sentences=5,
    )
    resumed_result = resumed.run(shards, config_path=config, clear_stop=True)
    byte_identical = all(
        (full.output_dir / name).read_bytes() == (resumed.output_dir / name).read_bytes()
        for name in OUTPUT_NAMES
    ) and (
        (full.workspace / "mapping" / "current.json").read_bytes()
        == (resumed.workspace / "mapping" / "current.json").read_bytes()
    ) and (
        (full.workspace / "mapping" / "versions" / "00000001.json").read_bytes()
        == (resumed.workspace / "mapping" / "versions" / "00000001.json").read_bytes()
    )
    if not byte_identical:
        raise RuntimeError("resume output is not byte-identical to the full run")

    v1_target = DEMO / "v1_roundtrip.jsonl"
    export_result = full.export_v1(
        [inputs / "shard_02_v1.jsonl"],
        config_path=config,
        output_path=v1_target,
    )
    original_v1 = [
        json.loads(line)
        for line in (inputs / "shard_02_v1.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    exported_v1 = [
        json.loads(line) for line in v1_target.read_text(encoding="utf-8").splitlines()
    ]
    v1_exact = original_v1 == exported_v1
    if not v1_exact:
        raise RuntimeError("v1 reverse export did not round-trip exactly")

    failure_engine = Engine(DEMO / "alignment_failure")
    try:
        failure_engine.run(
            [inputs / "shard_01_v2.jsonl", ROOT / "samples" / "injected_alignment_failed.jsonl"],
            config_path=config,
            reset=True,
        )
        raise RuntimeError("injected alignment failure was not raised")
    except BusinessError as error:
        if error.code != E_ALIGNMENT_FAILED:
            raise
        failure_result = {
            "code": error.code,
            "sent_id": error.details["failures"][0]["sent_id"],
            "alignment_rate": error.details["failures"][0]["alignment_rate"],
            "min_rate": error.details["min_rate"],
            "new_alignment_checkpoints_written": error.details[
                "wrote_alignment_stage"
            ],
        }

    incremental_inputs = DEMO / "incremental_inputs"
    inc_config, inc_shards = _copy_inputs(incremental_inputs)
    incremental = Engine(DEMO / "incremental")
    incremental.run(inc_shards, config_path=inc_config, reset=True)
    _write_changed_shard(inc_shards[2])
    shard_run = incremental.run(inc_shards, config_path=inc_config)
    shard_status = incremental.status()
    shard_reused = sorted(
        f"{item['shard']}/{item['stage']}"
        for item in shard_status["invalidation"]["reused_products"]
    )
    shard_reasons = shard_status["invalidation"]["reasons"]
    text = inc_config.read_text(encoding="utf-8").replace(
        "confidence_threshold: 0.55", "confidence_threshold: 0.70"
    )
    inc_config.write_text(text, encoding="utf-8")
    threshold_run = incremental.run(inc_shards, config_path=inc_config)
    threshold_status = incremental.status()
    threshold_reused = sorted(
        f"{item['shard']}/{item['stage']}"
        for item in threshold_status["invalidation"]["reused_products"]
    )

    text = inc_config.read_text(encoding="utf-8").replace("min_rate: 0.72", "min_rate: 0.70")
    inc_config.write_text(text, encoding="utf-8")
    incremental.stop_path.parent.mkdir(parents=True, exist_ok=True)
    incremental.stop_path.write_text("stop\n", encoding="utf-8")
    stale_stop = incremental.run(inc_shards, config_path=inc_config)
    try:
        incremental.mapping_resolved()
        stale_block_code = None
    except BusinessError as error:
        stale_block_code = error.code
    incremental.run(inc_shards, config_path=inc_config, clear_stop=True)

    rollback_workspace = DEMO / "rollback_probe"
    if rollback_workspace.exists():
        shutil.rmtree(rollback_workspace)
    shutil.copytree(incremental.workspace, rollback_workspace)
    rollback_store = MappingStore(rollback_workspace)
    rollback_store.current_path.write_bytes(
        (rollback_store.versions_dir / "00000001.json").read_bytes()
    )
    try:
        rollback_store.validate()
        rollback_classification = None
    except BusinessError as error:
        rollback_classification = error.details.get("classification")
        rollback_code = error.code
    else:
        rollback_code = None

    server, thread = start_server_thread(full.workspace)
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with urllib.request.urlopen(base + "/health") as response:
            http_health = json.loads(response.read())
        with urllib.request.urlopen(base + "/status") as response:
            http_status = json.loads(response.read())
        label = urllib.parse.quote("名")
        with urllib.request.urlopen(base + f"/mapping/candidates?label={label}") as response:
            http_candidate = json.loads(response.read())["candidates"][0]["tag"]
        with urllib.request.urlopen(base + "/summary") as response:
            http_summary = json.loads(response.read())
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    manifest = read_json(full.output_dir / "manifest.json")
    summary = read_json(full.output_dir / "summary.json")
    expected_digest_path = ROOT / "samples" / "expected_fixed_digest.txt"
    expected_digest = (
        expected_digest_path.read_text(encoding="utf-8").strip()
        if expected_digest_path.exists()
        else None
    )
    if expected_digest is not None and manifest["fixed_digest"] != expected_digest:
        raise RuntimeError(
            f"fixed digest changed: {manifest['fixed_digest']} != {expected_digest}"
        )
    report = {
        "schema": "posalign.demo.v1",
        "normal_shards": [path.name for path in shards],
        "full_run": {
            "status": full_result["status"],
            "sequence": full_result["sequence"],
            "mapping_version": full_result["mapping_version"],
            "fixed_digest": manifest["fixed_digest"],
            "expected_fixed_digest_match": expected_digest
            in (None, manifest["fixed_digest"]),
            "sentences": summary["alignment"]["sentences"],
            "aligned": summary["alignment"]["aligned"],
            "v1_unsegmented": summary["v1"]["unsegmented_count"],
            "mapping_conflicts": summary["mapping"]["conflicts"],
        },
        "graceful_stop_and_resume": {
            "stopped_after_sentences": stopped["stop"]["completed_sentences"],
            "stopped_sequence": stopped["sequence"],
            "resumed_status": resumed_result["status"],
            "byte_identical_outputs": byte_identical,
        },
        "v1_roundtrip": {
            "records": export_result["records"],
            "digest": export_result["digest"],
            "exact_semantic_roundtrip": v1_exact,
        },
        "alignment_failure": failure_result,
        "incremental_shard_change": {
            "mapping_version": shard_run["mapping_version"],
            "reasons": shard_reasons,
            "reused_products": shard_reused,
        },
        "incremental_threshold_change": {
            "mapping_version": threshold_run["mapping_version"],
            "reused_products": threshold_reused,
        },
        "stale_mapping_guard": {
            "stopped_status": stale_stop["status"],
            "blocked_query_code": stale_block_code,
        },
        "rollback_detection": {
            "code": rollback_code,
            "classification": rollback_classification,
        },
        "http": {
            "health": http_health["ok"],
            "status_sequence": http_status["checkpoints"]["sequence"],
            "candidate_for_名": http_candidate,
            "summary_v1_unsegmented": http_summary["v1"]["unsegmented_count"],
        },
    }
    atomic_write_json(DEMO / "DEMO_REPORT.json", report, pretty=True)

    print("POS alignment/migration demo completed")
    print(f"  fixed digest: {manifest['fixed_digest']}")
    print(f"  resume byte-identical: {byte_identical}")
    print(f"  v1 round-trip exact: {v1_exact}")
    print(f"  alignment failure: {failure_result['code']} ({failure_result['sent_id']})")
    print(f"  stale mapping guard: {stale_block_code}")
    print(f"  rollback detection: {rollback_code}/{rollback_classification}")
    print(f"  report: {DEMO / 'DEMO_REPORT.json'}")


if __name__ == "__main__":
    main()

