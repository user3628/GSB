#!/usr/bin/env python3
"""Deterministic end-to-end demonstration and acceptance report."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
import threading
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sparse_index.checkpoints import CheckpointStore
from sparse_index.config import IndexConfig
from sparse_index.errors import PipelineError
from sparse_index.http_api import LocalIndexService, make_handler
from sparse_index.pipeline import Pipeline
from sparse_index.query import QueryEngine
from sparse_index.utils import canonical_json_bytes, sha256_bytes, sha256_file


def _json_digest(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def _artifact_fingerprint(workspace: Path) -> dict[str, str]:
    paths = [
        workspace / "artifacts" / "stage1" / "terms.json",
        workspace / "artifacts" / "stage2" / "statistics.json",
        workspace / "artifacts" / "stage3" / "postings.bin",
        workspace / "artifacts" / "stage3" / "postings.json",
        workspace / "artifacts" / "stage4" / "query_results.json",
    ]
    return {
        path.relative_to(workspace).as_posix(): sha256_file(path)
        for path in paths
    }


def _cache_probe(config: IndexConfig, workspace: Path) -> dict[str, Any]:
    engine = QueryEngine(config, workspace / "artifacts")
    first = engine.execute(query_id="cache-probe", terms=["检索", "索引"], mode="and", limit=20)
    assert engine.cache.stats()["size"] == 1
    entries_path = workspace / "artifacts" / "cache" / "query_cache.json"
    if entries_path.exists():
        entries_path.unlink()
    second = engine.execute(query_id="cache-probe", terms=["检索", "索引"], mode="and", limit=20)
    if first["result"] != second["result"]:
        raise PipelineError("E_QUERY_MISMATCH", "缓存命中与未命中结果不一致")

    # Force capacity pressure with three independent queries.  The probe must
    # still return exactly the same field-for-field result after eviction.
    for index, term in enumerate(["压缩", "缓存", "跳表"]):
        engine.execute(query_id=f"fill-{index}", terms=[term], mode="or", limit=20)
    after_eviction = engine.execute(query_id="cache-probe", terms=["检索", "索引"], mode="and", limit=20)
    if first["result"] != after_eviction["result"]:
        raise PipelineError("E_QUERY_MISMATCH", "缓存淘汰后结果发生变化")
    return {
        "cache_hit_first": first["meta"]["cache_hit"],
        "cache_hit_second": second["meta"]["cache_hit"],
        "first_equals_second": first["result"] == second["result"],
        "after_eviction_equals_first": after_eviction["result"] == first["result"],
        "cache_stats": engine.cache.stats(),
    }


def _stop_resume_probe(config: IndexConfig, source_workspace: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="sparse-index-resume-") as temp_name:
        temp_workspace = Path(temp_name)
        config_dir = temp_workspace / "data"
        shutil.copytree(config.base_dir, config_dir)
        resume_config = IndexConfig.load(config_dir / "index.yaml")
        pipeline = Pipeline(resume_config, temp_workspace)
        counter = {"value": 0}

        def stop_after_three() -> bool:
            counter["value"] += 1
            return counter["value"] > 3

        stopped = False
        try:
            pipeline.build(stop_check=stop_after_three)
        except Exception as exc:
            if exc.__class__.__name__ != "GracefulStop":
                raise
            stopped = True
        if not stopped:
            raise AssertionError("stop probe did not stop")
        resumed = pipeline.build()
        resumed_fingerprint = _artifact_fingerprint(temp_workspace)
        reference_fingerprint = _artifact_fingerprint(source_workspace)
        if resumed_fingerprint != reference_fingerprint:
            raise PipelineError(
                "E_QUERY_MISMATCH",
                "停止恢复后的产物与完整构建不一致",
                details={"resumed": resumed_fingerprint, "reference": reference_fingerprint},
            )
        return {
            "stopped_at_safe_boundary": stopped,
            "resumed_status": resumed["status"],
            "digests_equal_to_complete_build": True,
            "artifact_digest": _json_digest(resumed_fingerprint),
        }


def _invalidation_probe(config: IndexConfig, source_workspace: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="sparse-index-invalidation-") as temp_name:
        temp_workspace = Path(temp_name)
        shutil.copytree(source_workspace / "data", temp_workspace / "data")
        base_config = IndexConfig.load(temp_workspace / "data" / "index.yaml")
        base_pipeline = Pipeline(base_config, temp_workspace)
        base_pipeline.build()
        before = _artifact_fingerprint(temp_workspace)
        raw = json.loads(json.dumps(base_config.raw))
        raw["postings"]["skip_step"] = 5
        variant_path = temp_workspace / "data" / "index_skip5.yaml"
        import yaml

        variant_path.write_text(
            yaml.safe_dump(raw, allow_unicode=True, sort_keys=True),
            encoding="utf-8",
        )
        changed_config = IndexConfig.load(variant_path)
        changed_pipeline = Pipeline(changed_config, temp_workspace)
        result = changed_pipeline.build()
        after = _artifact_fingerprint(temp_workspace)
        event = result["invalidation"]
        if event is None or event["earliest_affected_stage"] != 3:
            raise PipelineError("E_INVALID_STAGE", "增量失效探测未定位到阶段 3")
        return {
            "earliest_affected_stage": event["earliest_affected_stage"],
            "affected_stages": event["affected_stages"],
            "reasons": event["reasons"],
            "stage1_reused": before["artifacts/stage1/terms.json"] == after["artifacts/stage1/terms.json"],
            "stage2_reused": before["artifacts/stage2/statistics.json"] == after["artifacts/stage2/statistics.json"],
            "stage3_rebuilt": before["artifacts/stage3/postings.json"] != after["artifacts/stage3/postings.json"],
            "new_stage_fingerprints": event["new_stage_fingerprints"],
        }


def _http_probe(config: IndexConfig, workspace: Path) -> dict[str, Any]:
    service = LocalIndexService(config, workspace)
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(service))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}/query",
            data=canonical_json_bytes(
                {"query_id": "http-probe", "terms": ["检索", "索引"], "mode": "and", "limit": 20}
            ),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=5) as response:
            health = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    return {
        "health": health.get("status"),
        "query_total_hits": payload.get("result", {}).get("total_hits"),
        "query_ok": payload.get("ok"),
    }


def _error_probes(workspace: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="sparse-index-invalid-skip-") as temp_name:
        temp_workspace = Path(temp_name)
        shutil.copytree(workspace / "data", temp_workspace / "data")
        invalid_config = IndexConfig.load(
            temp_workspace / "data" / "error_cases" / "invalid_skip_step.yaml"
        )
        invalid_pipeline = Pipeline(invalid_config, temp_workspace)
        skip_error: dict[str, Any]
        try:
            invalid_pipeline.build()
        except PipelineError as exc:
            skip_error = {"code": exc.code, "details": exc.details}
        else:
            raise AssertionError("invalid skip step did not fail")

    oob_payload = json.loads(
        (workspace / "data" / "error_cases" / "query_out_of_bounds.json").read_text(encoding="utf-8")
    )
    config = IndexConfig.load(workspace / "data" / "index.yaml")
    engine = QueryEngine(config, workspace / "artifacts")
    try:
        engine.execute(
            query_id=oob_payload["query_id"],
            terms=oob_payload["terms"],
            mode=oob_payload["mode"],
            limit=oob_payload["limit"],
        )
    except PipelineError as exc:
        oob_error = {"code": exc.code, "details": exc.details}
    else:
        raise AssertionError("out-of-bounds query did not fail")

    zero_budget_config = IndexConfig.load(
        workspace / "data" / "error_cases" / "latency_budget_zero.yaml"
    )
    zero_budget_engine = QueryEngine(zero_budget_config, workspace / "artifacts")
    try:
        zero_budget_engine.execute(
            query_id="q-latency",
            terms=["检索"],
            mode="or",
            limit=20,
            use_cache=False,
        )
    except PipelineError as exc:
        latency_error = {
            "code": exc.code,
            "details": {
                key: value for key, value in exc.details.items() if key != "elapsed_ms"
            },
            "contains_elapsed_ms": "elapsed_ms" in exc.details,
        }
    else:
        raise AssertionError("zero latency budget did not fail")
    return {
        "invalid_skip_step": skip_error,
        "out_of_bounds_query": oob_error,
        "latency_budget": latency_error,
    }


def _checkpoint_probes(workspace: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="sparse-index-checkpoints-") as temp_name:
        artifacts = Path(temp_name) / "artifacts"
        store = CheckpointStore(artifacts)
        store.publish(
            stage="tokenize",
            input_digest="input-1",
            config_version="cfg-1",
            artifact_digest="artifact-1",
            stage_fingerprint="fp-1",
            expected_seq=1,
        )
        store.publish(
            stage="statistics",
            input_digest="input-2",
            config_version="cfg-1",
            artifact_digest="artifact-2",
            stage_fingerprint="fp-2",
            expected_seq=2,
        )
        stale: dict[str, Any]
        try:
            store.publish(
                stage="statistics",
                input_digest="input-2",
                config_version="cfg-1",
                artifact_digest="artifact-2",
                stage_fingerprint="fp-2",
                expected_seq=2,
            )
        except PipelineError as exc:
            stale = {"code": exc.code, "details": exc.details}
        else:
            detail = "duplicate/lagging publish unexpectedly succeeded"
            raise AssertionError(detail)
        invalid: dict[str, Any]
        try:
            store.publish(
                stage="query_summary",
                input_digest="input-4",
                config_version="cfg-1",
                artifact_digest="artifact-4",
                stage_fingerprint="fp-4",
                expected_seq=3,
            )
        except PipelineError as exc:
            invalid = {"code": exc.code, "details": exc.details}
        else:
            raise AssertionError("invalid stage jump did not fail")
    return {"stale_or_competing_publish": stale, "invalid_stage_jump": invalid}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", default=".")
    parser.add_argument("--config", default="data/index.yaml")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    workspace = Path(args.workspace).resolve()
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = workspace / config_path
    config = IndexConfig.load(config_path)
    pipeline = Pipeline(config, workspace)

    # A pristine build is required for the fixed report.  The demo owns its
    # artifacts directory but never regenerates or edits input shards.
    if (workspace / "artifacts" / "checkpoints").exists():
        for path in (workspace / "artifacts" / "checkpoints").glob("*.json"):
            path.unlink()
        archive = workspace / "artifacts" / "checkpoints" / "archive"
        if archive.exists():
            shutil.rmtree(archive)
        archive.mkdir(parents=True, exist_ok=True)
    for stage in range(1, 5):
        stage_dir = pipeline.stage_dir(stage)
        if stage_dir.exists():
            shutil.rmtree(stage_dir)
    for name in ("build_state.json", "invalidation_history.json"):
        path = workspace / "artifacts" / name
        if path.exists():
            path.unlink()
    cache_file = workspace / "artifacts" / "cache" / "query_cache.json"
    if cache_file.exists():
        cache_file.unlink()

    build = pipeline.build(force=True)
    stage4 = json.loads(
        (workspace / "artifacts" / "stage4" / "query_results.json").read_text(encoding="utf-8")
    )
    cache = _cache_probe(config, workspace)
    stop_resume = _stop_resume_probe(config, workspace)
    invalidation = _invalidation_probe(config, workspace)
    errors = _error_probes(workspace)
    http = _http_probe(config, workspace)
    checkpoints = _checkpoint_probes(workspace)
    artifact_digests = _artifact_fingerprint(workspace)
    report = {
        "ok": True,
        "seed": 20261120,
        "config_fingerprint": config.config_fingerprint,
        "dataset_digest": config.dataset_digest,
        "artifact_digests": artifact_digests,
        "build_status": build["status"],
        "stage_fingerprints": config.stage_fingerprints(),
        "query_summary": stage4["summary"],
        "query_hits": [
            {
                "query_id": item["query_id"],
                "total_hits": item["result"]["total_hits"],
                "hits": [
                    {"doc_id": hit["doc_id"], "score": hit["score"]}
                    for hit in item["result"]["hits"]
                ],
            }
            for item in stage4["queries"]
        ],
        "cache_verification": cache,
        "stop_resume_verification": stop_resume,
        "invalidation_verification": invalidation,
        "http_verification": http,
        "error_code_verification": errors,
        "checkpoint_verification": checkpoints,
        "final_status_up_to_date": pipeline.status()["up_to_date"],
    }
    report["report_digest"] = _json_digest(report)
    output_path = workspace / "artifacts" / "demo_report.json"
    output_path.write_bytes(canonical_json_bytes(report))
    if args.pretty:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    else:
        print(canonical_json_bytes(report).decode("utf-8"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
