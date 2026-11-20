"""Parallel shard execution engine with deterministic reduction."""
from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .budget import BudgetPlan, plan_admission
from .checkpoint import CheckpointRecord, CheckpointStore, atomic_write_json
from .config import WorkbenchConfig
from .errors import (E_INVALID_STAGE, E_PARSE_FAILED, StructuredError,
                     WorkbenchError)
from .locking import file_lock
from .parser import Arc, SentenceParse, parse_sentence
from .shards import Shard

RUN_STATE_FILENAME = "run_state.json"
PLAN_FILENAME = "plan.json"
RESULTS_DIRNAME = "results"
STOP_FILENAME = "STOP"
RUN_STAGES = ("planned", "running", "stopped", "completed", "failed")


@dataclass
class ShardResult:
    shard_index: int
    filename: str
    worker_id: int
    input_fingerprint: str
    parses: list[SentenceParse]
    token_count: int

    def summary(self) -> dict[str, Any]:
        arc_count = sum(len(p.arcs) for p in self.parses)
        root_count = sum(1 for p in self.parses for h in p.heads if h == 0)
        return {
            "shard_index": self.shard_index,
            "filename": self.filename,
            "worker_id": self.worker_id,
            "input_fingerprint": self.input_fingerprint,
            "sentence_count": len(self.parses),
            "token_count": self.token_count,
            "arc_count": arc_count,
            "root_count": root_count,
            "total_score": round(sum(p.total_score for p in self.parses), 9),
        }


@dataclass
class RunReport:
    status: str
    plan: BudgetPlan
    results: list[ShardResult]
    structured_errors: list[StructuredError] = field(default_factory=list)
    stop_requested: bool = False
    reused_checkpoints: list[int] = field(default_factory=list)
    invalidated_shards: list[int] = field(default_factory=list)
    invalidation_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "stop_requested": self.stop_requested,
            "plan": self.plan.to_dict(),
            "completed_shards": [r.summary() for r in self.results],
            "reused_checkpoints": self.reused_checkpoints,
            "invalidated_shards": self.invalidated_shards,
            "invalidation_reason": self.invalidation_reason,
            "errors": [e.to_dict() for e in self.structured_errors],
        }


def _summarize_relations(results: list[ShardResult]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for result in results:
        for parse in result.parses:
            for relation in parse.relations:
                counts[relation] = counts.get(relation, 0) + 1
    return dict(sorted(counts.items()))


def reduce_results(results: list[ShardResult]) -> dict[str, Any]:
    ordered = sorted(results, key=lambda r: r.shard_index)
    parses = [p for r in ordered for p in sorted(r.parses, key=lambda x: x.sentence_id)]
    total_tokens = sum(r.token_count for r in ordered)
    total_arcs = sum(len(parse.arcs) for shard_result in ordered
                     for parse in shard_result.parses)
    sentence_count = sum(len(r.parses) for r in ordered)
    score = round(sum(p.total_score for p in parses), 9)
    distance_hist: dict[int, int] = {}
    for parse in parses:
        for distance in parse.distances:
            distance_hist[distance] = distance_hist.get(distance, 0) + 1
    return {
        "shard_count": len(ordered),
        "sentence_count": sentence_count,
        "token_count": total_tokens,
        "arc_count": total_arcs,
        "root_count": sum(1 for p in parses for h in p.heads if h == 0),
        "total_score": score,
        "relation_counts": _summarize_relations(ordered),
        "distance_histogram": dict(sorted(distance_hist.items())),
    }


class Engine:
    """Runs shard parsing inside a workspace directory."""

    def __init__(self, workspace_dir: str | Path, shards: list[Shard],
                 config: WorkbenchConfig, checkpoint_store: CheckpointStore,
                 shard_complete_callback=None):
        self.workspace = Path(workspace_dir)
        self.workspace.mkdir(parents=True, exist_ok=True)
        (self.workspace / RESULTS_DIRNAME).mkdir(exist_ok=True)
        self.shards = shards
        self.config = config
        self.checkpoints = checkpoint_store
        self.state_path = self.workspace / RUN_STATE_FILENAME
        self.plan_path = self.workspace / PLAN_FILENAME
        self.stop_path = self.workspace / STOP_FILENAME
        self._state_lock = threading.Lock()
        self.shard_complete_callback = shard_complete_callback

    def request_stop(self) -> None:
        self.stop_path.write_text("stop requested\n", encoding="utf-8")

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def clear_stop(self) -> None:
        if self.stop_path.exists():
            self.stop_path.unlink()

    def _write_state(self, stage: str, extra: dict[str, Any] | None = None) -> None:
        payload: dict[str, Any] = {
            "stage": stage,
            "config_fingerprint": self.config.config_fingerprint,
            "model_fingerprint": self.config.model_fingerprint,
            "budget_fingerprint": self.config.budget_fingerprint,
            "shard_fingerprints": [s.fingerprint for s in self.shards],
        }
        if extra:
            payload.update(extra)
        atomic_write_json(self.state_path, payload)

    def read_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"stage": "new"}
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def require_stage(self, allowed: tuple[str, ...]) -> dict[str, Any]:
        state = self.read_state()
        if state.get("stage") not in allowed:
            raise WorkbenchError(
                E_INVALID_STAGE,
                f"stage '{state.get('stage')}' not in {list(allowed)}",
                {"current_stage": state.get("stage"), "allowed": list(allowed)},
            )
        return state

    def invalidate_stale_checkpoints(self) -> tuple[int | None, str | None]:
        """Find earliest shard whose inputs/config drifted and drop it onward."""
        records, warnings = self.checkpoints.load_consistent_prefix()
        if not records:
            return None, None
        shard_by_index = {s.index: s for s in self.shards}
        earliest: int | None = None
        reason: str | None = None
        for record in records:
            shard = shard_by_index.get(record.shard_index)
            if shard is None:
                earliest, reason = record.shard_index, "INPUT_CHANGED"
                break
            if shard.fingerprint != record.input_fingerprint:
                earliest, reason = record.shard_index, "INPUT_CHANGED"
                break
            if self.config.model_fingerprint != record.model_fingerprint:
                earliest, reason = record.shard_index, "MODEL_CHANGED"
                break
            if self.config.config_fingerprint != record.config_fingerprint:
                earliest, reason = record.shard_index, "BUDGET_OR_CONFIG_CHANGED"
                break
        if earliest is None and len(records) > len(self.shards):
            earliest, reason = len(self.shards), "INPUT_CHANGED"
        if earliest is not None:
            self._purge_from(earliest)
        return earliest, reason

    def _purge_from(self, shard_index: int) -> None:
        with file_lock(self.checkpoints.lock_path):
            for path in self.checkpoints.root.glob("shard_*.checkpoint.json"):
                stem_index = int(path.name.split("_")[1].split(".")[0])
                if stem_index >= shard_index:
                    path.unlink()
            result_dir = self.workspace / RESULTS_DIRNAME
            for path in result_dir.glob("shard_*.json"):
                stem_index = int(path.name.split("_")[1].split(".")[0])
                if stem_index >= shard_index:
                    path.unlink()
            prefix, _ = self.checkpoints.load_consistent_prefix()
            last = prefix[-1].shard_index if prefix else -1
            atomic_write_json(self.checkpoints.sequence_path,
                              {"last_shard_index": last})

    def plan(self) -> BudgetPlan:
        plan = plan_admission(self.shards, self.config.budget,
                              self.config.parallel.worker_threads)
        atomic_write_json(self.plan_path, plan.to_dict())
        self._write_state("planned", {"plan_global_consumed": plan.global_consumed})
        return plan

    def _process_shard(self, shard: Shard, worker_id: int) -> ShardResult:
        parses: list[SentenceParse] = []
        for position, sentence in enumerate(shard.sentences):
            sentence_id = f"shard{shard.index:02d}-sent{position:04d}"
            parses.append(parse_sentence(sentence, self.config.model, sentence_id))
        return ShardResult(
            shard_index=shard.index, filename=shard.filename,
            worker_id=worker_id, input_fingerprint=shard.fingerprint,
            parses=parses, token_count=shard.token_count,
        )

    def _persist_result(self, result: ShardResult, sequence: int) -> None:
        record = CheckpointRecord.create(
            shard_index=result.shard_index, sequence=sequence,
            input_fingerprint=result.input_fingerprint,
            model_fingerprint=self.config.model_fingerprint,
            config_fingerprint=self.config.config_fingerprint,
            result_summary=result.summary(),
        )
        self.checkpoints.publish(record)
        payload = {
            "summary": result.summary(),
            "sentences": [p.to_dict() for p in result.parses],
        }
        atomic_write_json(
            self.workspace / RESULTS_DIRNAME / f"shard_{result.shard_index:04d}.json",
            payload,
        )

    def run(self, *, sequential: bool = False) -> RunReport:
        with self._state_lock:
            invalidated, invalidation_reason = self.invalidate_stale_checkpoints()
            plan = self.plan()
            self._write_state("running", {"plan_global_consumed": plan.global_consumed})
            records, warnings = self.checkpoints.load_consistent_prefix()
            reusable = {r.shard_index for r in records}
            admitted = {d.shard_index for d in plan.admitted_shards}
            todo = [
                shard for shard in self.shards
                if shard.index in admitted and shard.index not in reusable
            ]
            worker_of = {d.shard_index: d.worker_id for d in plan.decisions}
            results: list[ShardResult] = []
            errors: list[StructuredError] = list(warnings)
            reused = sorted(reusable & admitted)
            stop_hit = False

            def finish(status: str) -> RunReport:
                self._write_state(status, {
                    "plan_global_consumed": plan.global_consumed,
                    "completed_shards": [r.shard_index for r in results],
                })
                return RunReport(
                    status=status, plan=plan, results=results,
                    structured_errors=errors, stop_requested=stop_hit,
                    reused_checkpoints=reused,
                    invalidated_shards=list(range(invalidated, len(self.shards)))
                    if invalidated is not None else [],
                    invalidation_reason=invalidation_reason,
                )

            def handle_failure(exc: BaseException, shard_index: int) -> RunReport:
                if isinstance(exc, WorkbenchError):
                    structured = StructuredError.from_exception(exc)
                else:
                    structured = StructuredError(
                        E_PARSE_FAILED, f"{type(exc).__name__}: {exc}",
                        {"shard_index": shard_index},
                    )
                structured.details.setdefault("shard_index", shard_index)
                errors.append(structured)
                return finish("failed")

            if sequential or self.config.parallel.worker_threads == 1 or not todo:
                for shard in todo:
                    if self.stop_requested():
                        stop_hit = True
                        return finish("stopped")
                    try:
                        result = self._process_shard(
                            shard, worker_of[shard.index])
                        self._persist_result(result, result.shard_index)
                        results.append(result)
                        if self.shard_complete_callback is not None:
                            self.shard_complete_callback(result.shard_index)
                    except BaseException as exc:  # structured reporting, no silent loss
                        return handle_failure(exc, shard.index)
                return finish("completed")

            pending: dict[Any, Shard] = {}
            with ThreadPoolExecutor(
                max_workers=self.config.parallel.worker_threads,
                thread_name_prefix="dep-worker",
            ) as pool:
                for shard in todo:
                    future = pool.submit(self._process_shard, shard,
                                         worker_of[shard.index])
                    pending[future] = shard
                expected_index = todo[0].index
                buffered: dict[int, Any] = {}
                remaining = set(pending)
                while expected_index <= todo[-1].index:
                    if expected_index in buffered:
                        chosen = buffered.pop(expected_index)
                    else:
                        import concurrent.futures as cf
                        done, _ = cf.wait(set(remaining),
                                          return_when=cf.FIRST_COMPLETED)
                        chosen = None
                        for future in done:
                            remaining.discard(future)
                            shard_done = pending[future]
                            if shard_done.index == expected_index:
                                chosen = future
                            else:
                                buffered[shard_done.index] = future
                        if chosen is None:
                            continue
                    shard = pending[chosen]
                    if self.stop_requested():
                        stop_hit = True
                        pool.shutdown(wait=True, cancel_futures=True)
                        return finish("stopped")
                    try:
                        result = chosen.result()
                    except BaseException as exc:
                        pool.shutdown(wait=True, cancel_futures=True)
                        return handle_failure(exc, shard.index)
                    if result.shard_index != expected_index:
                        pool.shutdown(wait=True, cancel_futures=True)
                        errors.append(StructuredError(
                            E_INVALID_STAGE,
                            f"out-of-order publish shard {result.shard_index}, "
                            f"expected {expected_index}",
                            {"shard_index": result.shard_index,
                             "expected_shard_index": expected_index},
                        ))
                        return finish("failed")
                    try:
                        self._persist_result(result, result.shard_index)
                        results.append(result)
                        if self.shard_complete_callback is not None:
                            self.shard_complete_callback(result.shard_index)
                    except BaseException as exc:
                        pool.shutdown(wait=True, cancel_futures=True)
                        return handle_failure(exc, result.shard_index)
                    expected_index += 1
            return finish("completed")

    def load_persisted_results(self, up_to_shard: int | None = None
                               ) -> list[ShardResult]:
        """Rebuild full ShardResult objects from persisted result files."""
        records = self.checkpoints.require_prefix(
            up_to_shard if up_to_shard is not None
            else self._max_persisted_index()
        )
        loaded: list[ShardResult] = []
        result_dir = self.workspace / RESULTS_DIRNAME
        for record in records:
            path = result_dir / f"shard_{record.shard_index:04d}.json"
            if not path.exists():
                raise WorkbenchError(
                    E_INVALID_STAGE,
                    f"result payload missing for shard {record.shard_index}",
                    {"shard_index": record.shard_index, "path": str(path)},
                )
            payload = json.loads(path.read_text(encoding="utf-8"))
            parses = [
                SentenceParse(
                    sentence_id=item["sentence_id"], text=item["text"],
                    tokens=item["tokens"], tags=item["tags"],
                    arcs=[Arc(**arc) for arc in item["arcs"]],
                    heads=item["heads"], relations=item["relations"],
                    distances=item["distances"],
                    total_score=item["total_score"], actions=item["actions"],
                )
                for item in payload["sentences"]
            ]
            summary = payload["summary"]
            loaded.append(ShardResult(
                shard_index=summary["shard_index"],
                filename=summary["filename"],
                worker_id=summary["worker_id"],
                input_fingerprint=summary["input_fingerprint"],
                parses=parses, token_count=summary["token_count"],
            ))
        return loaded

    def _max_persisted_index(self) -> int:
        prefix, _ = self.checkpoints.load_consistent_prefix()
        if not prefix:
            raise WorkbenchError(
                E_INVALID_STAGE, "no checkpoints available for aggregation",
                {"checkpoint_dir": str(self.checkpoints.root)},
            )
        return prefix[-1].shard_index

    def aggregate(self, up_to_shard: int | None = None) -> dict[str, Any]:
        results = self.load_persisted_results(up_to_shard)
        stats = reduce_results(results)
        plan_data = (
            json.loads(self.plan_path.read_text(encoding="utf-8"))
            if self.plan_path.exists() else None
        )
        return {
            "stats": stats,
            "plan": plan_data,
            "shards": [r.summary() for r in results],
        }

    def export_results(self, export_path: str | Path) -> Path:
        results = self.load_persisted_results()
        payload = {
            "config_fingerprint": self.config.config_fingerprint,
            "model_fingerprint": self.config.model_fingerprint,
            "shards": [
                {
                    "summary": result.summary(),
                    "sentences": [parse.to_dict() for parse in result.parses],
                }
                for result in sorted(results, key=lambda r: r.shard_index)
            ],
        }
        target = Path(export_path)
        atomic_write_json(target, payload)
        return target
