"""Pipeline orchestration: fingerprints, cache, journal, incremental runs."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import time

from . import config as config_mod
from .encode import encode_stage, parse_label
from .errors import (
    ConfigError,
    InconsistentError,
    InvalidStageError,
    SampleError,
    StateError,
)
from .infer import infer_stage
from .select import select_stage
from .store import Store
from .summarize import summarize_stage
from .util import (
    Deadline,
    canonical_json,
    first_diff_offset,
    read_csv_columns,
    sha256_bytes,
    sha256_file,
)

STAGES = ("infer", "encode", "select", "summarize")


class StopRequested(Exception):
    """Graceful stop: finish the current indivisible unit, then exit."""


class Engine:
    def __init__(self, state_dir: str):
        self.store = Store(state_dir)

    def _require_config(self):
        cfg_store = self.store.load_config_store()
        if cfg_store is None:
            raise ConfigError("config not initialized; run 'config-init' first")
        return cfg_store

    def _require_sample(self):
        reg = self.store.load_sample_registry()
        if reg is None:
            raise SampleError("no sample imported; run 'import-sample' first")
        if not os.path.exists(reg["path"]):
            raise SampleError("imported sample file is missing", {"path": reg["path"]})
        return reg

    def plan(self, cfg_store: dict, sample_fp: str):
        """Deterministic per-stage fingerprints and cache keys."""
        plan = []
        upstream_fp = "genesis:" + sample_fp
        for stage in STAGES:
            sections_fp = config_mod.stage_sections_hash(cfg_store["config"], stage)
            stage_fp = sha256_bytes(
                ("%s|%s|%s|%s" % (stage, sample_fp, sections_fp, upstream_fp)).encode("utf-8")
            )
            eff_version = config_mod.effective_version(cfg_store, stage)
            cache_key = sha256_bytes(
                ("%s|%d|%s" % (sample_fp, eff_version, stage_fp)).encode("utf-8")
            )
            plan.append(
                {
                    "stage": stage,
                    "stage_fp": stage_fp,
                    "eff_version": eff_version,
                    "cache_key": cache_key,
                    "upstream_fp": upstream_fp,
                }
            )
            upstream_fp = stage_fp
        return plan

    def _check_stop(self, stop_flag):
        if (stop_flag is not None and stop_flag()) or self.store.stop_requested():
            raise StopRequested()

    def _sleep(self, seconds: float, stop_flag):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            self._check_stop(stop_flag)
            time.sleep(min(0.05, max(0.0, end - time.monotonic())))

    def _publish(self, step, cfg_store, sample_fp, meta_bytes, matrix_bytes):
        record = {
            "seq": self.store.head_seq() + 1,
            "stage": step["stage"],
            "sample_fp": sample_fp,
            "config_version": cfg_store["version"],
            "stage_fp": step["stage_fp"],
            "cache_key": step["cache_key"],
            "upstream_fp": step["upstream_fp"],
            "artifact_digest": sha256_bytes(meta_bytes),
        }
        if matrix_bytes is not None:
            record["matrix_digest"] = sha256_bytes(matrix_bytes)
        self.store.publish(record)

    def _compute(self, stage, columns, cfg, artifacts, matrix_bytes, y, deadline, step):
        if stage == "infer":
            meta = infer_stage(columns, cfg)
            return meta, None
        if stage == "encode":
            meta, matrix, _y = encode_stage(columns, artifacts["infer"], cfg, deadline)
            return meta, matrix
        if stage == "select":
            enc = artifacts["encode"]
            names = [c["name"] for c in enc["columns"]]
            meta = select_stage(
                matrix_bytes, enc["rows"], enc["cols"], names, y, cfg, deadline
            )
            return meta, None
        if stage == "summarize":
            meta = summarize_stage(
                artifacts["infer"], artifacts["encode"], matrix_bytes,
                artifacts["select"], step["sample_fp"],
            )
            return meta, None
        raise StateError("unknown stage", {"stage": stage})

    def run(self, budget_ms=None, stage_sleep_ms=0, stop_flag=None) -> dict:
        """Run the pipeline incrementally and return a run report.

        Stage sources: 'journal' (already published, reused), 'cache'
        (artifact reused, record published now), 'computed' (recomputed).
        Raises LatencyBudgetExceeded when the budget is exceeded; returns
        status 'stopped' on a graceful stop request.
        """
        self.store.ensure()
        cfg_store = self._require_config()
        reg = self._require_sample()
        sample_fp = sha256_file(reg["path"])
        _, columns = read_csv_columns(reg["path"])
        cfg = cfg_store["config"]
        y = parse_label(columns, cfg)
        budget = budget_ms if budget_ms is not None else cfg["runtime"]["latency_budget_ms"]
        deadline = Deadline(budget)
        self.store.clear_stop()
        plan = self.plan(cfg_store, sample_fp)
        published = {r["cache_key"] for r in self.store.recover()}
        report = {
            "status": "completed",
            "sample_fp": sample_fp,
            "config_version": cfg_store["version"],
            "budget_ms": budget,
            "stages": [],
        }
        artifacts = {}
        matrix_bytes = None
        try:
            for step in plan:
                step["sample_fp"] = sample_fp
                self._check_stop(stop_flag)
                deadline.check(step["stage"])
                t0 = time.monotonic()
                key = step["cache_key"]
                if key in published:
                    source = "journal"
                    meta_bytes, mat = self.store.read_artifact(key)
                elif self.store.cache_has(key):
                    source = "cache"
                    meta_bytes, mat = self.store.read_artifact(key)
                    self._publish(step, cfg_store, sample_fp, meta_bytes, mat)
                else:
                    source = "computed"
                    meta_obj, mat = self._compute(
                        step["stage"], columns, cfg, artifacts, matrix_bytes, y, deadline, step
                    )
                    meta_obj["stage_fp"] = step["stage_fp"]
                    meta_obj["sample_fp"] = sample_fp
                    meta_bytes = canonical_json(meta_obj)
                    self.store.write_artifact(key, meta_bytes, mat)
                    self._publish(step, cfg_store, sample_fp, meta_bytes, mat)
                artifacts[step["stage"]] = json.loads(meta_bytes.decode("utf-8"))
                if step["stage"] == "encode":
                    matrix_bytes = mat
                report["stages"].append(
                    {
                        "stage": step["stage"],
                        "source": source,
                        "ms": round((time.monotonic() - t0) * 1000.0, 3),
                        "stage_fp": step["stage_fp"],
                        "cache_key": key,
                    }
                )
                if stage_sleep_ms:
                    self._sleep(stage_sleep_ms / 1000.0, stop_flag)
                deadline.check(step["stage"])
        except StopRequested:
            report["status"] = "stopped"
            report["completed_stages"] = [s["stage"] for s in report["stages"]]
            return report
        summary = artifacts["summarize"]
        report["summary_digest"] = summary["digest"]
        report["summary"] = summary
        report["elapsed_ms"] = round(deadline.elapsed_ms, 3)
        return report

    def status(self) -> dict:
        out = {"state_dir": self.store.root}
        cfg_store = self.store.load_config_store()
        reg = self.store.load_sample_registry()
        records = self.store.recover()
        out["journal"] = {
            "head_seq": len(records),
            "records": [
                {
                    "seq": r["seq"],
                    "stage": r["stage"],
                    "config_version": r["config_version"],
                    "cache_key": r["cache_key"],
                    "stage_fp": r["stage_fp"],
                }
                for r in records
            ],
        }
        out["config"] = None
        if cfg_store is not None:
            out["config"] = {
                "version": cfg_store["version"],
                "section_versions": cfg_store["section_versions"],
            }
        out["sample"] = reg
        out["plan"] = []
        if cfg_store is not None and reg is not None and os.path.exists(reg["path"]):
            sample_fp = sha256_file(reg["path"])
            published = {r["cache_key"] for r in records}
            for step in self.plan(cfg_store, sample_fp):
                out["plan"].append(
                    {
                        "stage": step["stage"],
                        "stage_fp": step["stage_fp"],
                        "eff_version": step["eff_version"],
                        "cache_key": step["cache_key"],
                        "published": step["cache_key"] in published,
                        "cached": self.store.cache_has(step["cache_key"]),
                    }
                )
        return out

    def stage_artifact(self, stage: str) -> dict:
        if stage not in STAGES:
            raise InvalidStageError(
                "unknown stage name", {"stage": stage, "known": list(STAGES)}
            )
        cfg_store = self._require_config()
        reg = self._require_sample()
        sample_fp = sha256_file(reg["path"])
        for step in self.plan(cfg_store, sample_fp):
            if step["stage"] == stage:
                if not self.store.cache_has(step["cache_key"]):
                    raise StateError(
                        "stage not materialized for current config; run first",
                        {"stage": stage},
                    )
                meta, _ = self.store.read_artifact(step["cache_key"])
                return json.loads(meta.decode("utf-8"))
        raise StateError("unknown stage", {"stage": stage})

    def selection(self) -> dict:
        art = self.stage_artifact("select")
        return {
            "method": art["method"],
            "kept": art["kept"],
            "dropped": art["dropped"],
            "kept_count": art["kept_count"],
            "dropped_count": art["dropped_count"],
        }

    def verify(self) -> dict:
        """Full from-scratch rebuild in a temp state; byte-compare artifacts."""
        self.store.ensure()
        cfg_store = self._require_config()
        reg = self._require_sample()
        sample_fp = sha256_file(reg["path"])
        plan = self.plan(cfg_store, sample_fp)
        for step in plan:
            if not self.store.cache_has(step["cache_key"]):
                raise StateError(
                    "stage not materialized; run the pipeline before verify",
                    {"stage": step["stage"]},
                )
        tmp = tempfile.mkdtemp(prefix="workbench-verify-")
        try:
            fresh = Store(tmp)
            fresh.ensure()
            fresh.write_config_store(cfg_store)
            fresh.write_sample_registry(reg)
            Engine(tmp).run(budget_ms=None)
            diffs = []
            for step in plan:
                key = step["cache_key"]
                meta_a, mat_a = self.store.read_artifact(key)
                meta_b, mat_b = fresh.read_artifact(key)
                if meta_a != meta_b:
                    diffs.append(
                        {
                            "stage": step["stage"],
                            "kind": "meta",
                            "offset": first_diff_offset(meta_a, meta_b),
                            "expected_sha256": sha256_bytes(meta_a),
                            "actual_sha256": sha256_bytes(meta_b),
                        }
                    )
                if (mat_a or b"") != (mat_b or b""):
                    diffs.append(
                        {
                            "stage": step["stage"],
                            "kind": "matrix",
                            "offset": first_diff_offset(mat_a or b"", mat_b or b""),
                            "expected_sha256": sha256_bytes(mat_a or b""),
                            "actual_sha256": sha256_bytes(mat_b or b""),
                        }
                    )
            if diffs:
                raise InconsistentError(
                    "incremental state diverges from a full rebuild", {"diffs": diffs}
                )
            return {"consistent": True, "stages_checked": [s["stage"] for s in plan]}
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
