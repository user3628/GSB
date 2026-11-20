"""Four-stage pipeline with fingerprints, incremental reuse and graceful stop."""
from __future__ import annotations

import json
import os

from . import checkpoints as cp
from . import features as feat_mod
from . import normalize as norm_mod
from . import scoring as score_mod
from . import strategy as strat
from .config import Config
from .errors import E_BAD_INPUT, E_INCREMENTAL_MISMATCH, raise_error
from .util import atomic_write, canonical_dumps, digest_file, digest_obj, sha256_hex, write_json

STOP_FILE = "STOP"
STAGE_NAMES = ["normalize", "features", "scoring", "summary"]


class StopAfterUnit(Exception):
    """Raised only at an indivisible clause-unit boundary."""


def load_shards(shard_paths: list[str]) -> tuple[list[dict], str, list[dict]]:
    clauses: list[dict] = []
    shard_info = []
    for path in sorted(shard_paths):
        if not os.path.exists(path):
            raise_error(E_BAD_INPUT, f"shard not found: {path}")
        shard_info.append({"path": os.path.basename(path), "sha256": digest_file(path)})
        with open(path, "r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise_error(E_BAD_INPUT, f"invalid JSONL in {path}:{line_no}: {exc}")
                for required in ("clause_id", "contract_id", "text", "lang", "template_id"):
                    if required not in record:
                        raise_error(
                            E_BAD_INPUT,
                            f"missing field {required} in {path}:{line_no}",
                        )
                clauses.append(record)
    if not clauses:
        raise_error(E_BAD_INPUT, "no clauses found in the given shards")
    ids = [c["clause_id"] for c in clauses]
    if len(ids) != len(set(ids)):
        raise_error(E_BAD_INPUT, "duplicate clause_id detected across shards")
    clauses.sort(key=lambda c: c["clause_id"])
    shard_info.sort(key=lambda s: s["path"])
    input_digest = digest_obj(
        {"shards": shard_info, "clause_ids": [c["clause_id"] for c in clauses]}
    )
    return clauses, input_digest, shard_info


class Pipeline:
    def __init__(self, config: Config, workdir: str):
        self.config = config
        self.workdir = workdir
        os.makedirs(workdir, exist_ok=True)
        self.store = cp.CheckpointStore(workdir)

    # -- stop control --------------------------------------------------
    def request_stop(self) -> None:
        atomic_write(os.path.join(self.workdir, STOP_FILE), b"stop\n")

    def clear_stop(self) -> None:
        path = os.path.join(self.workdir, STOP_FILE)
        if os.path.exists(path):
            os.unlink(path)

    def stop_requested(self) -> bool:
        return os.path.exists(os.path.join(self.workdir, STOP_FILE))

    # -- fingerprints --------------------------------------------------
    def expected_digests(self, clauses: list[dict]) -> dict[str, str]:
        normalized = norm_mod.run(clauses)
        features = feat_mod.run(normalized, self.config)
        scored = score_mod.run(features, self.config)
        total_chars = sum(len(c["text"]) for c in clauses)
        final = strat.run(scored, features, self.config, total_chars)
        return {
            "normalize": digest_obj(normalized),
            "features": digest_obj(features),
            "scoring": digest_obj(scored),
            "summary": digest_obj(final),
        }

    # -- main entry ----------------------------------------------------
    def run(self, shard_paths: list[str], force: bool = False) -> dict:
        clauses, input_digest, shard_info = load_shards(shard_paths)
        records = self.store.load_records()
        if force and records:
            self.store.reset()
            records = []
        if records:
            # Sequence regression and artifact/input replacement are hard failures;
            # a config fingerprint change is handled by digest-based invalidation.
            self.store.verify_chain(
                records[0]["config_fingerprint"], input_digest
            )

        expected = self.expected_digests(clauses)
        artifacts = self.store.stage_artifacts()

        # Incremental invalidation: keep only the prefix whose content digest matches.
        valid_prefix = -1
        for idx, stage in enumerate(STAGE_NAMES):
            record = artifacts.get(stage)
            if record and record["artifact_digest"] == expected[stage]:
                valid_prefix = idx
            else:
                break
        removed = self.store.rollback_to(valid_prefix)

        reused = [s for s in STAGE_NAMES[: valid_prefix + 1]]
        stages_at_start = set(self.store.stage_artifacts())
        stopped = False
        stop_stage = None

        # Recompute from the earliest invalidated stage downstream.
        normalized = self.store.load_artifact("normalize") if "normalize" in reused else None
        features = self.store.load_artifact("features") if "features" in reused else None
        scored = self.store.load_artifact("scoring") if "scoring" in reused else None

        try:
            if normalized is None:
                normalized = self._compute_stage(
                    "normalize", clauses, input_digest, expected,
                    lambda: [norm_mod.normalize_clause(c) for c in clauses],
                    "stage1_normalized.json",
                )
            if self.stop_requested():
                raise StopAfterUnit()
            if features is None:
                features = self._compute_stage(
                    "features", normalized, input_digest, expected,
                    lambda: self._loop_clauses(
                        normalized, lambda item: feat_mod.extract_clause(item, self.config)
                    ),
                    "stage2_features.json",
                )
            if self.stop_requested():
                raise StopAfterUnit()
            if scored is None:
                scored = self._compute_stage(
                    "scoring", features, input_digest, expected,
                    lambda: self._loop_clauses(
                        features, lambda item: score_mod.score_clause(item, self.config)
                    ),
                    "stage3_scoring.json",
                )
            if self.stop_requested():
                raise StopAfterUnit()
            total_chars = sum(len(c["text"]) for c in clauses)
            final = self.store.load_artifact("summary") if "summary" in reused else None
            if final is None:
                final = self._compute_stage(
                    "summary", scored, input_digest, expected,
                    lambda: strat.run(scored, features, self.config, total_chars),
                    "stage4_summary.json",
                )
        except StopAfterUnit:
            stopped = True
            stop_stage = self._inflight_stage(
                list(self.store.stage_artifacts()), reused
            )

        report = {
            "input_digest": input_digest,
            "shards": shard_info,
            "config_version": self.config.config_version,
            "config_fingerprint": self.config.fingerprint,
            "reused_stages": reused,
            "recomputed_stages": [],
            "stopped": stopped,
            "stop_stage": stop_stage,
            "expected_digests": expected,
        }
        if not stopped:
            self.clear_stop()
            final = self.store.load_artifact("summary")
            report["recomputed_stages"] = [
                s for s in STAGE_NAMES if s not in stages_at_start
            ]
            report["completed_stages"] = list(STAGE_NAMES)
            report["summary"] = final["summary"]
            report["export_paths"] = self._export(final, input_digest)
            report["final_report_digest"] = digest_file(
                os.path.join(self.workdir, "summary_report.json")
            )
            self._assert_byte_identical(expected)
        else:
            report["recomputed_stages"] = [
                s
                for s in STAGE_NAMES
                if s in self.store.stage_artifacts() and s not in stages_at_start
            ]
            report["completed_stages"] = [s for s in STAGE_NAMES if s in self.store.stage_artifacts()]
        return report

    def resume(self, shard_paths: list[str]) -> dict:
        self.clear_stop()
        return self.run(shard_paths)

    def _loop_clauses(self, items, fn):
        output = []
        for idx, item in enumerate(items):
            output.append(fn(item))
            # Boundary check happens only after the indivisible clause unit finished.
            if self.stop_requested():
                raise StopAfterUnit()
        return output

    def _inflight_stage(self, done_stages, reused):
        done = set(done_stages) | set(reused)
        for stage in STAGE_NAMES:
            if stage not in done:
                return stage
        return None

    def _compute_stage(self, stage, data, input_digest, expected, producer, artifact_path):
        artifact = producer()
        actual = digest_obj(artifact)
        if actual != expected[stage]:
            # Defensive: pure local functions must always reproduce the same digest.
            raise_error(
                E_INCREMENTAL_MISMATCH,
                "stage digest self-check failed",
                {"stage": stage, "expected": expected[stage], "actual": actual},
            )
        records = self.store.load_records()
        expected_seq = records[-1]["seq"] if records else 0
        self.store.publish(
            stage=stage,
            input_digest=input_digest,
            config_fingerprint=self.config.fingerprint,
            config_version=self.config.config_version,
            artifact_path=artifact_path,
            artifact=artifact,
            expected_seq=expected_seq,
        )
        return artifact

    def _export(self, final: dict, input_digest: str) -> dict:
        level_list_path = os.path.join(self.workdir, "level_list.json")
        report_path = os.path.join(self.workdir, "summary_report.json")
        write_json(level_list_path, final["level_list"])
        report = {
            "input_digest": input_digest,
            "config_version": self.config.config_version,
            "config_fingerprint": self.config.fingerprint,
            "generated_at_utc": "deterministic-no-wallclock",
            "tier": final["tier"],
            "strategy_decision": final["strategy_decision"],
            "summary": final["summary"],
            "level_list_digest": sha256_hex(canonical_dumps(final["level_list"])),
        }
        write_json(report_path, report)
        return {"level_list": level_list_path, "summary_report": report_path}

    def _assert_byte_identical(self, expected: dict) -> None:
        final = self.store.load_artifact("summary")
        if digest_obj(final) != expected["summary"]:
            raise_error(
                E_INCREMENTAL_MISMATCH,
                "summary artifact digest differs from clean computation",
                {"stage": "summary"},
            )


def verify_workdir_outputs(reference_workdir: str, candidate_workdir: str) -> None:
    """Byte-level comparison of exported artifacts between two work directories."""
    pairs = [
        "level_list.json",
        "summary_report.json",
        "stage1_normalized.json",
        "stage2_features.json",
        "stage3_scoring.json",
        "stage4_summary.json",
    ]
    diffs = []
    for name in pairs:
        path_a = os.path.join(reference_workdir, name)
        path_b = os.path.join(candidate_workdir, name)
        if not os.path.exists(path_a) or not os.path.exists(path_b):
            diffs.append({"file": name, "reason": "missing in one workdir"})
            continue
        with open(path_a, "rb") as fa, open(path_b, "rb") as fb:
            same = sha256_hex(fa.read()) == sha256_hex(fb.read())
        if not same:
            diffs.append({"file": name, "reason": "byte content differs"})
    if diffs:
        raise_error(
            E_INCREMENTAL_MISMATCH,
            "incremental output differs from clean full run",
            {"differences": diffs},
        )
