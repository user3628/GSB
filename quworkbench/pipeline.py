"""Pipeline orchestration: version fingerprints, invalidation propagation,
incremental rebuilds and full-rebuild equivalence checks.

Stage chain: normalize -> classify -> slots. Each stage version fingerprint
is composed from the content digests of its own inputs plus the upstream
stage fingerprint, so a change in any input invalidates exactly the earliest
affected stage and everything downstream of it.
"""

import json
import os
import shutil
import tempfile
from pathlib import Path

from .canon import canonical, digest
from .errors import (
    E_INCREMENTAL_MISMATCH,
    WorkbenchError,
)
from .model import score_intent, train_nb
from .slots import extract_slots, load_dictionary
from .store import STAGES, CheckpointStore
from .tokenizer import normalize_config_section, normalize_query

CONFIG_PATH = Path("config/pipeline_config.json")
INVALIDATIONS_JOURNAL = "invalidations.jsonl"


def load_config(workspace):
    path = Path(workspace) / CONFIG_PATH
    return json.loads(path.read_bytes().decode("utf-8"))


def save_config(workspace, config):
    path = Path(workspace) / CONFIG_PATH
    tmp = path.with_suffix(".json.tmp")
    data = canonical(config)
    with open(tmp, "wb") as fh:
        fh.write(data + b"\n")
    os.replace(tmp, path)


class Pipeline:
    def __init__(self, workspace):
        self.ws = Path(workspace)
        self.store = CheckpointStore(self.ws)
        self.config = load_config(self.ws)

    # ------------------------------------------------------------- raw data
    def shard_ids(self):
        shards = self.ws / "data" / "shards"
        return sorted(p.stem for p in shards.glob("*.jsonl"))

    def load_shard(self, shard_id):
        path = self.ws / "data" / "shards" / f"{shard_id}.jsonl"
        records = []
        for line in path.read_bytes().decode("utf-8").splitlines():
            if line.strip():
                records.append(json.loads(line))
        return records

    def write_shard(self, shard_id, records):
        shards = self.ws / "data" / "shards"
        shards.mkdir(parents=True, exist_ok=True)
        path = shards / f"{shard_id}.jsonl"
        tmp = shards / f".{shard_id}.jsonl.tmp"
        with open(tmp, "wb") as fh:
            for rec in records:
                fh.write(canonical(rec) + b"\n")
        os.replace(tmp, path)

    def shard_digest(self, shard_id):
        return digest({"shard_id": shard_id, "records": self.load_shard(shard_id)})

    def labeled_samples(self):
        path = self.ws / self.config["classify"]["labeled_path"]
        out = []
        for line in path.read_bytes().decode("utf-8").splitlines():
            if line.strip():
                out.append(json.loads(line))
        return out

    def labeled_digest(self):
        return digest(self.labeled_samples())

    def dictionary(self):
        return load_dictionary(self.ws / self.config["slots"]["dict_path"])

    # ------------------------------------------------------ version fingerprints
    def input_digests(self, shard_id, stage):
        cfg = self.config
        if stage == "normalize":
            return {
                "shard_content": self.shard_digest(shard_id),
                "normalize_config": digest(normalize_config_section(cfg["normalize"])),
            }
        if stage == "classify":
            c = cfg["classify"]
            return {
                "upstream": self.version_fingerprint(shard_id, "normalize"),
                "labeled_samples": self.labeled_digest(),
                "smoothing_alpha": digest(c["smoothing_alpha"]),
                "class_weights": digest(c["class_weights"]),
                "unknown_token": digest(c["unknown_token"]),
            }
        if stage == "slots":
            s = cfg["slots"]
            return {
                "upstream": self.version_fingerprint(shard_id, "classify"),
                "slot_dictionary": self.dictionary()["digest"],
                "slot_priority": digest(s["priority"]),
            }
        raise ValueError(stage)

    def version_fingerprint(self, shard_id, stage):
        return digest({"stage": stage, "inputs": self.input_digests(shard_id, stage)})

    # ------------------------------------------------------------ staleness
    def staleness(self, shard_id, stage):
        current = self.store.consistent(shard_id, stage)
        fingerprint = self.version_fingerprint(shard_id, stage)
        if current is None:
            return True, ["missing_checkpoint"], fingerprint
        if current.record.get("config_fingerprint") == fingerprint:
            return False, [], fingerprint
        old_inputs = current.record.get("input_digests", {})
        new_inputs = self.input_digests(shard_id, stage)
        reasons = []
        for key in sorted(set(old_inputs) | set(new_inputs)):
            if old_inputs.get(key) != new_inputs.get(key):
                reasons.append("upstream_invalidated" if key == "upstream" else f"{key}_changed")
        if not reasons:
            reasons = ["fingerprint_mismatch"]
        return True, reasons, fingerprint

    # --------------------------------------------------------- stage builders
    def _build_normalize(self, shard_id, fingerprint):
        results = []
        for rec in self.load_shard(shard_id):
            qid = rec["query_id"]
            try:
                normalized, tokens = normalize_query(
                    rec.get("raw_query", ""), query_id=qid, config=self.config["normalize"]
                )
                results.append(
                    {
                        "query_id": qid,
                        "status": "ok",
                        "normalized": normalized,
                        "tokens": tokens,
                    }
                )
            except WorkbenchError as exc:
                results.append({"query_id": qid, "status": "error", "error": exc.to_dict()})
        return {
            "stage": "normalize",
            "shard_id": shard_id,
            "version_fingerprint": fingerprint,
            "results": results,
        }

    def _train_model(self):
        cfg = self.config["classify"]
        labeled = []
        for sample in self.labeled_samples():
            _, tokens = normalize_query(sample["raw_query"], query_id=sample.get("sample_id"))
            labeled.append(([t["text"] for t in tokens], sample["intent"]))
        return train_nb(
            labeled,
            cfg["smoothing_alpha"],
            cfg["unknown_token"],
            cfg["class_weights"],
            labeled_digest=self.labeled_digest(),
        )

    def _build_classify(self, shard_id, fingerprint):
        model = self._train_model()
        normalize_artifact = self.store.read_artifact(shard_id, "normalize")
        results = []
        for item in normalize_artifact["results"]:
            qid = item["query_id"]
            if item["status"] != "ok":
                results.append({"query_id": qid, "status": "error", "error": item["error"]})
                continue
            token_texts = [t["text"] for t in item["tokens"]]
            intent, confidence, scores = score_intent(model, token_texts)
            results.append(
                {
                    "query_id": qid,
                    "status": "ok",
                    "intent": intent,
                    "confidence": confidence,
                    "scores": scores,
                }
            )
        return {
            "stage": "classify",
            "shard_id": shard_id,
            "version_fingerprint": fingerprint,
            "model_digest": model.model_digest,
            "results": results,
        }

    def _build_slots(self, shard_id, fingerprint):
        dictionary = self.dictionary()
        priority = self.config["slots"]["priority"]
        normalize_artifact = self.store.read_artifact(shard_id, "normalize")
        results = []
        for item in normalize_artifact["results"]:
            qid = item["query_id"]
            if item["status"] != "ok":
                results.append({"query_id": qid, "status": "error", "error": item["error"]})
                continue
            accepted, suppressed = extract_slots(item["normalized"], dictionary, priority)
            results.append(
                {
                    "query_id": qid,
                    "status": "ok",
                    "slots": accepted,
                    "diagnostics": {"suppressed": suppressed},
                }
            )
        return {
            "stage": "slots",
            "shard_id": shard_id,
            "version_fingerprint": fingerprint,
            "dict_version": dictionary["version"],
            "results": results,
        }

    def _build_stage(self, shard_id, stage, fingerprint):
        if stage == "normalize":
            return self._build_normalize(shard_id, fingerprint)
        if stage == "classify":
            return self._build_classify(shard_id, fingerprint)
        return self._build_slots(shard_id, fingerprint)

    # ------------------------------------------------------------------- run
    def _publish_stage(self, shard_id, stage, fingerprint):
        artifact = self._build_stage(shard_id, stage, fingerprint)
        record = {
            "shard_id": shard_id,
            "shard_fingerprint": self.shard_digest(shard_id),
            "config_fingerprint": fingerprint,
            "input_digests": self.input_digests(shard_id, stage),
        }
        return self.store.publish(
            shard_id,
            stage,
            record,
            artifact,
            expected_shard_fingerprint=self.shard_digest(shard_id),
        )

    def _log_invalidation(self, event):
        path = self.store.ckpt_dir / INVALIDATIONS_JOURNAL
        with open(path, "ab") as fh:
            fh.write(canonical(event) + b"\n")

    def run(self):
        """Incrementally rebuild every stale stage; reuse the rest."""
        invalidations = []
        for shard_id in self.shard_ids():
            for stage in STAGES:
                stale, reasons, fingerprint = self.staleness(shard_id, stage)
                if not stale:
                    continue
                current = self.store.consistent(shard_id, stage)
                record = self._publish_stage(shard_id, stage, fingerprint)
                event = {
                    "shard_id": shard_id,
                    "stage": stage,
                    "reasons": reasons,
                    "old_fingerprint": current.record["config_fingerprint"] if current else None,
                    "new_fingerprint": fingerprint,
                    "new_seq": record["seq"],
                }
                invalidations.append(event)
                self._log_invalidation(event)
        return {"invalidations": invalidations, "summary": self.summary()}

    def run_full(self):
        """Rebuild everything from an empty checkpoint tree."""
        if self.store.ckpt_dir.exists():
            shutil.rmtree(self.store.ckpt_dir)
        self.store.ckpt_dir.mkdir(parents=True, exist_ok=True)
        return self.run()

    # -------------------------------------------------------------- mutation
    def replace_shard(self, shard_id, records):
        """Single-shard replacement entry point."""
        self.write_shard(shard_id, records)
        return self.run()

    def change_config(self, key, value):
        """Single-config-change entry point. key is dotted, e.g.
        'classify.smoothing_alpha'."""
        parts = key.split(".")
        node = self.config
        for part in parts[:-1]:
            node = node[part]
        node[parts[-1]] = value
        save_config(self.ws, self.config)
        self.config = load_config(self.ws)
        return self.run()

    # --------------------------------------------------------------- summary
    def summary(self):
        """Deterministic run summary (sequence numbers excluded)."""
        shards = {}
        total = {"queries": 0, "ok": 0, "errors": 0}
        error_codes = {}
        for shard_id in self.shard_ids():
            stages = {}
            for stage in STAGES:
                artifact = self.store.read_artifact(shard_id, stage)
                if artifact is None:
                    continue
                stages[stage] = {
                    "version_fingerprint": artifact["version_fingerprint"],
                    "results_digest": digest(artifact["results"]),
                }
                if stage == "classify":
                    for item in artifact["results"]:
                        total["queries"] += 1
                        if item["status"] == "ok":
                            total["ok"] += 1
                        else:
                            total["errors"] += 1
                            code = item["error"]["code"]
                            error_codes[code] = error_codes.get(code, 0) + 1
            shards[shard_id] = stages
        summary = {"shards": shards, "counts": dict(total, error_codes=error_codes)}
        summary["digest"] = digest(summary)
        return summary

    def status(self):
        shards = {}
        for shard_id in self.shard_ids():
            stages = {}
            for stage in STAGES:
                stale, reasons, fingerprint = self.staleness(shard_id, stage)
                current = self.store.consistent(shard_id, stage)
                stages[stage] = {
                    "seq": current.seq if current else 0,
                    "checkpoint_fingerprint": current.record["config_fingerprint"] if current else None,
                    "current_fingerprint": fingerprint,
                    "stale": stale,
                    "reasons": reasons,
                }
            shards[shard_id] = stages
        return {"shards": shards, "summary": self.summary()}

    def invalidations(self):
        path = self.store.ckpt_dir / INVALIDATIONS_JOURNAL
        events = []
        if path.exists():
            for line in path.read_bytes().decode("utf-8").splitlines():
                if line.strip():
                    events.append(json.loads(line))
        return events

    def slot_diagnostics(self, query_id):
        for shard_id in self.shard_ids():
            artifact = self.store.read_artifact(shard_id, "slots")
            if artifact is None:
                continue
            for item in artifact["results"]:
                if item["query_id"] == query_id:
                    return dict(item, shard_id=shard_id, dict_version=artifact.get("dict_version"))
        return None

    # --------------------------------------------------------------- recover
    def recover(self):
        return self.store.recover()

    # -------------------------------------------------- incremental vs full
    def verify_against_full(self):
        """Rebuild from scratch in a temp workspace and compare byte-for-byte."""
        with tempfile.TemporaryDirectory(prefix="quw-full-") as tmp:
            ws2 = Path(tmp) / "ws"
            shutil.copytree(self.ws / "data", ws2 / "data")
            shutil.copytree(self.ws / "config", ws2 / "config")
            other = Pipeline(ws2)
            other.run_full()
            for shard_id in sorted(set(self.shard_ids()) | set(other.shard_ids())):
                for stage in STAGES:
                    mine = self.store.read_artifact(shard_id, stage)
                    theirs = other.store.read_artifact(shard_id, stage)
                    a = mine["results"] if mine else None
                    b = theirs["results"] if theirs else None
                    if canonical(a) == canonical(b):
                        continue
                    amap = {r["query_id"]: r for r in a or []}
                    bmap = {r["query_id"]: r for r in b or []}
                    first = None
                    for qid in sorted(set(amap) | set(bmap)):
                        if canonical(amap.get(qid)) != canonical(bmap.get(qid)):
                            first = qid
                            break
                    raise WorkbenchError(
                        E_INCREMENTAL_MISMATCH,
                        "incremental_results_differ_from_full_rebuild",
                        stage=stage,
                        details={"query_id": first, "shard_id": shard_id},
                    )
            if digest(self.summary()) != digest(other.summary()):
                raise WorkbenchError(
                    E_INCREMENTAL_MISMATCH,
                    "summary_digest_differs_from_full_rebuild",
                    stage="summary",
                    details={"query_id": None, "shard_id": None},
                )
            return {"match": True, "digest": self.summary()["digest"]}
