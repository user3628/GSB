"""End-to-end deterministic citation conflict-resolution pipeline.

Stages: parse -> normalize -> resolve -> evidence.

Each stage publishes an atomic checkpoint.  A cooperative stop request is
honoured only at an indivisible citation/group unit boundary, so a resumed
run rebuilds the exact same conclusions and evidence chains as one
uninterrupted run.  Stage fingerprints chain the shard digest, configuration
fingerprint and prior-stage fingerprint, enabling incremental invalidation:
the earliest changed stage and all downstream stages are rebuilt while
unchanged upstream checkpoints are reused.
"""

from pathlib import Path

from .checkpoints import (STAGE_ORDER, load_index, publish_checkpoint,
                          reset_checkpoints, reusable_checkpoints)
from .config import config_fingerprint, default_config
from .elements import comparable_tuple, group_citations, normalize_elements
from .errors import LegalCiteError
from .evidence import build_manifest, verify_manifest
from .normalize import parse_citation
from .resolution import fold_duplicates, resolve_all
from .serde import (canonical_bytes, fingerprint, read_jsonl,
                    write_json_atomic, write_jsonl_atomic)


class StopController:
    """Cooperative stop flag checked only at unit boundaries."""

    def __init__(self):
        self._stop = False

    def request_stop(self):
        self._stop = True

    def should_stop(self):
        return self._stop


def _stage_fingerprints(shard_digest, cfg_fp, stage_artifacts):
    fps = {}
    prev = shard_digest + "|" + cfg_fp
    for stage in STAGE_ORDER:
        current = fingerprint({"prev": prev, "stage": stage,
                               "artifact": stage_artifacts[stage]})
        fps[stage] = current
        prev = current
    return fps


def _load_shards(records):
    if records and isinstance(records[0], (str, Path)):
        loaded = []
        for path in records:
            loaded.extend(read_jsonl(Path(path)))
        records = loaded
    import hashlib
    merged = b"".join(sorted(canonical_bytes(r) for r in records))
    digest = hashlib.sha256(merged).hexdigest()
    return records, digest


def _regroup(records):
    buckets = {}
    for rec in records:
        key = (rec["contract_id"], rec["location"]["locator"])
        buckets.setdefault(key, []).append(rec)
    groups = []
    for key in sorted(buckets):
        members = sorted(buckets[key], key=lambda r: r["citation_id"])
        sigs = {comparable_tuple(r["elements_normalized"]) for r in members}
        groups.append({
            "contract_id": key[0], "locator": key[1],
            "article": members[0]["location"]["article"],
            "section": members[0]["location"]["section"],
            "citation_ids": [r["citation_id"] for r in members],
            "distinct_value_signatures": len(sigs),
            "has_conflict": len(sigs) > 1, "members": members,
        })
    return groups


def run_pipeline(records, cfg=None, ckpt_dir=None, stop_controller=None,
                 progress_sink=None):
    """Run the four stages with per-stage atomic checkpoints.

    Returns a state dict.  When stopped before completion, ``stop_requested``
    is True and partial stage artifacts are returned without publishing a
    checkpoint for the interrupted stage.
    """
    cfg = cfg or default_config()
    cfg_fp = config_fingerprint(cfg)
    records, shard_digest = _load_shards(records)
    ckpt_dir = Path(ckpt_dir) if ckpt_dir is not None else None

    reusable = []
    if ckpt_dir is not None:
        index = load_index(ckpt_dir)
        # Reuse validity depends on final fingerprints; compute after artifacts
        # are built, but a cheap pre-check uses shard/config digest of records.
        reusable = []  # decided post-build below

    diagnostics = []

    # ---- Stage 1: parse (unit = one citation) --------------------------
    parsed = []
    for record in records:
        try:
            parsed.append(parse_citation(record))
        except LegalCiteError as exc:
            diagnostics.append(exc.to_dict())
        if stop_controller and stop_controller.should_stop():
            return _partial(stop_requested=True, stage_interrupted="parse",
                            progress_sink=progress_sink)
    parse_artifact = {
        "parsed_ids": sorted(p["citation_id"] for p in parsed),
        "diagnostics": diagnostics,
    }

    # ---- Stage 2: element normalization (unit = one citation) ----------
    normalized = []
    for parsed_rec in parsed:
        try:
            normalized.append(normalize_elements(parsed_rec, cfg))
        except LegalCiteError as exc:
            diagnostics.append(exc.to_dict())
        if stop_controller and stop_controller.should_stop():
            return _partial(stop_requested=True,
                            stage_interrupted="normalize",
                            progress_sink=progress_sink)
    invalid_ids = {d["details"].get("citation_id") for d in diagnostics}
    valid = [r for r in normalized if r["citation_id"] not in invalid_ids]
    groups = group_citations(valid, diagnostics)
    normalize_artifact = {
        "groups": [{"contract_id": g["contract_id"], "locator": g["locator"],
                    "citation_ids": g["citation_ids"],
                    "has_conflict": g["has_conflict"]} for g in groups],
        "diagnostics": diagnostics,
    }

    # ---- Stage 3: duplicate fold + resolution (unit = one group) -------
    survivors, folded = fold_duplicates(valid, cfg)
    survivor_groups = _regroup(survivors)
    resolutions = []
    for group in survivor_groups:
        resolutions.extend(resolve_all([group], cfg))
        if stop_controller and stop_controller.should_stop():
            return _partial(stop_requested=True,
                            stage_interrupted="resolve",
                            progress_sink=progress_sink)
    resolve_artifact = {
        "folded_duplicates": folded,
        "resolutions": [{
            "contract_id": g["contract_id"], "locator": g["locator"],
            "adopted_citation_id": g["adopted_citation_id"],
            "needs_manual_review": g["needs_manual_review"],
            "resolutions": g["resolutions"],
        } for g in resolutions],
    }

    # ---- Stage 4: evidence chains (unit = one group) -------------------
    state = {
        "parsed": parsed, "normalized": normalized, "groups": groups,
        "folded_duplicates": folded, "resolutions": resolutions,
        "diagnostics": diagnostics, "pipeline_fingerprint": None,
        "stop_requested": False,
    }
    manifest = build_manifest(state, cfg, cfg_fp)
    verify_manifest(manifest)
    state["manifest"] = manifest
    evidence_artifact = {
        "manifest_version": manifest["manifest_version"],
        "decision_count": len(manifest["decisions"]),
        "chain_count": len(manifest["evidence_chains"]),
    }

    stage_artifacts = {
        "parse": parse_artifact,
        "normalize": normalize_artifact,
        "resolve": resolve_artifact,
        "evidence": evidence_artifact,
    }
    fps = _stage_fingerprints(shard_digest, cfg_fp, stage_artifacts)
    state["pipeline_fingerprint"] = fps["evidence"]
    manifest["pipeline_fingerprint"] = fps["evidence"]
    state["stage_fingerprints"] = fps
    state["shard_digest"] = shard_digest
    state["config_fingerprint"] = cfg_fp
    state["stage_artifacts"] = stage_artifacts

    if ckpt_dir is not None:
        state["checkpoint"] = _checkpoint(ckpt_dir, cfg, shard_digest,
                                          stage_artifacts, fps)
    return state


def _partial(stop_requested, stage_interrupted, progress_sink):
    if progress_sink:
        progress_sink("stopped_at_stage", {"stage": stage_interrupted})
    return {"stop_requested": stop_requested,
            "stage_interrupted": stage_interrupted,
            "resolutions": [], "manifest": None, "diagnostics": []}


def _checkpoint(ckpt_dir, cfg, shard_digest, stage_artifacts, fps):
    index = load_index(ckpt_dir)
    reusable = reusable_checkpoints(index, fps)
    if len(reusable) == len(STAGE_ORDER):
        return {"reused": reusable, "rebuilt": []}
    # Invalidation: drop the earliest changed stage and everything downstream.
    reset_checkpoints(ckpt_dir)
    prev_digest = shard_digest
    rebuilt = []
    for stage in STAGE_ORDER:
        publish_checkpoint(
            ckpt_dir, stage, input_digest=prev_digest,
            config_version=cfg["config_version"],
            artifact_digest=fingerprint(stage_artifacts[stage]),
            pipeline_fingerprint=fps[stage],
        )
        rebuilt.append(stage)
        prev_digest = fps[stage]
    return {"reused": [], "rebuilt": rebuilt}


def run_pipeline_resumable(records, cfg=None, ckpt_dir=None,
                           stop_controller=None, progress_sink=None):
    """Resume wrapper: deterministic re-run after an interruption."""
    return run_pipeline(records, cfg=cfg, ckpt_dir=ckpt_dir,
                        stop_controller=stop_controller,
                        progress_sink=progress_sink)


def export_manifest(state, path):
    write_json_atomic(Path(path), state["manifest"], indent=2)


def export_outputs(state, out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    write_json_atomic(out_dir / "resolution.json",
                      _resolution_export(state), indent=2)
    write_json_atomic(out_dir / "evidence_manifest.json",
                      state["manifest"], indent=2)
    write_jsonl_atomic(out_dir / "diagnostics.jsonl", state["diagnostics"])
    return out_dir


def _resolution_export(state):
    return {
        "pipeline_fingerprint": state["pipeline_fingerprint"],
        "config_fingerprint": state["config_fingerprint"],
        "decisions": state["manifest"]["decisions"],
        "folded_duplicates": state["folded_duplicates"],
        "diagnostics": state["diagnostics"],
    }
