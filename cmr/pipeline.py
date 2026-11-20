"""Four-stage build pipeline with version fingerprints, incremental
invalidation, per-shard checkpoints and graceful stop/resume.

Stages: features -> projection -> index -> recall.
"""

import json
import os

import numpy as np

from .canon import digest_file, digest_obj
from .checkpoint import StateStore, atomic_write_json, read_json
from .config import STAGES
from .errors import CmrError, E_DIM_MISMATCH, GracefulStop
from .image_features import block_hashes, extract_image_features, load_rgb
from .indexer import build_index, load_index
from .projection import project_and_normalize
from .retrieval import query_image, query_text
from .text_features import extract_text_features, normalize_text, term_weights


# ---------------------------------------------------------------- manifest
def load_manifest(data_dir, cfg, stage="features"):
    path = os.path.join(data_dir, "pairs.jsonl")
    meta, pairs = None, []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("type") == "manifest_meta":
                meta = obj
            else:
                pairs.append(obj)
    if meta is None:
        raise CmrError(E_DIM_MISMATCH, "manifest meta line with declared dims missing",
                       stage=stage)
    if (meta.get("declared_image_dim") != cfg.image_dim
            or meta.get("declared_text_dim") != cfg.text_dim):
        raise CmrError(
            E_DIM_MISMATCH,
            f"manifest declares dims ({meta.get('declared_image_dim')}, "
            f"{meta.get('declared_text_dim')}) but config yields "
            f"({cfg.image_dim}, {cfg.text_dim})",
            stage=stage,
            detail={
                "declared_image_dim": meta.get("declared_image_dim"),
                "declared_text_dim": meta.get("declared_text_dim"),
                "config_image_dim": cfg.image_dim,
                "config_text_dim": cfg.text_dim,
            },
        )
    pairs.sort(key=lambda p: p["pair_id"])
    return meta, pairs


# ---------------------------------------------------------------- fingerprints
def compute_fingerprints(data_dir, cfg):
    """Content-addressed version fingerprints per stage."""
    manifest_path = os.path.join(data_dir, "pairs.jsonl")
    projection_path = os.path.join(data_dir, "projection.json")
    manifest_digest = digest_file(manifest_path)

    pairs = []
    with open(manifest_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                obj = json.loads(line)
                if obj.get("type") != "manifest_meta":
                    pairs.append(obj)
    pairs.sort(key=lambda p: p["pair_id"])

    image_digests = {}
    shard_fps = {}
    for pair in pairs:
        img_path = os.path.join(data_dir, pair["image_file"])
        img_digest = digest_file(img_path) if os.path.exists(img_path) else "MISSING:" + pair["image_file"]
        image_digests[pair["pair_id"]] = img_digest
        shard_fps[pair["pair_id"]] = digest_obj({
            "pair_id": pair["pair_id"],
            "image_sha256": img_digest,
            "caption": pair["caption"],
            "lang": pair.get("lang", ""),
            "source": pair.get("source", ""),
        })
    input_fp = digest_obj({"manifest": manifest_digest, "images": image_digests})
    components = cfg.component_digests(projection_path)

    stage_fps = {}
    prev = input_fp
    for stage in STAGES:
        stage_fps[stage] = digest_obj({
            "stage": stage,
            "upstream": prev,
            "config_component": components[stage],
        })
        prev = stage_fps[stage]
    return {
        "input_fp": input_fp,
        "components": components,
        "stage_fps": stage_fps,
        "shard_fps": shard_fps,
    }


def _diff_reasons(old, new):
    reasons = []
    if old.get("input_fp") != new.get("input_fp"):
        old_shards = old.get("shard_fps", {})
        for shard, fp in new.get("shard_fps", {}).items():
            if old_shards.get(shard) != fp:
                reasons.append(f"input changed: shard {shard}")
        if not old_shards or not any(r.startswith("input changed: shard") for r in reasons):
            reasons.append("input changed: manifest or images")
    old_c, new_c = old.get("components", {}), new.get("components", {})
    labels = {
        "features": "grid/ngram/feature params",
        "projection": "projection matrix",
        "index": "index params",
        "recall": "ranking weights",
    }
    for key in ("features", "projection", "index", "recall"):
        if old_c.get(key) != new_c.get(key):
            reasons.append(f"config changed: {labels[key]}")
    return reasons or ["unknown"]


def plan_rebuild(store, fps):
    """Return (stale_from_index or None, invalidation report)."""
    published = read_json(store.path("published", "fingerprints.json"))
    if not published:
        return 0, {
            "reason": ["no published fingerprints: cold start"],
            "rebuild_scope": STAGES[:],
            "previous_stage_fps": None,
            "current_stage_fps": fps["stage_fps"],
        }
    old_fps = published.get("stage_fps", {})
    stale_from = None
    for i, stage in enumerate(STAGES):
        if old_fps.get(stage) != fps["stage_fps"][stage]:
            stale_from = i
            break
    if stale_from is None:
        return None, {
            "reason": ["up to date"],
            "rebuild_scope": [],
            "previous_stage_fps": old_fps,
            "current_stage_fps": fps["stage_fps"],
        }
    return stale_from, {
        "reason": _diff_reasons(published, fps),
        "rebuild_scope": STAGES[stale_from:],
        "previous_stage_fps": old_fps,
        "current_stage_fps": fps["stage_fps"],
    }


# ---------------------------------------------------------------- stage units
def _unit_guard(store, stop_after, counter):
    if store.stop_requested():
        return True
    if stop_after is not None and counter[0] >= stop_after:
        return True
    counter[0] += 1
    return False


def _shard_dir(store, shard):
    path = store.path("shards", shard)
    os.makedirs(path, exist_ok=True)
    return path


def _run_features(store, data_dir, cfg, pair, fps):
    shard = pair["pair_id"]
    img_path = os.path.join(data_dir, pair["image_file"])
    img_feat = extract_image_features(img_path, cfg, stage="features")
    txt_feat = extract_text_features(pair["caption"], cfg)
    shard_dir = _shard_dir(store, shard)
    np.savez(os.path.join(shard_dir, "features.npz"),
             img_feat=img_feat, txt_feat=txt_feat)
    norm = normalize_text(pair["caption"])
    bow, ngram = term_weights(norm, cfg)
    img = load_rgb(img_path, stage="features")
    terms = {"bow": bow, "ngram": ngram,
             "blocks": block_hashes(img, cfg.grid, cfg.orient_bins)}
    atomic_write_json(os.path.join(shard_dir, "features_terms.json"), terms)
    return digest_obj({"img_feat": img_feat.tolist(), "txt_feat": txt_feat.tolist(),
                       "terms": terms})


def _run_projection(store, cfg, pair, projections):
    shard = pair["pair_id"]
    shard_dir = _shard_dir(store, shard)
    feats = np.load(os.path.join(shard_dir, "features.npz"))
    img_vec = project_and_normalize(projections["image"], feats["img_feat"], stage="projection")
    txt_vec = project_and_normalize(projections["text"], feats["txt_feat"], stage="projection")
    np.savez(os.path.join(shard_dir, "projected.npz"), img_vec=img_vec, txt_vec=txt_vec)
    return digest_obj({"img_vec": img_vec.tolist(), "txt_vec": txt_vec.tolist()})


def _run_recall(store, cfg, pairs, projections, data_dir):
    index_obj, mat = load_index(store)
    report = {"pairs": {}, "ok": True}
    for pair in pairs:
        t2i = query_text(index_obj, mat, cfg, projections, pair["caption"], 1)
        i2t = query_image(index_obj, mat, cfg, projections,
                          "pair:" + pair["pair_id"], 1, data_dir=data_dir)
        t2i_hit = bool(t2i["results"]) and t2i["results"][0]["pair_id"] == pair["pair_id"]
        i2t_hit = bool(i2t["results"]) and i2t["results"][0]["pair_id"] == pair["pair_id"]
        report["pairs"][pair["pair_id"]] = {"t2i_hit": t2i_hit, "i2t_hit": i2t_hit}
        report["ok"] = report["ok"] and t2i_hit and i2t_hit
    report["digest"] = digest_obj(report["pairs"])
    atomic_write_json(store.path("recall", "recall.json"), report)
    return report



def _check_forced_stage_prerequisites(store, pairs, from_stage):
    """Forced staged builds require upstream artifacts (E_INVALID_STAGE)."""
    idx = STAGES.index(from_stage)
    if idx >= 1:
        for pair in pairs:
            if not os.path.exists(store.path("shards", pair["pair_id"], "features.npz")):
                raise CmrError("E_INVALID_STAGE",
                               f"cannot start at '{from_stage}': features artifact missing "
                               f"for shard {pair['pair_id']}",
                               stage=from_stage,
                               detail={"missing_stage": "features", "shard": pair["pair_id"]})
    if idx >= 2:
        for pair in pairs:
            if not os.path.exists(store.path("shards", pair["pair_id"], "projected.npz")):
                raise CmrError("E_INVALID_STAGE",
                               f"cannot start at '{from_stage}': projection artifact missing "
                               f"for shard {pair['pair_id']}",
                               stage=from_stage,
                               detail={"missing_stage": "projection", "shard": pair["pair_id"]})
    if idx >= 3 and not os.path.exists(store.path("index", "index.json")):
        raise CmrError("E_INVALID_STAGE",
                       "cannot start at 'recall': index artifact missing",
                       stage=from_stage, detail={"missing_stage": "index"})


# ---------------------------------------------------------------- build
def run_build(data_dir, state_dir, cfg, from_stage=None, stop_after=None):
    """Run the pipeline. Returns a build report dict.

    from_stage: force (re)execution starting at the named stage, reusing
    upstream artifacts (staged-build trigger).
    stop_after: stop gracefully after N indivisible units (test hook).
    """
    store = StateStore(state_dir)
    meta, pairs = load_manifest(data_dir, cfg)
    fps = compute_fingerprints(data_dir, cfg)

    if from_stage is not None:
        if from_stage not in STAGES:
            raise CmrError("E_INVALID_STAGE", f"unknown stage '{from_stage}'")
        _check_forced_stage_prerequisites(store, pairs, from_stage)
        stale_idx = STAGES.index(from_stage)
        report_inv = {"reason": [f"forced from stage '{from_stage}'"],
                      "rebuild_scope": STAGES[stale_idx:],
                      "previous_stage_fps": read_json(store.path("published", "fingerprints.json"), {}).get("stage_fps"),
                      "current_stage_fps": fps["stage_fps"]}
    else:
        stale_idx, report_inv = plan_rebuild(store, fps)

    counter = [0]
    if stale_idx is not None:
        scope = STAGES[stale_idx:]
        store.clear_stage_artifacts(scope)
        atomic_write_json(store.path("published", "invalidation.json"), report_inv)

    projections = None
    units_done = []
    stopped = False

    def maybe_stop(stage, shard):
        nonlocal stopped
        if _unit_guard(store, stop_after, counter):
            stopped = True
            raise GracefulStop(stage, shard)

    try:
        # ---- stage 1: features (per shard) ----
        if stale_idx is not None and stale_idx <= 0:
            for pair in pairs:
                shard = pair["pair_id"]
                maybe_stop("features", shard)
                if store.valid_checkpoint(shard, "features", fps["stage_fps"]["features"],
                                          fps["shard_fps"][shard]) and \
                        os.path.exists(store.path("shards", shard, "features.npz")):
                    continue
                digest = _run_features(store, data_dir, cfg, pair, fps)
                store.publish_checkpoint({
                    "shard": shard, "stage": "features",
                    "stage_fp": fps["stage_fps"]["features"],
                    "shard_fp": fps["shard_fps"][shard],
                    "artifact_digest": digest,
                })
                units_done.append(f"features:{shard}")

        # ---- stage 2: projection (per shard) ----
        if stale_idx is not None and stale_idx <= 1:
            projections = _load_projections_checked(data_dir, cfg)
            for pair in pairs:
                shard = pair["pair_id"]
                maybe_stop("projection", shard)
                if store.valid_checkpoint(shard, "projection", fps["stage_fps"]["projection"],
                                          fps["shard_fps"][shard]) and \
                        os.path.exists(store.path("shards", shard, "projected.npz")):
                    continue
                digest = _run_projection(store, cfg, pair, projections)
                store.publish_checkpoint({
                    "shard": shard, "stage": "projection",
                    "stage_fp": fps["stage_fps"]["projection"],
                    "shard_fp": fps["shard_fps"][shard],
                    "artifact_digest": digest,
                })
                units_done.append(f"projection:{shard}")

        # ---- stage 3: index (global, per-shard checkpoints) ----
        if stale_idx is not None and stale_idx <= 2:
            maybe_stop("index", "__index__")
            index_obj, shard_digests = build_index(store, cfg, pairs, data_dir)
            for pair in pairs:
                shard = pair["pair_id"]
                store.publish_checkpoint({
                    "shard": shard, "stage": "index",
                    "stage_fp": fps["stage_fps"]["index"],
                    "shard_fp": fps["shard_fps"][shard],
                    "artifact_digest": shard_digests[shard],
                })
            store.publish_checkpoint({
                "shard": "__index__", "stage": "index",
                "stage_fp": fps["stage_fps"]["index"],
                "shard_fp": "__global__",
                "artifact_digest": index_obj["meta"]["index_digest"],
            })
            units_done.append("index:__index__")

        # ---- stage 4: recall (global, per-shard checkpoints) ----
        if stale_idx is not None and stale_idx <= 3:
            maybe_stop("recall", "__recall__")
            if projections is None:
                projections = _load_projections_checked(data_dir, cfg)
            report = _run_recall(store, cfg, pairs, projections, data_dir)
            for pair in pairs:
                shard = pair["pair_id"]
                store.publish_checkpoint({
                    "shard": shard, "stage": "recall",
                    "stage_fp": fps["stage_fps"]["recall"],
                    "shard_fp": fps["shard_fps"][shard],
                    "artifact_digest": digest_obj(report["pairs"][shard]),
                })
            store.publish_checkpoint({
                "shard": "__recall__", "stage": "recall",
                "stage_fp": fps["stage_fps"]["recall"],
                "shard_fp": "__global__",
                "artifact_digest": report["digest"],
            })
            units_done.append("recall:__recall__")
    except GracefulStop:
        return {
            "status": "stopped",
            "stopped": True,
            "units_done": units_done,
            "invalidation": report_inv,
            "stage_fps": fps["stage_fps"],
        }

    # publish fingerprints only after a complete pass
    atomic_write_json(store.path("published", "fingerprints.json"), {
        "input_fp": fps["input_fp"],
        "components": fps["components"],
        "stage_fps": fps["stage_fps"],
        "shard_fps": fps["shard_fps"],
    })
    index_obj = read_json(store.path("index", "index.json"), {})
    recall = read_json(store.path("recall", "recall.json"), {})
    return {
        "status": "ok" if stale_idx is not None else "up_to_date",
        "stopped": False,
        "units_done": units_done,
        "invalidation": report_inv,
        "stage_fps": fps["stage_fps"],
        "index_digest": index_obj.get("meta", {}).get("index_digest"),
        "recall_digest": recall.get("digest"),
        "recall_ok": recall.get("ok"),
    }


def _load_projections_checked(data_dir, cfg):
    from .projection import load_projections

    w_img, w_txt = load_projections(os.path.join(data_dir, "projection.json"), cfg,
                                    stage="projection")
    return {"image": w_img, "text": w_txt}


def ensure_fresh(data_dir, state_dir, cfg):
    """Guarantee the published index matches current inputs/config.

    Stale indexes are never used to answer queries: rebuild first.
    """
    store = StateStore(state_dir)
    fps = compute_fingerprints(data_dir, cfg)
    stale_idx, _ = plan_rebuild(store, fps)
    if stale_idx is None and os.path.exists(store.path("index", "index.json")):
        return {"status": "up_to_date", "stage_fps": fps["stage_fps"]}
    return run_build(data_dir, state_dir, cfg)
