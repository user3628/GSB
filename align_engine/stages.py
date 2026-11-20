"""Stage executors: validate -> project -> concat/summary -> export.

All computation happens in the float domain, is fully deterministic and
produces canonical-JSON artifacts so identical inputs + config always yield
byte-identical outputs (including the cosine similarity matrix).
"""

import json
import os

import numpy as np

from . import codecs
from .errors import E_DIM_MISMATCH, E_INVALID_STAGE, EngineError
from .ir import Project, Shard
from .util import canonical_json_bytes, sha256_file, sha256_hex


def _dim_mismatch(shard_id, declared, actual, **extra):
    details = {"shard_id": shard_id, "declared_dim": declared,
               "actual_dim": actual}
    details.update(extra)
    return EngineError(E_DIM_MISMATCH,
                       "dimension mismatch in shard {}".format(shard_id),
                       details)


def _normalize_rows(matrix):
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    out = np.zeros_like(matrix)
    np.divide(matrix, norms, out=out, where=norms > 0)
    return out


# ------------------------------------------------------------ stage 1: validate
def execute_validate(project, shard_id):
    shard = project.shard(shard_id)
    if shard.modality not in project.modalities:
        raise EngineError(E_INVALID_STAGE, "unknown modality for shard",
                          {"shard_id": shard_id, "modality": shard.modality})
    declared = int(project.modalities[shard.modality]["dim"])
    actual = int(shard.vectors_q.shape[1])
    if actual != shard.dim or actual != declared:
        raise _dim_mismatch(shard_id, declared, actual,
                            modality=shard.modality)
    if project.projection is not None:
        text_dim = int(project.modalities["text"]["dim"])
        vision_dim = int(project.modalities["vision"]["dim"])
        shape = list(project.projection["shape"])
        if shape != [text_dim, vision_dim]:
            raise _dim_mismatch(shard_id, [text_dim, vision_dim], shape,
                                modality=shard.modality,
                                where="projection_matrix")
    return {
        "shard_id": shard_id,
        "modality": shard.modality,
        "declared_dim": declared,
        "actual_dim": actual,
        "rows": len(shard.ids),
        "ok": True,
    }


# ------------------------------------------------------------- stage 2: project
def execute_project(workspace, project, shard_id):
    shard = project.shard(shard_id)
    vectors = shard.dequantize()
    if shard.modality == "text":
        normalized = _normalize_rows(vectors)
        return {
            "shard_id": shard_id,
            "modality": "text",
            "ids": list(shard.ids),
            "normalized": [[float(x) for x in row] for row in normalized],
        }
    if shard.modality == "vision":
        scale = float(project.projection["scale"])
        matrix = np.asarray(project.projection["matrix"], dtype=np.float64)
        mapped = _normalize_rows((vectors * scale) @ matrix.T)
        text_matrix, text_ids_order = _load_all_text_normalized(
            workspace, project)
        sim = mapped @ text_matrix.T
        return {
            "shard_id": shard_id,
            "modality": "vision",
            "ids": list(shard.ids),
            "row_text_ids": list(shard.meta.get("text_ids", [])),
            "operators": list(shard.meta.get("operators", [])),
            "mapped": [[float(x) for x in row] for row in mapped],
            "text_ids_order": text_ids_order,
            "sim_matrix": [[float(x) for x in row] for row in sim],
        }
    raise EngineError(E_INVALID_STAGE, "cannot project modality",
                      {"shard_id": shard_id, "modality": shard.modality})


def _load_all_text_normalized(workspace, project):
    rows = []
    ids = []
    for text_shard_id in project.shard_ids("text"):
        artifact = _load_artifact(workspace, "project", text_shard_id)
        for text_id, row in zip(artifact["ids"], artifact["normalized"]):
            ids.append(text_id)
            rows.append(row)
    order = sorted(range(len(ids)), key=lambda i: ids[i])
    matrix = np.asarray([rows[i] for i in order], dtype=np.float64)
    return matrix, [ids[i] for i in order]


# -------------------------------------------------------------- stage 3: concat
def execute_concat(workspace, project, config, text_shard_id):
    text_artifact = _load_artifact(workspace, "project", text_shard_id)
    vision_rows = []
    for vision_shard_id in project.shard_ids("vision"):
        artifact = _load_artifact(workspace, "project", vision_shard_id)
        text_pos = {tid: i for i, tid in enumerate(artifact["text_ids_order"])}
        for row_idx, vision_id in enumerate(artifact["ids"]):
            row_text_id = artifact["row_text_ids"][row_idx]
            vision_rows.append({
                "vision_shard_id": vision_shard_id,
                "vision_id": vision_id,
                "text_id": row_text_id,
                "operator": artifact["operators"][row_idx],
                "mapped": artifact["mapped"][row_idx],
                "cosine": artifact["sim_matrix"][row_idx][text_pos[row_text_id]],
            })
    vision_rows.sort(key=lambda r: (r["vision_shard_id"], r["vision_id"]))

    rule = config.concat_rule
    weights = config.operator_weights
    samples = []
    uncovered = []
    for row_idx, text_id in enumerate(text_artifact["ids"]):
        matches = [r for r in vision_rows if r["text_id"] == text_id]
        if not matches:
            uncovered.append({"text_id": text_id,
                              "reason": "no_vision_match"})
            continue
        text_vec = np.asarray(text_artifact["normalized"][row_idx],
                              dtype=np.float64)
        for match in matches:
            operator = match["operator"]
            status = "aligned"
            reason = None
            if operator is None:
                status, reason = "degraded", "missing_operator_label"
                weight, confidence = None, None
            elif operator in weights:
                weight = float(weights[operator])
                confidence = float(match["cosine"]) * weight
            elif "default" in weights:
                weight = float(weights["default"])
                confidence = float(match["cosine"]) * weight
            else:
                status, reason = "degraded", "unknown_operator"
                weight, confidence = None, None
            mapped_vec = np.asarray(match["mapped"], dtype=np.float64)
            aligned = _concat_vectors(text_vec, mapped_vec, rule)
            sample = {
                "text_id": text_id,
                "vision_id": match["vision_id"],
                "vision_shard_id": match["vision_shard_id"],
                "operator": operator,
                "status": status,
                "cosine_similarity": float(match["cosine"]),
                "operator_weight": weight,
                "confidence": confidence,
                "aligned_vector": [float(x) for x in aligned],
            }
            if reason:
                sample["reason"] = reason
            samples.append(sample)
    samples.sort(key=lambda s: (s["text_id"], s["vision_id"]))
    return {
        "shard_id": text_shard_id,
        "samples": samples,
        "uncovered": uncovered,
        "fragment": {
            "aligned": sum(1 for s in samples if s["status"] == "aligned"),
            "degraded": sum(1 for s in samples if s["status"] == "degraded"),
            "uncovered": len(uncovered),
        },
    }


def _concat_vectors(text_vec, mapped_vec, rule):
    text_part = text_vec * float(rule.get("text_gain", 1.0))
    vision_part = mapped_vec * float(rule.get("vision_gain", 1.0))
    if rule.get("order", "text_then_vision") == "vision_then_text":
        return np.concatenate([vision_part, text_part])
    return np.concatenate([text_part, vision_part])


def execute_summary(workspace, project, config, stage_fingerprints):
    text_dim = int(project.modalities["text"]["dim"])
    vision_dim = int(project.modalities["vision"]["dim"])
    samples = []
    uncovered = []
    for text_shard_id in project.shard_ids("text"):
        artifact = _load_artifact(workspace, "concat", text_shard_id)
        for sample in artifact["samples"]:
            entry = {k: v for k, v in sample.items()
                     if k != "aligned_vector"}
            entry["text_shard_id"] = text_shard_id
            samples.append(entry)
        uncovered.extend(artifact["uncovered"])
    samples.sort(key=lambda s: (s["text_shard_id"], s["text_id"],
                                s["vision_id"]))
    uncovered.sort(key=lambda u: u["text_id"])
    degraded = [{"text_id": s["text_id"], "vision_id": s["vision_id"],
                 "reason": s.get("reason", "degraded")}
                for s in samples if s["status"] == "degraded"]
    covered_text_ids = sorted({s["text_id"] for s in samples})
    vision_sample_count = sum(
        len(project.shard(sid).ids) for sid in project.shard_ids("vision"))
    text_sample_count = sum(
        len(project.shard(sid).ids) for sid in project.shard_ids("text"))
    summary = {
        "project_id": project.project_id,
        "structure_version": project.structure_version,
        "config_version": config.config_version(),
        "dimensions": {"text": text_dim, "vision": vision_dim,
                       "aligned": 2 * text_dim},
        "totals": {
            "text_samples": text_sample_count,
            "vision_samples": vision_sample_count,
            "aligned_samples": sum(1 for s in samples
                                   if s["status"] == "aligned"),
            "degraded_samples": len(degraded),
            "uncovered_text_ids": len(uncovered),
        },
        "modality_coverage": {
            "text": {"shards": len(project.shard_ids("text")),
                     "samples": text_sample_count},
            "vision": {"shards": len(project.shard_ids("vision")),
                       "samples": vision_sample_count},
            "aligned": {"shards": len(project.shard_ids("text")),
                        "samples": len(samples)},
        },
        "coverage": {
            "covered_text_ids": covered_text_ids,
            "uncovered": uncovered,
            "degraded": degraded,
        },
        "samples": samples,
        "stage_fingerprints": stage_fingerprints,
    }
    summary["digest"] = sha256_hex(canonical_json_bytes(summary))
    return summary


# -------------------------------------------------------------- stage 4: export
def execute_export(workspace, project, config, fmt, summary):
    aligned_project = build_aligned_project(workspace, project, config, summary)
    out_path = export_path(workspace, fmt)
    codecs.write_project(fmt, aligned_project, out_path)
    files = _package_files(out_path)
    package_digest = sha256_hex(canonical_json_bytes(files))
    return {
        "format": fmt,
        "path": os.path.relpath(out_path, workspace),
        "package_digest": package_digest,
        "files": files,
        "aligned_shards": [s.shard_id for s in aligned_project.shards],
        "aligned_dim": 2 * int(project.modalities["text"]["dim"]),
    }


def build_aligned_project(workspace, project, config, summary):
    text_dim = int(project.modalities["text"]["dim"])
    shards = []
    for text_shard_id in project.shard_ids("text"):
        artifact = _load_artifact(workspace, "concat", text_shard_id)
        usable = [s for s in artifact["samples"] if "aligned_vector" in s]
        ids = ["{}::{}".format(s["text_id"], s["vision_id"]) for s in usable]
        vectors = [s["aligned_vector"] for s in usable]
        meta = {
            "source_text_shard": text_shard_id,
            "text_ids": [s["text_id"] for s in usable],
            "vision_ids": [s["vision_id"] for s in usable],
            "operators": [s["operator"] for s in usable],
            "statuses": [s["status"] for s in usable],
        }
        matrix = (np.asarray(vectors, dtype=np.float64)
                  if vectors else np.zeros((0, 2 * text_dim)))
        shards.append(Shard.from_vectors(
            "aligned-{}".format(text_shard_id), "aligned", ids, matrix,
            config.bits, meta, structure_version=project.structure_version))
    return Project(
        project_id="{}-aligned".format(project.project_id),
        structure_version=project.structure_version,
        modalities={"aligned": {"dim": 2 * text_dim}},
        projection=None,
        shards=shards,
        meta={
            "kind": "aligned",
            "source_project_id": project.project_id,
            "summary_digest": summary["digest"],
            "config_version": config.config_version(),
        })


def export_path(workspace, fmt):
    base = os.path.join(workspace, "exports")
    if fmt == "json_project":
        return os.path.join(base, "json_project", "aligned_project.json")
    if fmt == "npz_pack":
        return os.path.join(base, "npz_pack", "aligned_project.npz")
    if fmt == "npy_dir":
        return os.path.join(base, "npy_dir")
    raise EngineError(E_INVALID_STAGE, "unknown export format",
                      {"format": fmt})


def _package_files(out_path):
    files = []
    if os.path.isfile(out_path):
        files.append({"path": os.path.basename(out_path),
                      "sha256": sha256_file(out_path)})
        return files
    for root, _dirs, names in os.walk(out_path):
        for name in sorted(names):
            full = os.path.join(root, name)
            rel = os.path.relpath(full, out_path)
            files.append({"path": rel, "sha256": sha256_file(full)})
    files.sort(key=lambda f: f["path"])
    return files


# ------------------------------------------------------------------- artifacts
def artifact_relpath(stage_index, stage, unit):
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in unit)
    return os.path.join("artifacts", "s{}_{}".format(stage_index, stage),
                        "{}.json".format(safe))


def write_artifact(workspace, stage_index, stage, unit, payload):
    from .util import atomic_write
    rel = artifact_relpath(stage_index, stage, unit)
    data = canonical_json_bytes(payload)
    atomic_write(os.path.join(workspace, rel), data)
    return rel, sha256_hex(data)


def _load_artifact(workspace, stage, unit):
    from .checkpoint import STAGE_INDEX
    rel = artifact_relpath(STAGE_INDEX[stage], stage, unit)
    path = os.path.join(workspace, rel)
    if not os.path.exists(path):
        raise EngineError(
            E_INVALID_STAGE, "missing prerequisite stage artifact",
            {"stage": stage, "unit": unit, "artifact_path": rel})
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


