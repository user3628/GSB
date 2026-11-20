"""Shared service layer used by both the CLI and the HTTP workbench."""

import json
import os

import numpy as np

from . import codecs
from .checkpoint import STAGE_INDEX, STAGE_ORDER, CheckpointStore
from .config import EngineConfig
from .errors import E_STATE, EngineError
from .importer import import_project
from .manifest import (dump_manifest, export_manifest_v2, load_manifest_doc)
from .pipeline import (EXPORT_FORMATS, SUMMARY_UNIT, Workspace, build_plan,
                       reset_workspace, run_pipeline,
                       workspace_output_digests)
from .stages import _load_artifact
from .util import canonical_json_bytes


def do_import(workspace, manifest_path, shards_dir, extra_shards_dir=None):
    project, import_meta = import_project(workspace, manifest_path,
                                          shards_dir, extra_shards_dir)
    return {
        "project_id": project.project_id,
        "structure_version": project.structure_version,
        "upgraded_from": import_meta["upgraded_from"],
        "modalities": project.modalities,
        "shards": {s.shard_id: {"modality": s.modality, "dim": s.dim,
                                "rows": len(s.ids)}
                   for s in project.shards},
        "structure_fingerprint": import_meta["structure_fingerprint"],
    }


def do_run(workspace, stage=None, stop_after_units=None):
    return run_pipeline(workspace, target_stage=stage,
                        stop_after_units=stop_after_units)


def do_stop(workspace):
    ws = Workspace(workspace)
    ws.request_stop()
    return {"stop_requested": True}


def do_reset(workspace):
    reset_workspace(workspace)
    return {"reset": True}


def _plan_and_state(workspace):
    ws = Workspace(workspace)
    project = ws.load_project()
    if project is None:
        raise EngineError(E_STATE, "project not imported",
                          {"workspace": ws.root})
    config = ws.load_config()
    units = build_plan(project, config)
    state = CheckpointStore(ws.root).load_verified()
    return ws, project, config, units, state


def do_status(workspace):
    ws, project, config, units, state = _plan_and_state(workspace)
    entries = []
    for unit in units:
        record = (state["units"].get(unit.unit, {})
                  .get("stages", {}).get(unit.stage))
        if record is None:
            status = "pending"
        elif record["fingerprint"] == unit.fingerprint:
            status = "valid"
        else:
            status = "stale"
        entries.append({
            "stage": unit.stage,
            "stage_index": STAGE_INDEX[unit.stage],
            "unit": unit.unit,
            "status": status,
            "fingerprint": unit.fingerprint,
            "seq": record["seq"] if record else None,
            "artifact_digest": record["artifact_digest"] if record else None,
            "artifact_summary": (record.get("artifact_summary")
                                 if record else None),
        })
    report_path = ws.path("run_report.json")
    last_run = None
    if os.path.exists(report_path):
        with open(report_path, "r", encoding="utf-8") as handle:
            last_run = json.load(handle)
    return {
        "project_id": project.project_id,
        "structure_version": project.structure_version,
        "config_version": config.config_version(),
        "stages": STAGE_ORDER,
        "units": entries,
        "last_run": last_run,
        "output_digests": _safe_output_digests(ws),
    }


def _safe_output_digests(ws):
    from .pipeline import workspace_output_digests
    try:
        return workspace_output_digests(ws)
    except Exception:
        return {}


def do_fingerprints(workspace):
    ws, project, config, units, state = _plan_and_state(workspace)
    from .importer import (dims_fingerprint, projection_fingerprint,
                           structure_fingerprint)
    return {
        "structure_fingerprint": structure_fingerprint(project),
        "dims_fingerprint": dims_fingerprint(project),
        "projection_fingerprint": projection_fingerprint(project),
        "concat_fingerprint": config.concat_fp(),
        "quant_fingerprint": config.quant_fp(),
        "config_version": config.config_version(),
        "unit_fingerprints": {
            "{}:{}".format(u.stage, u.unit): u.fingerprint for u in units},
    }


def do_summary(workspace):
    ws = Workspace(workspace)
    return _load_artifact(ws.root, "concat", SUMMARY_UNIT)


def do_uncovered(workspace):
    summary = do_summary(workspace)
    return {
        "uncovered": summary["coverage"]["uncovered"],
        "degraded": summary["coverage"]["degraded"],
        "covered_text_ids": summary["coverage"]["covered_text_ids"],
        "totals": summary["totals"],
    }


def do_checkpoints(workspace):
    ws = Workspace(workspace)
    state = CheckpointStore(ws.root).load_verified()
    records = []
    for unit, unit_state in sorted(state["units"].items()):
        for stage, record in sorted(
                unit_state["stages"].items(),
                key=lambda kv: kv[1]["seq"]):
            records.append(record)
    return {"records": records}


def do_invalidations(workspace):
    ws = Workspace(workspace)
    return {"invalidations": CheckpointStore(ws.root).invalidations()}


def do_convert(src, src_format, dst, dst_format, verify=False):
    project = codecs.read_project(src_format, src)
    codecs.write_project(dst_format, project, dst)
    result = {
        "src": src, "src_format": src_format,
        "dst": dst, "dst_format": dst_format,
        "project_fingerprint": project.fingerprint(),
    }
    if verify:
        reloaded = codecs.read_project(dst_format, dst)
        result["roundtrip_equal"] = (
            reloaded.canonical_bytes() == project.canonical_bytes())
    return result


def do_verify_roundtrip(src, src_format, work_dir):
    """src -> IR -> all three formats -> IR; compare canonical bytes."""
    project = codecs.read_project(src_format, src)
    reference = project.canonical_bytes()
    os.makedirs(work_dir, exist_ok=True)
    results = {}
    for fmt in EXPORT_FORMATS:
        if fmt == "json_project":
            dst = os.path.join(work_dir, "roundtrip.json")
        elif fmt == "npz_pack":
            dst = os.path.join(work_dir, "roundtrip.npz")
        else:
            dst = os.path.join(work_dir, "roundtrip_npydir")
        codecs.write_project(fmt, project, dst)
        reloaded = codecs.read_project(fmt, dst)
        results[fmt] = {
            "equal": reloaded.canonical_bytes() == reference,
            "fingerprint": reloaded.fingerprint(),
        }
    return {"source_fingerprint": project.fingerprint(),
            "formats": results,
            "all_equal": all(r["equal"] for r in results.values())}


def equivalence_report(project_a, project_b):
    """Byte-level equivalence: vector matrices, dims, scale factors."""
    checks = {}
    checks["structure_version"] = (
        project_a.structure_version == project_b.structure_version)
    checks["modalities"] = project_a.modalities == project_b.modalities
    proj_a = project_a.projection or {}
    proj_b = project_b.projection or {}
    checks["scale"] = proj_a.get("scale") == proj_b.get("scale")
    matrix_a = np.asarray(proj_a.get("matrix", []), dtype=np.float64)
    matrix_b = np.asarray(proj_b.get("matrix", []), dtype=np.float64)
    checks["projection_matrix_bytes"] = (
        matrix_a.shape == matrix_b.shape
        and matrix_a.tobytes() == matrix_b.tobytes())
    shards_a = {s.shard_id: s for s in project_a.shards}
    shards_b = {s.shard_id: s for s in project_b.shards}
    checks["shard_ids"] = sorted(shards_a) == sorted(shards_b)
    shard_vectors_ok = True
    shard_scales_ok = True
    shard_dims_ok = True
    for shard_id in shards_a:
        if shard_id not in shards_b:
            shard_vectors_ok = False
            continue
        sa, sb = shards_a[shard_id], shards_b[shard_id]
        shard_dims_ok = shard_dims_ok and sa.dim == sb.dim
        shard_scales_ok = shard_scales_ok and sa.scales == sb.scales
        shard_vectors_ok = shard_vectors_ok and (
            sa.vectors_q.shape == sb.vectors_q.shape
            and sa.vectors_q.tobytes() == sb.vectors_q.tobytes())
    checks["shard_dims"] = shard_dims_ok
    checks["shard_scales_bytes"] = shard_scales_ok
    checks["shard_vectors_bytes"] = shard_vectors_ok
    return {"equal": all(checks.values()), "checks": checks}


def do_upgrade(v1_path, v2_path, shards_dir, work_dir, out_path=None):
    """Import v1 with explicit upgrade; verify against a direct v2 import."""
    ws_v1 = os.path.join(work_dir, "ws_v1_upgraded")
    ws_v2 = os.path.join(work_dir, "ws_v2_direct")
    os.makedirs(ws_v1, exist_ok=True)
    os.makedirs(ws_v2, exist_ok=True)
    project_v1, meta_v1 = import_project(ws_v1, v1_path, shards_dir)
    project_v2, meta_v2 = import_project(ws_v2, v2_path, shards_dir)
    report = equivalence_report(project_v1, project_v2)
    reexported_doc = export_manifest_v2(project_v1)
    v2_doc = load_manifest_doc(v2_path)
    matrix_ok = (np.asarray(reexported_doc["projection"]["matrix"]).tobytes()
                 == np.asarray(v2_doc["projection"]["matrix"]).tobytes())
    report["reexport_v2_manifest"] = {
        "scale_equal": (reexported_doc["projection"]["scale"]
                        == v2_doc["projection"]["scale"]),
        "dims_equal": reexported_doc["modalities"] == v2_doc["modalities"],
        "matrix_bytes_equal": bool(matrix_ok),
    }
    report["reexport_v2_manifest"]["equal"] = all(
        report["reexport_v2_manifest"].values())
    if out_path:
        with open(out_path, "w", encoding="utf-8") as handle:
            handle.write(dump_manifest(reexported_doc))
        report["reexported_manifest_path"] = out_path
    report["upgrade_notes"] = project_v1.meta.get("upgrade")
    report["equal"] = report["equal"] and matrix_ok
    return report


def do_export(workspace):
    """Run the export stage (prerequisites must be complete)."""
    report = run_pipeline(workspace, target_stage="export")
    return {"run": report, "output_digests": workspace_output_digests(
        Workspace(workspace))}


def do_config_show(workspace):
    return Workspace(workspace).load_config().to_dict()


def do_config_set(workspace, key_path, value):
    ws = Workspace(workspace)
    config = ws.load_config()
    data = config.to_dict()
    parts = key_path.split(".")
    node = data
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    node[parts[-1]] = value
    path = ws.path("config.json")
    with open(path, "wb") as handle:
        handle.write(canonical_json_bytes(data))
    return {"config": data, "config_version": EngineConfig(data)
            .config_version()}
