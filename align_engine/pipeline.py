"""Pipeline orchestration: staged execution with checkpoints, invalidation
propagation, graceful stop and byte-identical resume.

Stages: 1 validate -> 2 project -> 3 concat(+summary) -> 4 export.
Units: shards for stages 1-2, text shards + ``__summary__`` for stage 3 and
``__export_<fmt>__`` for stage 4.  Each unit's stage fingerprint is derived
from its input content fingerprints, the structure fingerprint and the
relevant config fingerprints, so a change in any input invalidates exactly
the earliest affected stage and everything downstream.
"""

import json
import os
import shutil
from dataclasses import dataclass, field

from . import stages as stage_exec
from .checkpoint import STAGE_INDEX, STAGE_ORDER, CheckpointStore
from .config import EngineConfig
from .errors import E_INVALID_STAGE, E_STATE, EngineError
from .importer import (dims_fingerprint, projection_fingerprint,
                       structure_fingerprint)
from .ir import Project
from .util import (atomic_write, canonical_json_bytes, fingerprint_obj,
                   sha256_hex)

SUMMARY_UNIT = "__summary__"
EXPORT_FORMATS = ["json_project", "npz_pack", "npy_dir"]
STOP_FILENAME = "STOP_REQUESTED"


def export_unit(fmt):
    return "__export_{}__".format(fmt)


@dataclass
class PlanUnit:
    stage: str
    unit: str
    fingerprint: str
    input_fingerprint: str
    deps: list = field(default_factory=list)

    @property
    def key(self):
        return (self.stage, self.unit)


class Workspace:
    def __init__(self, root):
        self.root = os.path.abspath(root)

    def path(self, *parts):
        return os.path.join(self.root, *parts)

    @property
    def project_path(self):
        return self.path("project", "project.json")

    @property
    def stop_path(self):
        return self.path(STOP_FILENAME)

    def load_project(self):
        if not os.path.exists(self.project_path):
            return None
        with open(self.project_path, "r", encoding="utf-8") as handle:
            return Project.from_dict(json.load(handle))

    def load_config(self):
        path = self.path("config.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as handle:
                return EngineConfig(json.load(handle))
        return EngineConfig()

    def stop_requested(self):
        return os.path.exists(self.stop_path)

    def request_stop(self):
        atomic_write(self.stop_path, b"stop\n")

    def clear_stop(self):
        if os.path.exists(self.stop_path):
            os.unlink(self.stop_path)


def build_plan(project, config):
    """Compute deterministic stage fingerprints for every unit."""
    dims_fp = dims_fingerprint(project)
    proj_fp = projection_fingerprint(project)
    concat_fp = config.concat_fp()
    quant_fp = config.quant_fp()
    shard_fp = {s.shard_id: s.fingerprint() for s in project.shards}
    text_ids = project.shard_ids("text")
    vision_ids = project.shard_ids("vision")

    units = []
    fps = {}

    def add(stage, unit, fingerprint, input_fp, deps):
        fps[(stage, unit)] = fingerprint
        units.append(PlanUnit(stage, unit, fingerprint, input_fp, deps))

    for sid in text_ids + vision_ids:
        fp = fingerprint_obj("v1|validate", shard_fp[sid], dims_fp)
        add("validate", sid, fp, shard_fp[sid], [])
    for sid in text_ids:
        fp = fingerprint_obj("v1|project", fps[("validate", sid)], proj_fp)
        add("project", sid, fp, shard_fp[sid], [("validate", sid)])
    for sid in vision_ids:
        deps = [("validate", sid)] + [("project", t) for t in text_ids]
        fp = fingerprint_obj("v1|project", fps[("validate", sid)], proj_fp,
                             *[fps[("project", t)] for t in text_ids])
        add("project", sid, fp, shard_fp[sid], deps)
    for sid in text_ids:
        deps = [("project", sid)] + [("project", v) for v in vision_ids]
        fp = fingerprint_obj("v1|concat", fps[("project", sid)],
                             *[fps[("project", v)] for v in vision_ids],
                             concat_fp)
        add("concat", sid, fp, shard_fp[sid], deps)
    summary_deps = [("concat", t) for t in text_ids]
    summary_fp = fingerprint_obj(
        "v1|summary", *[fps[("concat", t)] for t in text_ids])
    add("concat", SUMMARY_UNIT, summary_fp, summary_fp, summary_deps)
    for fmt in EXPORT_FORMATS:
        fp = fingerprint_obj("v1|export", fmt, summary_fp, quant_fp)
        add("export", export_unit(fmt), fp, summary_fp,
            [("concat", SUMMARY_UNIT)])
    return units


def _dependents_map(units):
    dependents = {}
    for unit in units:
        for dep in unit.deps:
            dependents.setdefault(dep, set()).add(unit.key)
    return dependents


def _transitive_dependents(units, key):
    dependents = _dependents_map(units)
    seen = set()
    stack = [key]
    while stack:
        current = stack.pop()
        for nxt in dependents.get(current, ()):  # downstream units
            if nxt not in seen:
                seen.add(nxt)
                stack.append(nxt)
    return seen


def run_pipeline(workspace, target_stage=None, stop_after_units=None,
                 clear_stop=True):
    """Run (or resume) the alignment pipeline.

    ``target_stage`` restricts execution to a single stage; prerequisites
    must already be complete, otherwise ``E_INVALID_STAGE`` is raised.
    ``stop_after_units`` is a deterministic test/demo hook that requests a
    graceful stop after N executed units.
    """
    ws = workspace if isinstance(workspace, Workspace) else Workspace(workspace)
    project = ws.load_project()
    if project is None:
        raise EngineError(E_STATE, "project not imported",
                          {"workspace": ws.root})
    if project.projection is None:
        raise EngineError(E_STATE, "project has no projection", {})
    config = ws.load_config()
    if target_stage is not None and target_stage not in STAGE_ORDER:
        raise EngineError(E_INVALID_STAGE, "unknown stage",
                          {"stage": target_stage,
                           "known_stages": STAGE_ORDER})
    if clear_stop:
        ws.clear_stop()
    store = CheckpointStore(ws.root)
    state = store.load_verified()
    units = build_plan(project, config)

    if target_stage is not None:
        target_index = STAGE_INDEX[target_stage]
        missing = [
            {"stage": u.stage, "unit": u.unit}
            for u in units
            if STAGE_INDEX[u.stage] < target_index
            and not _record_matches(state, u)
        ]
        if missing:
            raise EngineError(
                E_INVALID_STAGE,
                "missing prerequisite stage artifacts",
                {"target_stage": target_stage, "missing": missing})

    selected = [u for u in units
                if target_stage is None or u.stage == target_stage]
    events = []
    executed = 0
    for unit in selected:
        if ws.stop_requested():
            return _report(ws, "stopped", events, selected)
        record = _get_record(state, unit)
        if record and record["fingerprint"] == unit.fingerprint:
            events.append({"event": "reuse", "stage": unit.stage,
                           "unit": unit.unit,
                           "fingerprint": unit.fingerprint})
            continue
        if record:
            affected = {(unit.stage, unit.unit)}
            affected |= _transitive_dependents(units, unit.key)
            entries = sorted(
                (key for key in affected
                 if _get_record_by_key(state, key) is not None),
                key=lambda k: (STAGE_INDEX[k[0]], k[1]))
            store.invalidate(
                [[unit_id, stage] for stage, unit_id in entries],
                reason="fingerprint_mismatch",
                old_fingerprint=record["fingerprint"],
                new_fingerprint=unit.fingerprint)
            events.append({
                "event": "invalidate",
                "stage": unit.stage,
                "unit": unit.unit,
                "reason": "fingerprint_mismatch",
                "old_fingerprint": record["fingerprint"],
                "new_fingerprint": unit.fingerprint,
                "dropped": [{"stage": s, "unit": u} for s, u in entries],
            })
            state = store.load_verified()
        artifact_rel, digest, brief = _execute_unit(ws, project, config,
                                                    unit, units)
        record = {
            "unit": unit.unit,
            "stage": unit.stage,
            "stage_index": STAGE_INDEX[unit.stage],
            "fingerprint": unit.fingerprint,
            "input_fingerprint": unit.input_fingerprint,
            "structure_fingerprint": structure_fingerprint(project),
            "config_version": config.config_version(),
            "artifact_digest": digest,
            "artifact_path": artifact_rel,
            "artifact_summary": brief,
        }
        store.publish(record)
        state = store.load_verified()
        events.append({"event": "rebuild", "stage": unit.stage,
                       "unit": unit.unit, "fingerprint": unit.fingerprint,
                       "artifact_digest": digest})
        executed += 1
        if stop_after_units is not None and executed >= stop_after_units:
            ws.request_stop()
    status = "stopped" if ws.stop_requested() else "completed"
    return _report(ws, status, events, selected)


def _record_matches(state, unit):
    record = _get_record(state, unit)
    return record is not None and record["fingerprint"] == unit.fingerprint


def _get_record(state, unit):
    return _get_record_by_key(state, (unit.stage, unit.unit))


def _get_record_by_key(state, key):
    stage, unit_id = key
    return state["units"].get(unit_id, {}).get("stages", {}).get(stage)


def _execute_unit(ws, project, config, unit, units):
    stage_index = STAGE_INDEX[unit.stage]
    if unit.stage == "validate":
        payload = stage_exec.execute_validate(project, unit.unit)
    elif unit.stage == "project":
        payload = stage_exec.execute_project(ws.root, project, unit.unit)
    elif unit.stage == "concat" and unit.unit == SUMMARY_UNIT:
        fp_map = {"{}:{}".format(u.stage, u.unit): u.fingerprint
                  for u in units}
        payload = stage_exec.execute_summary(ws.root, project, config, fp_map)
    elif unit.stage == "concat":
        payload = stage_exec.execute_concat(ws.root, project, config,
                                            unit.unit)
    elif unit.stage == "export":
        fmt = unit.unit[len("__export_"):-2]
        summary = stage_exec._load_artifact(ws.root, "concat", SUMMARY_UNIT)
        payload = stage_exec.execute_export(ws.root, project, config, fmt,
                                            summary)
    else:  # pragma: no cover - defensive
        raise EngineError(E_INVALID_STAGE, "unknown stage",
                          {"stage": unit.stage})
    rel, digest = stage_exec.write_artifact(
        ws.root, stage_index, unit.stage, unit.unit, payload)
    brief = _brief(unit, payload)
    return rel, digest, brief


def _brief(unit, payload):
    if unit.stage == "validate":
        return {"declared_dim": payload["declared_dim"],
                "actual_dim": payload["actual_dim"],
                "rows": payload["rows"]}
    if unit.stage == "project":
        return {"modality": payload["modality"], "rows": len(payload["ids"])}
    if unit.stage == "concat" and unit.unit == SUMMARY_UNIT:
        return {"digest": payload["digest"], "totals": payload["totals"]}
    if unit.stage == "concat":
        return dict(payload["fragment"])
    if unit.stage == "export":
        return {"format": payload["format"],
                "package_digest": payload["package_digest"]}
    return {}


def _report(ws, status, events, selected):
    done = sorted({"{}:{}".format(e["stage"], e["unit"]) for e in events})
    report = {
        "status": status,
        "workspace": ws.root,
        "events": events,
        "units_touched": done,
        "units_total": len(selected),
    }
    atomic_write(ws.path("run_report.json"), canonical_json_bytes(report))
    return report


def reset_workspace(workspace):
    ws = workspace if isinstance(workspace, Workspace) else Workspace(workspace)
    CheckpointStore(ws.root).reset()
    for name in ("artifacts", "exports"):
        path = ws.path(name)
        if os.path.isdir(path):
            shutil.rmtree(path)
    ws.clear_stop()


def workspace_output_digests(workspace):
    """Digests of final outputs (summary + exports) for byte-identity checks."""
    ws = workspace if isinstance(workspace, Workspace) else Workspace(workspace)
    outputs = {}
    summary_rel = stage_exec.artifact_relpath(
        STAGE_INDEX["concat"], "concat", SUMMARY_UNIT)
    summary_path = ws.path(summary_rel)
    if os.path.exists(summary_path):
        with open(summary_path, "rb") as handle:
            outputs["summary"] = sha256_hex(handle.read())
    exports_dir = ws.path("exports")
    if os.path.isdir(exports_dir):
        for root, _dirs, names in os.walk(exports_dir):
            for name in sorted(names):
                full = os.path.join(root, name)
                rel = os.path.relpath(full, exports_dir)
                with open(full, "rb") as handle:
                    outputs["exports/" + rel] = sha256_hex(handle.read())
    return outputs
