"""Fault-injection harness with reproducible anomaly records.

Each injection:
  1. ensures a clean published build exists,
  2. snapshots every file it may touch (data files + whole state tree),
  3. applies the perturbation and runs the affected stage/query,
  4. captures the stable business error,
  5. restores the snapshot so the published legal index is never polluted,
  6. writes a deterministic anomaly record (no timestamps / pids).
Repeating the same injection yields a byte-identical record.
"""

import json
import os

from .canon import digest_obj
from .checkpoint import StateStore, atomic_write_json
from .errors import CmrError
from .indexer import load_index
from .pipeline import _load_projections_checked, ensure_fresh, run_build
from .retrieval import query_image_vector, query_text

INJECTION_KINDS = [
    "dim_mismatch",
    "nan_projection",
    "missing_fragment",
    "empty_query",
    "k_out_of_range",
    "bad_image_vector",
]

INJECT_IDS = {kind: f"INJ-{kind.upper().replace('_', '-')}-0001" for kind in INJECTION_KINDS}


class _Snapshot:
    def __init__(self, data_dir, state_dir):
        self.data_dir = data_dir
        self.state_dir = state_dir
        self.saved = {}

    def save_file(self, path):
        key = os.path.relpath(path, self.data_dir) if path.startswith(self.data_dir) \
            else os.path.join("__state__", os.path.relpath(path, self.state_dir))
        if os.path.exists(path):
            self.saved[key] = (path, open(path, "rb").read())
        else:
            self.saved[key] = (path, None)

    def save_tree(self, root):
        for dirpath, _dirs, files in os.walk(root):
            for name in files:
                self.save_file(os.path.join(dirpath, name))

    def restore(self):
        # remove files that appeared during the injection
        for dirpath, _dirs, files in os.walk(self.state_dir):
            for name in files:
                p = os.path.join(dirpath, name)
                rel = os.path.join("__state__", os.path.relpath(p, self.state_dir))
                if rel not in self.saved and "injections" not in os.path.relpath(p, self.state_dir).split(os.sep):
                    os.remove(p)
        for path, data in self.saved.values():
            if data is None:
                if os.path.exists(path):
                    os.remove(path)
            else:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as fh:
                    fh.write(data)


def _record_path(store, inject_id):
    return store.path("injections", f"{inject_id}.json")


def run_injection(kind, data_dir, state_dir, cfg):
    """Execute one injection; returns the anomaly record dict."""
    if kind not in INJECTION_KINDS:
        raise CmrError("E_INJECTION_UNKNOWN", f"unknown injection '{kind}'",
                       detail={"known": INJECTION_KINDS})
    cfg = cfg
    ensure_fresh(data_dir, state_dir, cfg)  # clean legal baseline

    store = StateStore(state_dir)
    inject_id = INJECT_IDS[kind]
    snap = _Snapshot(data_dir, state_dir)
    snap.save_tree(state_dir)  # full state snapshot (restored afterwards)
    for name in ("pairs.jsonl", "projection.json", "config.json"):
        snap.save_file(os.path.join(data_dir, name))
    frag_dir = os.path.join(data_dir, "fragments")
    if os.path.isdir(frag_dir):
        snap.save_tree(frag_dir)

    error_payload = None
    try:
        _apply_and_run(kind, data_dir, state_dir, cfg)
    except CmrError as exc:
        error_payload = exc.to_dict()["error"]
    finally:
        snap.restore()

    record = {
        "inject_id": inject_id,
        "kind": kind,
        "params": _injection_params(kind),
        "result": error_payload,
    }
    record["record_digest"] = digest_obj(record)
    atomic_write_json(_record_path(store, inject_id), record)
    return record


def _injection_params(kind):
    return {
        "dim_mismatch": {"declared_image_dim_delta": 8},
        "nan_projection": {"matrix": "image", "row": 0, "col": 0, "value": "NaN"},
        "missing_fragment": {"target": "first pair image_file"},
        "empty_query": {"text": ""},
        "k_out_of_range": {"k_offset": 1},
        "bad_image_vector": {"vector_dim_delta": -1},
    }[kind]


def _apply_and_run(kind, data_dir, state_dir, cfg):
    if kind == "dim_mismatch":
        manifest = os.path.join(data_dir, "pairs.jsonl")
        lines = open(manifest, encoding="utf-8").read().splitlines()
        meta = json.loads(lines[0])
        meta["declared_image_dim"] = meta["declared_image_dim"] + 8
        lines[0] = json.dumps(meta, ensure_ascii=False, sort_keys=True)
        with open(manifest, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        run_build(data_dir, state_dir, cfg)

    elif kind == "nan_projection":
        proj_path = os.path.join(data_dir, "projection.json")
        obj = json.load(open(proj_path, encoding="utf-8"))
        obj["image"]["data"][0][0] = float("nan")
        with open(proj_path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False, sort_keys=True)
            fh.write("\n")
        run_build(data_dir, state_dir, cfg, from_stage="projection")

    elif kind == "missing_fragment":
        manifest = os.path.join(data_dir, "pairs.jsonl")
        first_pair = None
        for line in open(manifest, encoding="utf-8"):
            obj = json.loads(line)
            if obj.get("type") != "manifest_meta":
                first_pair = obj
                break
        os.remove(os.path.join(data_dir, first_pair["image_file"]))
        run_build(data_dir, state_dir, cfg, from_stage="index")

    elif kind == "empty_query":
        store = StateStore(state_dir)
        index_obj, mat = load_index(store)
        projections = _load_projections_checked(data_dir, cfg)
        query_text(index_obj, mat, cfg, projections, "", cfg.top_k)

    elif kind == "k_out_of_range":
        store = StateStore(state_dir)
        index_obj, mat = load_index(store)
        projections = _load_projections_checked(data_dir, cfg)
        num_images = sum(1 for a in index_obj["addressing"] if a["modality"] == "image")
        query_text(index_obj, mat, cfg, projections, "gradient 渐变", num_images + 1)

    elif kind == "bad_image_vector":
        store = StateStore(state_dir)
        index_obj, mat = load_index(store)
        projections = _load_projections_checked(data_dir, cfg)
        query_image_vector(index_obj, mat, cfg, projections,
                           [0.0] * (cfg.image_dim - 1), cfg.top_k)


def list_injections(state_dir):
    store = StateStore(state_dir)
    out = []
    inj_dir = store.path("injections")
    for name in sorted(os.listdir(inj_dir)):
        if name.endswith(".json"):
            with open(os.path.join(inj_dir, name), encoding="utf-8") as fh:
                out.append(json.load(fh))
    return out
