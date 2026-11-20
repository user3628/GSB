"""Workbench facade shared by the CLI and the HTTP server."""

import json
import os

from .checkpoint import StateStore, read_json
from .config import Config
from .errors import CmrError
from .indexer import load_index
from .inject import list_injections, run_injection
from .pipeline import compute_fingerprints, ensure_fresh, plan_rebuild, run_build
from .retrieval import query_image, query_image_vector, query_text
from .samples import init_workspace


class Workbench:
    def __init__(self, data_dir="data", state_dir="state"):
        self.data_dir = data_dir
        self.state_dir = state_dir
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(state_dir, exist_ok=True)

    # ---- setup ----
    def config_path(self):
        return os.path.join(self.data_dir, "config.json")

    def load_config(self):
        if os.path.exists(self.config_path()):
            return Config.load(self.config_path())
        return Config()

    def init(self):
        cfg = self.load_config()
        created = init_workspace(self.data_dir, cfg)
        StateStore(self.state_dir)
        return {"created": created, "config": cfg.to_dict()}

    # ---- build ----
    def build(self, from_stage=None, stop_after=None):
        cfg = self.load_config()
        self._require_manifest()
        return run_build(self.data_dir, self.state_dir, cfg,
                         from_stage=from_stage, stop_after=stop_after)

    def import_pairs(self, jsonl_text):
        """Import a JSONL manifest (replaces pairs.jsonl)."""
        lines = [l for l in jsonl_text.splitlines() if l.strip()]
        if not lines:
            raise CmrError("E_IMPORT_INVALID", "empty manifest")
        meta = json.loads(lines[0])
        if meta.get("type") != "manifest_meta":
            raise CmrError("E_IMPORT_INVALID", "first line must be manifest_meta")
        count = 0
        for line in lines[1:]:
            obj = json.loads(line)
            for field in ("pair_id", "image_file", "caption", "lang", "source"):
                if field not in obj:
                    raise CmrError("E_IMPORT_INVALID",
                                   f"pair line missing field '{field}'")
            count += 1
        path = os.path.join(self.data_dir, "pairs.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        return {"imported_pairs": count, "manifest": path}

    # ---- query ----
    def _query_context(self):
        cfg = self.load_config()
        self._require_manifest()
        ensure_fresh(self.data_dir, self.state_dir, cfg)
        store = StateStore(self.state_dir)
        index_obj, mat = load_index(store)
        from .pipeline import _load_projections_checked
        projections = _load_projections_checked(self.data_dir, cfg)
        return cfg, index_obj, mat, projections

    def query_text(self, text, k=None):
        cfg, index_obj, mat, projections = self._query_context()
        k = cfg.top_k if k is None else k
        result = query_text(index_obj, mat, cfg, projections, text, k)
        result["index_digest"] = index_obj["meta"]["index_digest"]
        return result

    def query_image(self, image, k=None):
        cfg, index_obj, mat, projections = self._query_context()
        k = cfg.top_k if k is None else k
        image_ref = image
        if not image.startswith("pair:") and not os.path.isabs(image):
            candidate = os.path.join(self.data_dir, image)
            if os.path.exists(candidate):
                image_ref = candidate
        result = query_image(index_obj, mat, cfg, projections, image_ref, k,
                             data_dir=self.data_dir)
        result["index_digest"] = index_obj["meta"]["index_digest"]
        return result

    def query_image_vector(self, vector, k=None):
        cfg, index_obj, mat, projections = self._query_context()
        k = cfg.top_k if k is None else k
        result = query_image_vector(index_obj, mat, cfg, projections, vector, k)
        result["index_digest"] = index_obj["meta"]["index_digest"]
        return result

    # ---- status / diagnostics ----
    def status(self):
        cfg = self.load_config()
        store = StateStore(self.state_dir)
        out = {
            "data_dir": os.path.abspath(self.data_dir),
            "state_dir": os.path.abspath(self.state_dir),
            "config": cfg.to_dict(),
            "stop_requested": store.stop_requested(),
        }
        try:
            fps = compute_fingerprints(self.data_dir, cfg)
            stale_idx, inv = plan_rebuild(store, fps)
            out["current_stage_fps"] = fps["stage_fps"]
            out["stale"] = stale_idx is not None
            out["planned_rebuild_scope"] = inv["rebuild_scope"]
            out["planned_invalidation_reason"] = inv["reason"]
        except (OSError, json.JSONDecodeError) as exc:
            out["fingerprint_error"] = str(exc)
        published = read_json(store.path("published", "fingerprints.json"))
        out["published_stage_fps"] = published.get("stage_fps") if published else None
        out["last_invalidation"] = read_json(store.path("published", "invalidation.json"))
        index_obj = read_json(store.path("index", "index.json"))
        out["index_meta"] = index_obj.get("meta") if index_obj else None
        out["recall"] = read_json(store.path("recall", "recall.json"))
        out["checkpoints"] = store.list_checkpoints()
        out["injections"] = list_injections(self.state_dir)
        return out

    # ---- injections ----
    def inject(self, kind):
        cfg = self.load_config()
        self._require_manifest()
        return run_injection(kind, self.data_dir, self.state_dir, cfg)

    # ---- stop / resume ----
    def stop(self):
        store = StateStore(self.state_dir)
        store.request_stop()
        return {"stop_requested": True}

    def resume(self, stop_after=None):
        store = StateStore(self.state_dir)
        store.clear_stop()
        return self.build(stop_after=stop_after)

    def publish(self, record, expected_seq=None):
        store = StateStore(self.state_dir)
        return store.publish_checkpoint(record, expected_seq=expected_seq)

    def _require_manifest(self):
        if not os.path.exists(os.path.join(self.data_dir, "pairs.jsonl")):
            raise CmrError("E_NO_MANIFEST",
                           "pairs.jsonl not found; run `init` or import pairs first")
