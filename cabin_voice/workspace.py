from __future__ import annotations

import copy
import os
import shutil
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, digest_obj, read_json
from .config import AppConfig, ZONE_ORDER, config_from_dict
from .errors import CabinError, invalid_stage
from .locking import LockSet
from .manifest import load_manifest
from .snapshots import SnapshotStore


class QueryCache:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, key: str) -> Path:
        return self.root / f"{key}.json"

    def get(self, key: str) -> dict[str, Any] | None:
        path = self.path_for(key)
        if not path.exists():
            return None
        try:
            entry = read_json(path)
        except (OSError, ValueError):
            return None
        if entry.get("cache_key") != key:
            return None
        if digest_obj(entry.get("result")) != entry.get("result_digest"):
            return None
        return entry

    def put(self, key: str, result: dict[str, Any]) -> dict[str, Any]:
        entry = {
            "cache_key": key,
            "result": result,
            "result_digest": digest_obj(result),
        }
        atomic_write_json(self.path_for(key), entry)
        return entry


class Workspace:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.state_path = self.root / "state.json"
        self.data_dir = self.root / "data"
        self.locks = LockSet(self.root / "locks")
        self.snapshots = SnapshotStore(self.root)
        self.cache = QueryCache(self.root / "cache" / "queries")
        self.pending_dir = self.root / "pending"
        self.exports_dir = self.root / "exports"
        self.stop_path = self.root / "STOP"
        self.root.mkdir(parents=True, exist_ok=True)
        self.pending_dir.mkdir(parents=True, exist_ok=True)
        self.exports_dir.mkdir(parents=True, exist_ok=True)

    def reset_runtime(self) -> None:
        for path in (
            self.state_path,
            self.root / "snapshots",
            self.root / "cache",
            self.pending_dir,
            self.exports_dir,
            self.stop_path,
        ):
            if path.is_dir():
                shutil.rmtree(path)
            elif path.exists():
                path.unlink()
        self.cache.root.mkdir(parents=True, exist_ok=True)
        self.pending_dir.mkdir(parents=True, exist_ok=True)
        self.exports_dir.mkdir(parents=True, exist_ok=True)

    def load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            raise invalid_stage(
                "workspace has no imported manifest",
                required_stage="import",
                state_path=str(self.state_path),
            )
        try:
            state = read_json(self.state_path)
        except (OSError, ValueError) as exc:
            raise CabinError(
                "E_INVALID_STAGE",
                "workspace state is unreadable",
                details={"reason": str(exc), "path": str(self.state_path)},
                http_status=409,
            ) from exc
        if state.get("schema_version") != 1:
            raise invalid_stage("unsupported workspace state schema")
        return state

    def save_state(self, state: dict[str, Any]) -> None:
        atomic_write_json(self.state_path, state)

    def initialize_from_manifest(
        self,
        manifest_path: str | Path,
        *,
        reset: bool = False,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self.locks.hold("state"):
            manifest = load_manifest(manifest_path)
            if self.state_path.exists() and not reset:
                raise invalid_stage(
                    "workspace is already initialized; pass reset=true to replace it",
                    state_path=str(self.state_path),
                )
            if reset:
                self.reset_runtime()
            if config is None:
                config_value = AppConfig(
                    query_budget_ms=int(manifest.get("default_query_budget_ms", 20))
                )
            else:
                config_value = config_from_dict(config)
                if "query_budget_ms" not in config:
                    config_value = config_from_dict(
                        {
                            **config_value.to_dict(),
                            "query_budget_ms": int(manifest.get("default_query_budget_ms", 20)),
                        }
                    )
            config_value.validate()
            priorities = {
                shard["shard_id"]: copy.deepcopy(shard["initial_priorities"])
                for shard in manifest["shards"]
            }
            word_hits = {
                shard["shard_id"]: copy.deepcopy(shard["initial_word_hits"])
                for shard in manifest["shards"]
            }
            state: dict[str, Any] = {
                "schema_version": 1,
                "workspace_root": str(self.root),
                "zone_order": list(ZONE_ORDER),
                "manifest_path": manifest["manifest_path"],
                "manifest_digest": manifest["manifest_digest"],
                "sample_rate": manifest["sample_rate"],
                "duration_upper_bound_ms": manifest["duration_upper_bound_ms"],
                "shards": manifest["shards"],
                "config": config_value.to_dict(),
                "config_version": 0,
                "edit": {
                    "version": 0,
                    "scope_shard_id": manifest["shards"][0]["shard_id"],
                    "priorities": priorities,
                    "word_hits": word_hits,
                    "activity_ops": {
                        shard["shard_id"]: [] for shard in manifest["shards"]
                    },
                    "audit": [],
                },
                "stages": {},
                "snapshot_head": None,
                "stop_requested": False,
            }
            import_artifact = {
                "manifest_digest": manifest["manifest_digest"],
                "sample_rate": manifest["sample_rate"],
                "duration_upper_bound_ms": manifest["duration_upper_bound_ms"],
                "shards": [
                    {
                        "shard_id": shard["shard_id"],
                        "content_fingerprint": shard["content_fingerprint"],
                        "total_samples": shard["total_samples"],
                        "zone_offset_sum": sum(
                            zone["end_sample"] - zone["start_sample"]
                            for zone in shard["zones"]
                        ),
                    }
                    for shard in manifest["shards"]
                ],
            }
            combined_fingerprint = digest_obj(
                [shard["content_fingerprint"] for shard in manifest["shards"]]
            )
            record = self.snapshots.append(
                stage="import",
                shard_fingerprint=combined_fingerprint,
                config_version=0,
                edit_version=0,
                artifact=import_artifact,
            )
            state["stages"]["import"] = {
                "stage": "import",
                "artifact": import_artifact,
                "artifact_digest": record["artifact_digest"],
                "input_fingerprint": manifest["manifest_digest"],
                "config_digest": config_value.phase_digest("import"),
                "edit_version": 0,
                "snapshot_seq": record["seq"],
            }
            state["snapshot_head"] = self._head_reference()
            self.save_state(state)
            return state

    def _head_reference(self) -> dict[str, Any] | None:
        return self.snapshots.read_head()

    def apply_state_transition(self, updated_state: dict[str, Any]) -> None:
        self.save_state(updated_state)

    def scope_contains(self, state: dict[str, Any], shard_id: str) -> bool:
        return shard_id == state["edit"]["scope_shard_id"]

    def request_stop(self, reason: str = "user_requested") -> dict[str, Any]:
        with self.locks.hold("state"):
            atomic_write_json(
                self.stop_path,
                {"requested": True, "reason": reason, "pid": os.getpid()},
            )
            if self.state_path.exists():
                state = self.load_state()
                state["stop_requested"] = True
                self.save_state(state)
            return {"ok": True, "status": "stop_requested", "reason": reason}

    def is_stop_requested(self) -> bool:
        return self.stop_path.exists()

    def clear_stop(self) -> dict[str, Any]:
        with self.locks.hold("state"):
            if self.stop_path.exists():
                self.stop_path.unlink()
            if self.state_path.exists():
                state = self.load_state()
                state["stop_requested"] = False
                self.save_state(state)
            return {"ok": True, "status": "running"}

    def write_export(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        target = self.exports_dir / name
        atomic_write_json(target, payload)
        return {
            "path": str(target),
            "digest": digest_obj(payload),
            "bytes": target.stat().st_size,
        }
