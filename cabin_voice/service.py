from __future__ import annotations

from pathlib import Path
from typing import Any

from .audio import generate_demo_dataset
from .engine import PipelineEngine
from .errors import invalid_stage
from .query import QueryService
from .transactions import EditTransactions
from .workspace import Workspace


class CabinService:
    def __init__(self, workspace_root: str | Path) -> None:
        self.workspace = Workspace(workspace_root)
        self.engine = PipelineEngine(self.workspace)
        self.queries = QueryService(self.workspace, self.engine)
        self.transactions = EditTransactions(self.workspace)

    def initialize_demo(self, *, seed: int = 20261122, reset: bool = True) -> dict[str, Any]:
        dataset_root = self.workspace.data_dir / f"demo-{seed}"
        paths = generate_demo_dataset(dataset_root, seed=seed)
        state = self.workspace.initialize_from_manifest(
            paths["manifest_path"],
            reset=reset,
            config=None,
        )
        return {
            "ok": True,
            "status": "imported",
            "dataset": paths,
            "manifest_digest": state["manifest_digest"],
            "edit_version": state["edit"]["version"],
            "shards": [shard["shard_id"] for shard in state["shards"]],
        }

    def import_manifest(
        self,
        manifest_path: str,
        *,
        reset: bool = False,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        state = self.workspace.initialize_from_manifest(
            manifest_path,
            reset=reset,
            config=config,
        )
        return {
            "ok": True,
            "status": "imported",
            "manifest_digest": state["manifest_digest"],
            "shards": [shard["shard_id"] for shard in state["shards"]],
            "edit_version": state["edit"]["version"],
            "snapshot_head": state.get("snapshot_head"),
        }

    def run(
        self,
        *,
        stage: str = "all",
        force_full: bool = False,
        verify: bool = True,
    ) -> dict[str, Any]:
        return self.engine.run(stage=stage, force_full=force_full, verify=verify)

    def resume(self, *, verify: bool = True) -> dict[str, Any]:
        cleared = self.workspace.clear_stop()
        result = self.engine.run(stage="all", verify=verify)
        return {**result, "resume": cleared}

    def stop(self, reason: str = "user_requested") -> dict[str, Any]:
        return self.workspace.request_stop(reason)

    def query(
        self,
        kind: str,
        *,
        shard_id: str,
        time_ms: int,
        budget_ms: int | None = None,
    ) -> dict[str, Any]:
        return self.queries.query(
            kind,
            shard_id=shard_id,
            time_ms=time_ms,
            budget_ms=budget_ms,
        )

    def edit(
        self,
        operation: str,
        *,
        parameters: dict[str, Any],
        expected_version: int,
        shard_id: str,
        edit_id: str | None = None,
    ) -> dict[str, Any]:
        return self.transactions.apply_direct(
            operation=operation,
            parameters=parameters,
            expected_version=expected_version,
            shard_id=shard_id,
            edit_id=edit_id,
        )

    def prepare_edit(
        self,
        operation: str,
        *,
        parameters: dict[str, Any],
        expected_version: int,
        shard_id: str,
        edit_id: str | None = None,
    ) -> dict[str, Any]:
        return self.transactions.prepare(
            operation=operation,
            parameters=parameters,
            expected_version=expected_version,
            shard_id=shard_id,
            edit_id=edit_id,
        )

    def publish_edit(self, edit_id: str) -> dict[str, Any]:
        return self.transactions.publish(edit_id)

    def retry_edit(self, edit_id: str, *, expected_version: int | None = None) -> dict[str, Any]:
        return self.transactions.retry(edit_id, expected_version=expected_version)

    def discard_edit(self, edit_id: str) -> dict[str, Any]:
        return self.transactions.discard(edit_id)

    def pending_edits(self) -> dict[str, Any]:
        return {"ok": True, "pending": self.transactions.list_pending()}

    def status(self) -> dict[str, Any]:
        return self.engine.status()

    def versions(self) -> dict[str, Any]:
        return self.engine.versions()

    def verify_snapshots(self) -> dict[str, Any]:
        return self.engine.verify_snapshots()

    def snapshots(self) -> dict[str, Any]:
        state = self.workspace.load_state()
        head = self.workspace.snapshots.read_head()
        records = self.workspace.snapshots._records()
        return {
            "ok": True,
            "head": head,
            "records": [
                {
                    "seq": record["seq"],
                    "stage": record["stage"],
                    "artifact_digest": record["artifact_digest"],
                    "record_digest": record["record_digest"],
                    "previous_chain_digest": record["previous_chain_digest"],
                    "config_version": record["config_version"],
                    "edit_version": record["edit_version"],
                }
                for record in records
            ],
            "state_head": state.get("snapshot_head"),
        }

    def export(self, name: str = "result.json") -> dict[str, Any]:
        return self.engine.export(name)

    def audit(self) -> dict[str, Any]:
        state = self.workspace.load_state()
        return {
            "ok": True,
            "edit_version": state["edit"]["version"],
            "scope_shard_id": state["edit"]["scope_shard_id"],
            "audit": state["edit"]["audit"],
        }

    def timeline(self, shard_id: str) -> dict[str, Any]:
        state = self.workspace.load_state()
        if shard_id not in {shard["shard_id"] for shard in state["shards"]}:
            raise invalid_stage("unknown shard_id", shard_id=shard_id)
        from .projection import build_activity_projection

        projection = build_activity_projection(state)
        return {"ok": True, "timeline": projection[shard_id]}

    def vad_frames(self, shard_id: str, zone_id: str) -> dict[str, Any]:
        state = self.workspace.load_state()
        vad_stage = state.get("stages", {}).get("vad")
        if not vad_stage:
            raise invalid_stage("VAD stage is missing", required_stage="vad")
        try:
            return {
                "ok": True,
                "shard_id": shard_id,
                "zone_id": zone_id,
                "frames": vad_stage["artifact"]["shards"][shard_id]["zones"][zone_id],
            }
        except KeyError as exc:
            raise invalid_stage("unknown shard or zone", shard_id=shard_id, zone_id=zone_id) from exc
