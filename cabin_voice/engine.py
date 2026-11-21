from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from .arbitration import compute_arbitration
from .canonical import canonical_bytes, digest_obj, first_difference
from .config import AppConfig, ZONE_ORDER, config_from_dict
from .errors import CabinError, PipelineStopped, invalid_stage, stale_version
from .projection import build_activity_projection, build_core_result, effective_activity_map
from .timeline import effective_activity_for_shard
from .vad import compute_vad
from .workspace import Workspace


class PipelineEngine:
    def __init__(self, workspace: Workspace) -> None:
        self.workspace = workspace

    @staticmethod
    def _config(state: dict[str, Any]) -> AppConfig:
        return config_from_dict(state["config"])

    @staticmethod
    def _manifest_view(state: dict[str, Any]) -> dict[str, Any]:
        return {
            "sample_rate": state["sample_rate"],
            "shards": state["shards"],
        }

    @staticmethod
    def _combined_content_fingerprint(state: dict[str, Any]) -> str:
        return digest_obj([shard["content_fingerprint"] for shard in state["shards"]])

    def vad_input_fingerprint(
        self,
        state: dict[str, Any],
        config: AppConfig | None = None,
    ) -> str:
        config = config or self._config(state)
        layout = [
            {
                "shard_id": shard["shard_id"],
                "content_fingerprint": shard["content_fingerprint"],
                "zones": [
                    {
                        "zone_id": zone["zone_id"],
                        "start_sample": zone["start_sample"],
                        "end_sample": zone["end_sample"],
                    }
                    for zone in shard["zones"]
                ],
            }
            for shard in state["shards"]
        ]
        return digest_obj(
            {
                "phase": "vad",
                "sample_rate": state["sample_rate"],
                "layout": layout,
                "config_digest": config.phase_digest("vad"),
            }
        )

    @staticmethod
    def _effective_from_vad_artifact(
        state: dict[str, Any],
        vad_artifact: dict[str, Any],
    ) -> dict[str, dict[str, list[list[int]]]]:
        result: dict[str, dict[str, list[list[int]]]] = {}
        for shard in state["shards"]:
            shard_id = shard["shard_id"]
            zones = vad_artifact["shards"][shard_id]["zones"]
            base = {zone_id: zones[zone_id]["intervals"] for zone_id in ZONE_ORDER}
            result[shard_id] = effective_activity_for_shard(
                shard_id,
                base,
                state["edit"]["activity_ops"].get(shard_id, []),
            )
        return result

    def arbitration_input_fingerprint(
        self,
        state: dict[str, Any],
        vad_artifact: dict[str, Any],
        config: AppConfig | None = None,
    ) -> str:
        config = config or self._config(state)
        effective = self._effective_from_vad_artifact(state, vad_artifact)
        return digest_obj(
            {
                "phase": "arbitrate",
                "vad_artifact_digest": digest_obj(vad_artifact),
                "effective_activity_digest": digest_obj(effective),
                "priorities": state["edit"]["priorities"],
                "word_hits": state["edit"]["word_hits"],
                "config_digest": config.phase_digest("arbitrate"),
            }
        )

    def _commit_stage(
        self,
        expected_state: dict[str, Any],
        *,
        stage: str,
        artifact: dict[str, Any],
        input_fingerprint: str,
        config_digest: str,
    ) -> dict[str, Any]:
        with self.workspace.locks.hold("state"):
            latest = self.workspace.load_state()
            if (
                latest["edit"]["version"] != expected_state["edit"]["version"]
                or latest["config_version"] != expected_state["config_version"]
                or latest["manifest_digest"] != expected_state["manifest_digest"]
            ):
                raise stale_version(
                    "workspace changed while the pipeline was computing; retry the run",
                    reason="workspace_changed_during_pipeline",
                    expected_edit_version=expected_state["edit"]["version"],
                    current_edit_version=latest["edit"]["version"],
                    expected_config_version=expected_state["config_version"],
                    current_config_version=latest["config_version"],
                )
            record = self.workspace.snapshots.append(
                stage=stage,
                shard_fingerprint=self._combined_content_fingerprint(latest),
                config_version=latest["config_version"],
                edit_version=latest["edit"]["version"],
                artifact=artifact,
            )
            latest["stages"][stage] = {
                "stage": stage,
                "artifact": artifact,
                "artifact_digest": record["artifact_digest"],
                "input_fingerprint": input_fingerprint,
                "config_digest": config_digest,
                "edit_version": latest["edit"]["version"],
                "snapshot_seq": record["seq"],
            }
            latest["snapshot_head"] = self.workspace.snapshots.read_head()
            self.workspace.save_state(latest)
            return latest

    def _compute_vad(
        self,
        state: dict[str, Any],
        config: AppConfig,
    ) -> dict[str, Any]:
        return compute_vad(
            self._manifest_view(state),
            config.vad,
            should_stop=self.workspace.is_stop_requested,
        )

    def _compute_arbitration(
        self,
        state: dict[str, Any],
        vad_artifact: dict[str, Any],
        config: AppConfig,
    ) -> dict[str, Any]:
        effective = self._effective_from_vad_artifact(state, vad_artifact)
        return compute_arbitration(
            state["shards"],
            effective,
            state["edit"]["priorities"],
            state["edit"]["word_hits"],
            config.arbitration,
            should_stop=self.workspace.is_stop_requested,
        )

    def _full_core_result(self, state: dict[str, Any]) -> dict[str, Any]:
        config = self._config(state)
        vad_artifact = self._compute_vad(state, config)
        arbitration_artifact = self._compute_arbitration(state, vad_artifact, config)
        temporary = copy.deepcopy(state)
        temporary["stages"]["vad"] = {"artifact": vad_artifact}
        temporary["stages"]["arbitrate"] = {"artifact": arbitration_artifact}
        return build_core_result(temporary)

    def _verify_incremental(
        self,
        state: dict[str, Any],
        incremental_result: dict[str, Any],
    ) -> str:
        full_result = self._full_core_result(state)
        incremental_bytes = canonical_bytes(incremental_result)
        full_bytes = canonical_bytes(full_result)
        if incremental_bytes != full_bytes:
            difference = first_difference(incremental_bytes, full_bytes)
            raise CabinError(
                "E_INCREMENTAL_MISMATCH",
                "incremental result differs from a from-scratch full build",
                details={
                    "first_difference": difference,
                    "first_difference_offset": difference["offset"],
                    "incremental_digest": digest_obj(incremental_result),
                    "full_digest": digest_obj(full_result),
                },
                http_status=409,
            )
        return digest_obj(full_result)

    def run(
        self,
        *,
        stage: str = "all",
        force_full: bool = False,
        verify: bool = True,
    ) -> dict[str, Any]:
        if stage not in {"all", "vad", "arbitrate"}:
            raise invalid_stage(
                "unsupported pipeline stage",
                requested_stage=stage,
                allowed_stages=["all", "vad", "arbitrate"],
            )
        with self.workspace.locks.hold("pipeline"):
            state = self.workspace.load_state()
            if "import" not in state.get("stages", {}):
                raise invalid_stage("import stage is missing", required_stage="import")
            if self.workspace.is_stop_requested():
                return self._stop_result(state, [])
            config = self._config(state)
            recomputed: list[str] = []
            reused: list[str] = []
            if stage in {"all", "vad"}:
                desired_vad = self.vad_input_fingerprint(state, config)
                existing_vad = state.get("stages", {}).get("vad")
                if not force_full and existing_vad and existing_vad.get("input_fingerprint") == desired_vad:
                    reused.append("vad")
                else:
                    try:
                        vad_artifact = self._compute_vad(state, config)
                    except PipelineStopped:
                        return self._stop_result(self.workspace.load_state(), recomputed)
                    state = self._commit_stage(
                        state,
                        stage="vad",
                        artifact=vad_artifact,
                        input_fingerprint=desired_vad,
                        config_digest=config.phase_digest("vad"),
                    )
                    recomputed.append("vad")
                if self.workspace.is_stop_requested():
                    return self._stop_result(state, recomputed)
            if stage in {"all", "arbitrate"}:
                vad_stage = state.get("stages", {}).get("vad")
                if not vad_stage:
                    raise invalid_stage(
                        "arbitration requires a completed VAD stage",
                        required_stage="vad",
                        available_stages=sorted(state.get("stages", {})),
                    )
                desired_arb = self.arbitration_input_fingerprint(
                    state,
                    vad_stage["artifact"],
                    config,
                )
                existing_arb = state.get("stages", {}).get("arbitrate")
                if (
                    not force_full
                    and existing_arb
                    and existing_arb.get("input_fingerprint") == desired_arb
                ):
                    reused.append("arbitrate")
                else:
                    try:
                        arbitration_artifact = self._compute_arbitration(
                            state,
                            vad_stage["artifact"],
                            config,
                        )
                    except PipelineStopped:
                        return self._stop_result(self.workspace.load_state(), recomputed)
                    state = self._commit_stage(
                        state,
                        stage="arbitrate",
                        artifact=arbitration_artifact,
                        input_fingerprint=desired_arb,
                        config_digest=config.phase_digest("arbitrate"),
                    )
                    recomputed.append("arbitrate")
            core_result = None
            result_digest = None
            verified_digest = None
            if stage != "vad" and "vad" in state.get("stages", {}) and "arbitrate" in state.get(
                "stages", {}
            ):
                vad_stage = state["stages"]["vad"]
                arbitration_stage = state["stages"]["arbitrate"]
                if (
                    vad_stage.get("input_fingerprint")
                    == self.vad_input_fingerprint(state, config)
                    and arbitration_stage.get("input_fingerprint")
                    == self.arbitration_input_fingerprint(
                        state,
                        vad_stage["artifact"],
                        config,
                    )
                ):
                    core_result = build_core_result(state)
                    result_digest = digest_obj(core_result)
                    if verify:
                        verified_digest = self._verify_incremental(state, core_result)
            stopped = self.workspace.is_stop_requested()
            return {
                "ok": True,
                "status": "stopped" if stopped else "completed",
                "stage": stage,
                "recomputed_stages": recomputed,
                "reused_stages": reused,
                "result_digest": result_digest,
                "verified_full_digest": verified_digest,
                "edit_version": state["edit"]["version"],
                "config_version": state["config_version"],
                "snapshot_head": state.get("snapshot_head"),
                "stop_requested": stopped,
            }

    def _stop_result(self, state: dict[str, Any], recomputed: list[str]) -> dict[str, Any]:
        return {
            "ok": True,
            "status": "stopped",
            "recomputed_stages": recomputed,
            "reused_stages": [],
            "edit_version": state["edit"]["version"],
            "config_version": state["config_version"],
            "snapshot_head": state.get("snapshot_head"),
            "stop_requested": True,
        }

    def status(self) -> dict[str, Any]:
        state = self.workspace.load_state()
        config = self._config(state)
        stages: dict[str, Any] = {}
        import_stage = state.get("stages", {}).get("import")
        stages["import"] = {
            "present": import_stage is not None,
            "snapshot_seq": import_stage.get("snapshot_seq") if import_stage else None,
            "artifact_digest": import_stage.get("artifact_digest") if import_stage else None,
        }
        desired_vad = self.vad_input_fingerprint(state, config)
        vad_stage = state.get("stages", {}).get("vad")
        stages["vad"] = {
            "present": vad_stage is not None,
            "fresh": bool(vad_stage and vad_stage.get("input_fingerprint") == desired_vad),
            "input_fingerprint": desired_vad,
            "snapshot_seq": vad_stage.get("snapshot_seq") if vad_stage else None,
            "artifact_digest": vad_stage.get("artifact_digest") if vad_stage else None,
        }
        if vad_stage:
            desired_arb = self.arbitration_input_fingerprint(
                state,
                vad_stage["artifact"],
                config,
            )
        else:
            desired_arb = None
        arb_stage = state.get("stages", {}).get("arbitrate")
        stages["arbitrate"] = {
            "present": arb_stage is not None,
            "fresh": bool(
                arb_stage
                and desired_arb is not None
                and arb_stage.get("input_fingerprint") == desired_arb
            ),
            "input_fingerprint": desired_arb,
            "snapshot_seq": arb_stage.get("snapshot_seq") if arb_stage else None,
            "artifact_digest": arb_stage.get("artifact_digest") if arb_stage else None,
        }
        result_digest = None
        if (
            vad_stage
            and arb_stage
            and stages["vad"]["fresh"]
            and stages["arbitrate"]["fresh"]
        ):
            result_digest = digest_obj(build_core_result(state))
        return {
            "ok": True,
            "manifest_digest": state["manifest_digest"],
            "sample_rate": state["sample_rate"],
            "shards": [
                {
                    "shard_id": shard["shard_id"],
                    "title": shard["title"],
                    "content_fingerprint": shard["content_fingerprint"],
                    "zone_count": len(shard["zones"]),
                    "total_samples": shard["total_samples"],
                }
                for shard in state["shards"]
            ],
            "edit_version": state["edit"]["version"],
            "scope_shard_id": state["edit"]["scope_shard_id"],
            "config_version": state["config_version"],
            "stages": stages,
            "result_digest": result_digest,
            "snapshot_head": state.get("snapshot_head"),
            "stop_requested": self.workspace.is_stop_requested(),
            "config": config.to_dict(),
            "zone_order": list(ZONE_ORDER),
        }

    def versions(self) -> dict[str, Any]:
        state = self.workspace.load_state()
        config = self._config(state)
        stage_fingerprints = {
            stage: record.get("input_fingerprint")
            for stage, record in state.get("stages", {}).items()
        }
        status = self.status()
        result_digest = status.get("result_digest")
        return {
            "ok": True,
            "manifest_digest": state["manifest_digest"],
            "edit_version": state["edit"]["version"],
            "config_version": state["config_version"],
            "config_digests": {
                "vad": config.phase_digest("vad"),
                "arbitrate": config.phase_digest("arbitrate"),
                "all": config.phase_digest("all"),
            },
            "stage_input_fingerprints": stage_fingerprints,
            "result_digest": result_digest,
            "snapshot_head": state.get("snapshot_head"),
        }

    def export(self, name: str = "result.json") -> dict[str, Any]:
        state = self.workspace.load_state()
        bundle = build_core_result(state)
        export_info = self.workspace.write_export(name, bundle)
        digest = export_info["digest"]
        for legacy_name in ("activity.json", "tdoa.json", "assignments.json"):
            if legacy_name == name:
                continue
            key = legacy_name.split(".")[0]
            self.workspace.write_export(legacy_name, {key: bundle[key], "edit_version": bundle["edit_version"]})
        return {
            "ok": True,
            "export": export_info,
            "result_digest": digest,
            "edit_version": bundle["edit_version"],
            "config_version": bundle["config_version"],
        }

    def verify_snapshots(self) -> dict[str, Any]:
        state = self.workspace.load_state()
        expected_stage_digests = {
            stage: record["artifact_digest"]
            for stage, record in state.get("stages", {}).items()
            if record.get("artifact_digest")
        }
        return self.workspace.snapshots.verify(
            expected_state_head=state.get("snapshot_head"),
            expected_stage_digests=expected_stage_digests,
        )
