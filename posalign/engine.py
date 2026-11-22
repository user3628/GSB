"""End-to-end orchestration for migration, alignment, adjudication, and summary."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from .adjudication import adjudicate
from .alignment import align_shard
from .canonical import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_shards,
    digest_json,
    pretty_json_bytes,
    read_bytes,
    read_json,
    sha256_bytes,
    shard_id_for,
    write_jsonl,
)
from .checkpoints import CheckpointStore
from .config import GovernanceConfig, load_governance_config
from .errors import (
    BusinessError,
    E_ALIGNMENT_FAILED,
    E_INVALID_INPUT,
    E_STALE_MAPPING,
    StopRequested,
)
from .filelock import exclusive_file_lock
from .mapping_store import MappingStore
from .migration import build_migration_report
from .parse import StopGate, export_v1_records, parse_shard
from .summary import build_global_summary, build_shard_summary
from .textnorm import normalize_label


ENGINE_SCHEMA = "posalign-engine/1.0"


class Engine:
    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace).expanduser().resolve()
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.state = CheckpointStore(self.workspace)
        self.mapping = MappingStore(self.workspace)
        self.output_dir = self.workspace / "output"
        self.stale_output_path = self.output_dir / "STALE.json"
        self.stop_path = self.workspace / "state" / "STOP"
        self.stopped_report_path = self.workspace / "state" / "STOPPED.json"
        self.failure_path = self.output_dir / "alignment_failure.json"
        self.run_lock_path = self.workspace / ".posalign.run.lock"

    def reset_managed_state(self) -> None:
        state_root = self.workspace / "state"
        mapping_root = self.workspace / "mapping"
        managed: list[Path] = [
            state_root / "head.json",
            state_root / "checkpoints",
            state_root / "objects",
            state_root / "mapping_anchor.json",
            state_root / "STOP",
            state_root / "STOPPED.json",
            state_root / "lock",
            mapping_root / "current.json",
            mapping_root / "ledger.json",
            mapping_root / "versions",
            mapping_root / "STALE.json",
            mapping_root / "lock",
            self.output_dir / "STALE.json",
            self.output_dir / "alignment_failure.json",
            self.output_dir / "manifest.json",
            self.output_dir / "fixed_digest.txt",
            self.output_dir / "migration.json",
            self.output_dir / "alignment.json",
            self.output_dir / "mapping_candidates.json",
            self.output_dir / "mapping_resolved.json",
            self.output_dir / "summary.json",
            self.output_dir / "stats.json",
        ]
        for path in managed:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink(missing_ok=True)
        self.workspace.mkdir(parents=True, exist_ok=True)

    def _desired_parse(
        self,
        config: GovernanceConfig,
        shard_id: str,
        shard_digest: str,
    ) -> str:
        return digest_json(
            {
                "schema": "parse-input.v1",
                "engine": ENGINE_SCHEMA,
                "shard_id": shard_id,
                "shard_digest": shard_digest,
                "alias_fingerprint": config.alias_fingerprint,
                "candidate_rules_fingerprint": config.candidate_rules_fingerprint,
                "v1_fingerprint": config.v1_fingerprint,
            }
        )

    def _desired_align(self, parse_fingerprint: str, config: GovernanceConfig) -> str:
        return digest_json(
            {
                "schema": "align-input.v1",
                "engine": ENGINE_SCHEMA,
                "parse_fingerprint": parse_fingerprint,
                "alignment_fingerprint": config.alignment_fingerprint,
            }
        )

    def _mapping_input_digest(
        self, desired_align: dict[str, str], config: GovernanceConfig
    ) -> str:
        return digest_json(
            {
                "schema": "mapping-input.v1",
                "engine": ENGINE_SCHEMA,
                "alignment": desired_align,
                "adjudication_fingerprint": config.adjudication_fingerprint,
                "confidence_threshold": config.adjudication.confidence_threshold,
            }
        )

    def _desired_adjudicate(
        self, mapping_input_digest: str, config: GovernanceConfig
    ) -> str:
        return digest_json(
            {
                "schema": "adjudicate-input.v1",
                "engine": ENGINE_SCHEMA,
                "mapping_input_digest": mapping_input_digest,
                "adjudication_fingerprint": config.adjudication_fingerprint,
            }
        )

    def _desired_summary(
        self, align_fingerprint: str, mapping_version: int, mapping_digest: str
    ) -> str:
        return digest_json(
            {
                "schema": "summary-input.v1",
                "engine": ENGINE_SCHEMA,
                "align_fingerprint": align_fingerprint,
                "mapping_version": mapping_version,
                "mapping_digest": mapping_digest,
            }
        )

    def _components(
        self,
        config: GovernanceConfig,
        shard_digests: dict[str, str],
    ) -> dict[str, Any]:
        return {
            "engine": ENGINE_SCHEMA,
            "governance": config.components(),
            "shards": dict(sorted(shard_digests.items())),
        }

    def _product(self, shard: str, stage: str) -> dict[str, Any] | None:
        head = self.state.read_head()
        return head["products"].get(f"{shard}/{stage}")

    def _product_matches(
        self, shard: str, stage: str, fingerprint: str
    ) -> dict[str, Any] | None:
        product = self._product(shard, stage)
        if product is not None and product.get("fingerprint") == fingerprint:
            return product
        return None

    def _write_stale_outputs(self, reasons: list[dict[str, Any]], generation: int) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(
            self.stale_output_path,
            {
                "schema": "posalign.output_stale.v1",
                "reason": "upstream_invalidation",
                "generation": generation,
                "reasons": reasons,
            },
        )

    def _write_stop_report(
        self,
        *,
        request: StopRequested,
        generation: int,
        sequence: int,
        mapping_version: int,
    ) -> dict[str, Any]:
        report = {
            "schema": "posalign.stopped.v1",
            "status": "stopped",
            "reason": request.reason,
            "completed_sentences": request.completed_sentences,
            "generation": generation,
            "sequence": sequence,
            "mapping_version": mapping_version,
            "resume_safe": True,
            "forced_termination_report": False,
        }
        atomic_write_json(self.stopped_report_path, report)
        self.state.mark_status(
            "stopped",
            extra={
                "reason": request.reason,
                "completed_sentences": request.completed_sentences,
            },
        )
        return report

    def _write_alignment_failure(self, error: BusinessError) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(
            self.failure_path,
            {
                "schema": "posalign.alignment_failure.v1",
                "ok": False,
                "error": error.to_dict()["error"],
                "new_alignment_checkpoints_written": False,
                "mapping_published": False,
            },
        )

    def _report(
        self,
        status: str,
        *,
        generation: int,
        sequence: int,
        invalidation: dict[str, Any] | None,
        mapping_version: int,
        outputs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "ok": True,
            "status": status,
            "generation": generation,
            "sequence": sequence,
            "mapping_version": mapping_version,
            "invalidation": invalidation,
            "outputs": outputs or {},
        }

    def run(
        self,
        shards: list[str | Path],
        *,
        config_path: str | Path,
        reset: bool = False,
        stop_after_sentences: int | None = None,
        clear_stop: bool = False,
    ) -> dict[str, Any]:
        if stop_after_sentences is not None and stop_after_sentences < 1:
            raise BusinessError(
                E_INVALID_INPUT,
                "stop_after_sentences must be at least 1",
            )
        with exclusive_file_lock(self.run_lock_path, timeout=120.0):
            if reset:
                self.reset_managed_state()
            if clear_stop:
                self.stop_path.unlink(missing_ok=True)
            return self._run_locked(
                shards,
                config_path=config_path,
                stop_after_sentences=stop_after_sentences,
            )

    def _run_locked(
        self,
        shards: list[str | Path],
        *,
        config_path: str | Path,
        stop_after_sentences: int | None,
    ) -> dict[str, Any]:
        config = load_governance_config(config_path)
        paths = canonical_shards(shards)
        raw = {shard_id_for(path): read_bytes(path) for path in paths}
        shard_digests = {name: sha256_bytes(data) for name, data in raw.items()}
        shard_ids = sorted(shard_digests)
        desired_parse = {
            shard: self._desired_parse(config, shard, shard_digests[shard])
            for shard in shard_ids
        }
        desired_align = {
            shard: self._desired_align(desired_parse[shard], config)
            for shard in shard_ids
        }
        mapping_input_digest = self._mapping_input_digest(desired_align, config)
        desired_adjudicate = self._desired_adjudicate(mapping_input_digest, config)
        components = self._components(config, shard_digests)
        signature = digest_json(
            {
                "components": components,
                "parse": desired_parse,
                "align": desired_align,
                "adjudicate": desired_adjudicate,
                "shards": shard_ids,
            }
        )

        try:
            pre_validation = self.mapping.validate(
                expected_alias_fingerprint=config.alias_fingerprint,
                expected_adjudication_fingerprint=config.adjudication_fingerprint,
                expected_input_digest=mapping_input_digest,
            )
        except BusinessError:
            raise
        stale = self.mapping.stale()
        force_adjudicate = (
            pre_validation.status == "empty"
            or pre_validation.requires_rebuild
            or stale is not None
        )
        force_reasons: list[dict[str, Any]] = []
        if pre_validation.status == "empty":
            force_reasons.append(
                {"stage": "adjudicate", "shard": "*", "reason": "mapping_table_missing"}
            )
        for reason in pre_validation.rebuild_reasons:
            force_reasons.append(
                {"stage": "adjudicate", "shard": "*", "reason": reason}
            )
        if stale is not None:
            force_reasons.append(
                {"stage": "adjudicate", "shard": "*", "reason": "mapping_marked_stale"}
            )

        prepared = self.state.prepare_run(
            components=components,
            shard_ids=shard_ids,
            desired_parse=desired_parse,
            desired_align=desired_align,
            desired_adjudicate=desired_adjudicate,
            signature=signature,
            force_adjudicate=force_adjudicate,
            force_reasons=force_reasons,
        )
        invalidation = prepared.invalidation
        if not prepared.no_work:
            self._write_stale_outputs(invalidation.get("reasons", []), prepared.generation)
            if force_adjudicate:
                self.mapping.mark_stale(invalidation.get("reasons", []))

        stop_gate = StopGate(
            stop_file=self.stop_path,
            max_sentences=stop_after_sentences,
        )
        try:
            parse_payloads: dict[str, dict[str, Any]] = {}
            for path in paths:
                shard = shard_id_for(path)
                product = self._product_matches(shard, "parse", desired_parse[shard])
                if product is not None:
                    payload = self.state.payload_for(product)
                else:
                    payload = parse_shard(path, config=config, stop_gate=stop_gate)
                    self.state.publish(
                        stage="parse",
                        shard=shard,
                        payload=payload,
                        fingerprint=desired_parse[shard],
                        generation=prepared.generation,
                        governance_version=config.version,
                        governance_fingerprints=config.components(),
                        shard_content_fp=shard_digests[shard],
                        mapping_version=0,
                    )
                    product = self.state.read_head()["products"][f"{shard}/parse"]
                parse_payloads[shard] = payload

            seen_sent_ids: dict[str, str] = {}
            for shard in shard_ids:
                for record in parse_payloads[shard]["records"]:
                    if record["sent_id"] in seen_sent_ids:
                        raise BusinessError(
                            E_INVALID_INPUT,
                            "sent_id must be unique across shards",
                            {
                                "sent_id": record["sent_id"],
                                "first_shard": seen_sent_ids[record["sent_id"]],
                                "second_shard": shard,
                            },
                        )
                    seen_sent_ids[record["sent_id"]] = shard

            align_payloads: dict[str, dict[str, Any]] = {}
            invalid_align: list[str] = []
            for shard in shard_ids:
                product = self._product_matches(shard, "align", desired_align[shard])
                if product is not None:
                    align_payloads[shard] = self.state.payload_for(product)
                else:
                    invalid_align.append(shard)
            computed_align: dict[str, dict[str, Any]] = {}
            try:
                for shard in invalid_align:
                    computed_align[shard] = align_shard(
                        parse_payloads[shard], config=config, stop_gate=stop_gate
                    )
            except BusinessError as error:
                if error.code == E_ALIGNMENT_FAILED:
                    self._write_alignment_failure(error)
                    self.state.mark_status("failed", extra={"code": error.code})
                raise
            for shard in invalid_align:
                self.state.publish(
                    stage="align",
                    shard=shard,
                    payload=computed_align[shard],
                    fingerprint=desired_align[shard],
                    generation=prepared.generation,
                    governance_version=config.version,
                    governance_fingerprints=config.components(),
                    shard_content_fp=shard_digests[shard],
                    mapping_version=0,
                )
                product = self.state.read_head()["products"][f"{shard}/align"]
                align_payloads[shard] = computed_align[shard]

            stop_gate.check_requested()

            mapping_validation = self.mapping.validate(
                expected_alias_fingerprint=config.alias_fingerprint,
                expected_adjudication_fingerprint=config.adjudication_fingerprint,
                expected_input_digest=mapping_input_digest,
            )
            mapping_ready = (
                mapping_validation.status == "valid"
                and self.mapping.stale() is None
            )
            need_mapping_rebuild = not mapping_ready
            if need_mapping_rebuild:
                self.state.invalidate_stages(
                    ["adjudicate", "summarize"],
                    reasons=[
                        {
                            "stage": "adjudicate",
                            "shard": "*",
                            "reason": "mapping_rebuild_required",
                        }
                    ],
                )
                self.mapping.mark_stale(
                    [
                        {
                            "stage": "adjudicate",
                            "shard": "*",
                            "reason": "mapping_rebuild_required",
                        }
                    ]
                )
                mapping_core = adjudicate(
                    [align_payloads[shard] for shard in shard_ids], config
                )
                table = self.mapping.publish(
                    mapping_core,
                    alias_fingerprint=config.alias_fingerprint,
                    adjudication_fingerprint=config.adjudication_fingerprint,
                    input_digest=mapping_input_digest,
                )
                mapping_version = int(table["version"])
                mapping_digest = table["mapping_digest"]
                for shard in shard_ids:
                    self.state.publish(
                        stage="adjudicate",
                        shard=shard,
                        payload=mapping_core,
                        fingerprint=desired_adjudicate,
                        generation=prepared.generation,
                        governance_version=config.version,
                        governance_fingerprints=config.components(),
                        shard_content_fp=shard_digests[shard],
                        mapping_version=mapping_version,
                        required_shards=shard_ids,
                    )
            else:
                table = self.mapping.current(expected_input_digest=mapping_input_digest)
                if table is None:
                    raise BusinessError(
                        E_STALE_MAPPING,
                        "mapping table disappeared during run",
                    )
                mapping_core = table["mapping"]
                mapping_version = int(table["version"])
                mapping_digest = table["mapping_digest"]
                for shard in shard_ids:
                    product = self._product_matches(
                        shard, "adjudicate", desired_adjudicate
                    )
                    if product is None or product.get("mapping_version") != mapping_version:
                        self.state.publish(
                            stage="adjudicate",
                            shard=shard,
                            payload=mapping_core,
                            fingerprint=desired_adjudicate,
                            generation=prepared.generation,
                            governance_version=config.version,
                            governance_fingerprints=config.components(),
                            shard_content_fp=shard_digests[shard],
                            mapping_version=mapping_version,
                            required_shards=shard_ids,
                        )
            stop_gate.check_requested()

            shard_summaries: dict[str, dict[str, Any]] = {}
            for shard in shard_ids:
                desired_summary = self._desired_summary(
                    desired_align[shard], mapping_version, mapping_digest
                )
                product = self._product_matches(shard, "summarize", desired_summary)
                if product is not None:
                    shard_summaries[shard] = self.state.payload_for(product)
                else:
                    summary_payload = build_shard_summary(
                        parse_payload=parse_payloads[shard],
                        align_payload=align_payloads[shard],
                        mapping_core=mapping_core,
                        mapping_version=mapping_version,
                        mapping_digest=mapping_digest,
                        config=config,
                    )
                    self.state.publish(
                        stage="summarize",
                        shard=shard,
                        payload=summary_payload,
                        fingerprint=desired_summary,
                        generation=prepared.generation,
                        governance_version=config.version,
                        governance_fingerprints=config.components(),
                        shard_content_fp=shard_digests[shard],
                        mapping_version=mapping_version,
                        required_shards=shard_ids,
                    )
                    shard_summaries[shard] = summary_payload
                stop_gate.check_requested()

            outputs = self._write_outputs(
                config=config,
                parse_payloads=[parse_payloads[shard] for shard in shard_ids],
                align_payloads=[align_payloads[shard] for shard in shard_ids],
                shard_summaries=[shard_summaries[shard] for shard in shard_ids],
                mapping_core=mapping_core,
                mapping_version=mapping_version,
                mapping_digest=mapping_digest,
            )
            self.mapping.clear_stale()
            self.state.mark_status("complete")
            self.stopped_report_path.unlink(missing_ok=True)
            self.stop_path.unlink(missing_ok=True)
            self.stale_output_path.unlink(missing_ok=True)
            self.failure_path.unlink(missing_ok=True)
            final_status = "up_to_date" if prepared.no_work else "completed"
            head = self.state.read_head()
            return self._report(
                final_status,
                generation=head["generation"],
                sequence=head["seq"],
                invalidation=head.get("last_invalidation"),
                mapping_version=mapping_version,
                outputs=outputs,
            )
        except StopRequested as request:
            head = self.state.read_head()
            try:
                table = self.mapping.current(allow_stale=True)
                mapping_version = int(table["version"]) if table else 0
            except BusinessError:
                mapping_version = 0
            stop_report = self._write_stop_report(
                request=request,
                generation=int(head["generation"]),
                sequence=int(head["seq"]),
                mapping_version=mapping_version,
            )
            return {
                "ok": True,
                "status": "stopped",
                "generation": head["generation"],
                "sequence": head["seq"],
                "mapping_version": mapping_version,
                "invalidation": head.get("last_invalidation"),
                "stop": stop_report,
            }
        except BusinessError as error:
            self.state.mark_status(
                "failed", extra={"code": error.code, "message": error.message}
            )
            raise

    def _write_outputs(
        self,
        *,
        config: GovernanceConfig,
        parse_payloads: list[dict[str, Any]],
        align_payloads: list[dict[str, Any]],
        shard_summaries: list[dict[str, Any]],
        mapping_core: dict[str, Any],
        mapping_version: int,
        mapping_digest: str,
    ) -> dict[str, Any]:
        migration = build_migration_report(
            parse_payloads,
            config_components=config.components(),
        )
        align_sorted = sorted(align_payloads, key=lambda item: item["shard_id"])
        aggregate_align_stats = {
            "sentences": sum(item["stats"]["sentences"] for item in align_sorted),
            "aligned": sum(item["stats"]["aligned"] for item in align_sorted),
            "unpaired": sum(item["stats"]["unpaired"] for item in align_sorted),
            "unsegmented": sum(item["stats"]["unsegmented"] for item in align_sorted),
            "empty": sum(item["stats"]["empty"] for item in align_sorted),
            "matched_pairs": sum(item["stats"]["matched_pairs"] for item in align_sorted),
            "insertions": sum(item["stats"]["insertions"] for item in align_sorted),
            "deletions": sum(item["stats"]["deletions"] for item in align_sorted),
        }
        rates = [
            item["stats"]["average_alignment_rate"]
            for item in align_sorted
            if item["stats"]["average_alignment_rate"] is not None
        ]
        aggregate_align_stats["average_alignment_rate"] = (
            round(sum(rates) / len(rates), 6) if rates else None
        )
        alignment = {
            "schema": "posalign.alignment_report.v1",
            "shards": align_sorted,
            "stats": aggregate_align_stats,
        }
        mapping_resolved = {
            "schema": "posalign.mapping_resolved.v1",
            "mapping_version": mapping_version,
            "mapping_digest": mapping_digest,
            "resolved": mapping_core.get("resolved", []),
        }
        mapping_candidates = {
            "schema": "posalign.mapping_candidates.v1",
            "mapping_version": mapping_version,
            "mapping_digest": mapping_digest,
            "labels": mapping_core.get("candidate_index", {}),
            "diagnostics": mapping_core.get("diagnostics", []),
        }
        summary, stats = build_global_summary(
            shard_summaries=shard_summaries,
            parse_payloads=parse_payloads,
            align_payloads=align_payloads,
            mapping_core=mapping_core,
            mapping_version=mapping_version,
            config=config,
        )
        artifacts = {
            "migration.json": pretty_json_bytes(migration),
            "alignment.json": pretty_json_bytes(alignment),
            "mapping_candidates.json": pretty_json_bytes(mapping_candidates),
            "mapping_resolved.json": pretty_json_bytes(mapping_resolved),
            "summary.json": pretty_json_bytes(summary),
            "stats.json": pretty_json_bytes(stats),
        }
        self.output_dir.mkdir(parents=True, exist_ok=True)
        artifact_hashes: dict[str, str] = {}
        for name, data in artifacts.items():
            atomic_write_bytes(self.output_dir / name, data)
            artifact_hashes[name] = sha256_bytes(data)
        fixed_digest = digest_json(artifact_hashes)
        atomic_write_bytes(
            self.output_dir / "fixed_digest.txt",
            (fixed_digest + "\n").encode("utf-8"),
        )
        manifest = {
            "schema": "posalign.output_manifest.v1",
            "mapping_version": mapping_version,
            "mapping_digest": mapping_digest,
            "artifacts": artifact_hashes,
            "fixed_digest": fixed_digest,
            "complete": True,
        }
        atomic_write_json(self.output_dir / "manifest.json", manifest, pretty=True)
        return {
            "directory": str(self.output_dir),
            "artifacts": artifact_hashes,
            "fixed_digest": fixed_digest,
        }

    def status(self) -> dict[str, Any]:
        checkpoint_status = self.state.status()
        mapping_status = self.mapping.status()
        manifest = read_json(self.output_dir / "manifest.json", default=None)
        stale_outputs = read_json(self.stale_output_path, default=None)
        stopped = read_json(self.stopped_report_path, default=None)
        failure = read_json(self.failure_path, default=None)
        return {
            "ok": True,
            "workspace": str(self.workspace),
            "checkpoints": checkpoint_status,
            "invalidation": checkpoint_status.get("invalidation"),
            "mapping": mapping_status,
            "outputs": manifest if stale_outputs is None else None,
            "outputs_stale": stale_outputs,
            "stopped": stopped,
            "last_alignment_failure": failure,
        }

    def mapping_resolved(self, *, allow_stale: bool = False) -> dict[str, Any]:
        table = self.mapping.current(allow_stale=allow_stale)
        if table is None:
            return {
                "ok": True,
                "mapping_version": 0,
                "mapping_digest": None,
                "resolved": [],
            }
        return {
            "ok": True,
            "mapping_version": table["version"],
            "mapping_digest": table["mapping_digest"],
            "resolved": table["mapping"].get("resolved", []),
        }

    def mapping_candidates(
        self, label: str | None = None, *, allow_stale: bool = False
    ) -> dict[str, Any]:
        table = self.mapping.current(allow_stale=allow_stale)
        if table is None:
            if label is not None:
                migration = self._migration_candidate_for_label(label)
                if migration is not None:
                    return {
                        "ok": True,
                        "mapping_version": 0,
                        "mapping_digest": None,
                        "label": label,
                        "candidate_stage": "migration",
                        "candidates": migration["candidates"],
                        "diagnostics": migration["diagnostics"],
                    }
            return {
                "ok": True,
                "mapping_version": 0,
                "mapping_digest": None,
                "label": label,
                "candidate_stage": "none",
                "candidates": [] if label is not None else {},
            }
        index = table["mapping"].get("candidate_index", {})
        if label is None:
            candidates: Any = index
            diagnostics: Any = table["mapping"].get("diagnostics", [])
            candidate_stage = "adjudicated_and_migration"
        else:
            normalized = normalize_label(label)
            candidates = index.get(normalized, [])
            diagnostics = []
            candidate_stage = "adjudicated"
            if not candidates:
                migration = self._migration_candidate_for_label(label)
                if migration is not None:
                    candidates = migration["candidates"]
                    diagnostics = migration["diagnostics"]
                    candidate_stage = "migration"
        return {
            "ok": True,
            "mapping_version": table["version"],
            "mapping_digest": table["mapping_digest"],
            "label": label,
            "candidate_stage": candidate_stage,
            "candidates": candidates,
            "diagnostics": diagnostics,
        }

    def _migration_candidate_for_label(self, label: str) -> dict[str, Any] | None:
        report = read_json(self.output_dir / "migration.json", default=None)
        if not isinstance(report, dict):
            return None
        normalized = normalize_label(label)
        for row in report.get("labels", []):
            if row.get("normalized_old_tag") == normalized:
                return {
                    "normalized_old_tag": normalized,
                    "candidates": row.get("candidates", []),
                    "diagnostics": row.get("diagnostics", []),
                    "occurrences": row.get("occurrences", 0),
                    "sent_ids": row.get("sent_ids", []),
                }
        return None

    def export_v1(
        self,
        shards: list[str | Path],
        *,
        config_path: str | Path,
        output_path: str | Path,
    ) -> dict[str, Any]:
        config = load_governance_config(config_path)
        paths = canonical_shards(shards)
        payloads = [parse_shard(path, config=config) for path in paths]
        rows = export_v1_records(payloads)
        target = Path(output_path).expanduser().resolve()
        data = write_jsonl(target, rows)
        return {
            "ok": True,
            "output": str(target),
            "records": len(rows),
            "digest": sha256_bytes(data),
        }

    def clear_stop(self) -> None:
        self.stop_path.unlink(missing_ok=True)

