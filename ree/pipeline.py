from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .config import REEConfig, load_config
from .errors import (
    REEError,
    invalid_stage,
    missing_artifact,
    stale_artifact,
    unsupported_version,
)
from .estimator import estimate_counts
from .extractor import extract_documents
from .formats import DecodedFormat, decode_json_intervals, decode_legacy_flat_v1
from .inputs import InputCatalog, read_jsonl
from .models import CURRENT_SCHEMA_VERSION, DocumentIR
from .storage import (
    STAGES,
    ArtifactStore,
    CheckpointManager,
    CheckpointRecord,
    InvalidationStore,
    Workspace,
    fingerprint,
    hash_files,
    resolve_workspace_path,
)
from .validation import validate_roundtrips


class Pipeline:
    def __init__(
        self,
        root: str | Path,
        *,
        config_path: str = "config/extraction.yaml",
        runtime_dir: str = ".ree",
        catalog_path: str = "data/manifest.json",
    ) -> None:
        self.workspace = Workspace(root, runtime_dir=runtime_dir)
        self.root = self.workspace.root
        self.config_path = resolve_workspace_path(self.root, config_path)
        self.artifacts = ArtifactStore(self.workspace)
        self.checkpoints = CheckpointManager(self.workspace, self.artifacts)
        self.invalidations = InvalidationStore(self.workspace)
        self.catalog = InputCatalog(self.workspace, catalog_path)

    def load_config(self) -> REEConfig:
        return load_config(self.config_path)

    def _component_paths(self) -> dict[str, list[str]]:
        payload = self.catalog.load(create=True)
        return {kind: payload.get(kind, []) for kind in ("documents", "seeds", "legacy")}

    def _expected_components(
        self,
        stage: str,
        records: dict[str, CheckpointRecord],
        config: REEConfig,
    ) -> dict[str, Any] | None:
        paths = self._component_paths()
        if stage == "parse":
            components = {
                "documents": hash_files(self.root, paths["documents"]),
                "seeds": hash_files(self.root, paths["seeds"]),
                "legacy": hash_files(self.root, paths["legacy"]),
                "supported_schema_version": CURRENT_SCHEMA_VERSION,
                **config.stage_components("parse"),
            }
            return components
        dependency = STAGES[STAGES.index(stage) - 1]
        dependency_record = records.get(dependency)
        if dependency_record is None:
            return None
        components = {
            f"{dependency}_artifact": dependency_record.artifact_digest,
            **config.stage_components(stage),
        }
        return components

    @staticmethod
    def _changed_components(
        old: dict[str, Any], new: dict[str, Any] | None
    ) -> list[str]:
        if new is None:
            return []
        return sorted(
            key
            for key in set(old) | set(new)
            if old.get(key) != new.get(key)
        )

    def _artifact_status(
        self, record: CheckpointRecord
    ) -> tuple[bool, str | None]:
        path = resolve_workspace_path(self.root, record.artifact_path)
        if not path.exists():
            return False, "checkpoint artifact file is missing"
        import hashlib

        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != record.artifact_digest:
            return False, "checkpoint artifact digest does not match"
        return True, None

    def evaluate(self, *, persist_invalidations: bool = True) -> dict[str, Any]:
        config = self.load_config()
        records = self.checkpoints.latest_by_stage()
        statuses: dict[str, Any] = {}
        first_unfresh: str | None = None
        first_unfresh_reason: str | None = None
        upstream_unfresh: str | None = None

        for stage in STAGES:
            record = records.get(stage)
            expected_components = self._expected_components(stage, records, config)
            expected_digest = (
                fingerprint(expected_components)
                if expected_components is not None
                else None
            )
            state = "missing"
            reason = "stage has not produced an artifact"
            artifact_ok = False
            if record is None:
                reason = "stage checkpoint is missing"
            else:
                artifact_ok, artifact_reason = self._artifact_status(record)
                if upstream_unfresh is not None:
                    state = "stale"
                    reason = f"upstream stage {upstream_unfresh} is not fresh"
                elif not artifact_ok:
                    state = "stale"
                    reason = artifact_reason or "artifact is unavailable"
                elif expected_components is None:
                    state = "missing"
                    reason = "required upstream artifact is missing"
                elif record.input_digest != expected_digest:
                    state = "stale"
                    changed = self._changed_components(
                        record.input_components, expected_components
                    )
                    reason = (
                        "input components changed: " + ", ".join(changed)
                        if changed
                        else "stage version fingerprint changed"
                    )
                else:
                    state = "fresh"
                    reason = "fingerprint and artifact match"

            statuses[stage] = {
                "state": state,
                "reason": reason,
                "checkpoint_seq": record.seq if record else None,
                "artifact_path": record.artifact_path if record else None,
                "artifact_digest": record.artifact_digest if record else None,
                "fingerprint": record.input_digest if record else None,
                "expected_fingerprint": expected_digest,
                "checkpoint_config_version": record.config_version if record else None,
                "artifact_available": artifact_ok,
                "input_components": record.input_components if record else None,
                "expected_input_components": expected_components,
            }
            if state != "fresh":
                if first_unfresh is None:
                    first_unfresh = stage
                    first_unfresh_reason = reason
                upstream_unfresh = first_unfresh

        last_checkpoint = self.checkpoints.latest()
        invalidations = self.invalidations.list()
        if (
            persist_invalidations
            and first_unfresh is not None
            and records
            and statuses[first_unfresh]["checkpoint_seq"] is not None
        ):
            affected = list(STAGES[STAGES.index(first_unfresh) :])
            earliest_record = records[first_unfresh]
            old_fingerprints = {
                stage: (
                    records[stage].input_digest if stage in records else None
                )
                for stage in affected
            }
            new_fingerprints = {
                stage: statuses[stage]["expected_fingerprint"] for stage in affected
            }
            changed = self._changed_components(
                earliest_record.input_components,
                statuses[first_unfresh]["expected_input_components"],
            )
            payload = {
                "schema_version": 1,
                "earliest_stage": first_unfresh,
                "affected_stages": affected,
                "reason": first_unfresh_reason,
                "changed_components": changed,
                "old_fingerprints": old_fingerprints,
                "new_fingerprints": new_fingerprints,
                "last_checkpoint_seq": last_checkpoint.seq if last_checkpoint else 0,
            }
            self.invalidations.record(payload)
            invalidations = self.invalidations.list()

        return {
            "workspace": ".",
            "config_version": config.digest(),
            "stages": statuses,
            "last_checkpoint_seq": last_checkpoint.seq if last_checkpoint else 0,
            "stop_requested": self.workspace.stop_requested(),
            "last_run": self.workspace.read_run_state(),
            "invalidations": invalidations,
            "all_fresh": all(
                statuses[stage]["state"] == "fresh" for stage in STAGES
            ),
        }

    def status(self) -> dict[str, Any]:
        return self.evaluate(persist_invalidations=True)

    def _load_dependency(self, stage: str, records: dict[str, CheckpointRecord]) -> Any:
        dependency = STAGES[STAGES.index(stage) - 1]
        record = records.get(dependency)
        if record is None:
            raise missing_artifact(
                stage, f"missing prerequisite checkpoint for stage {dependency}"
            )
        return self.artifacts.load(record.artifact_path, record.artifact_digest)

    def _document_records(
        self,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, str], list[dict[str, Any]]]:
        paths = self._component_paths()
        documents: dict[str, dict[str, Any]] = {}
        source_files: dict[str, str] = {}
        raw_records: list[dict[str, Any]] = []
        for relative in sorted(paths["documents"] + paths["legacy"]):
            path = resolve_workspace_path(self.root, relative)
            for record in read_jsonl(path):
                doc_id = record.get("doc_id")
                if doc_id in documents:
                    raise REEError(
                        "E_DUPLICATE_DOCUMENT",
                        "parse",
                        f"duplicate doc_id: {doc_id}",
                        doc_id=str(doc_id) if doc_id else None,
                    )
                documents[str(doc_id)] = record
                source_files[str(doc_id)] = relative
                raw_records.append(record)
        return documents, source_files, raw_records

    @staticmethod
    def _record_version(record: dict[str, Any], *, default: int = 2) -> int:
        value = record.get("schema_version", record.get("annotation_version", default))
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise REEError(
                "E_INVALID_FORMAT",
                "parse",
                f"invalid annotation schema version: {value}",
                doc_id=str(record.get("doc_id", "")) or None,
            ) from exc

    @staticmethod
    def _embedded_annotation(record: dict[str, Any]) -> dict[str, Any] | None:
        annotation = record.get("annotation")
        if isinstance(annotation, dict):
            payload = dict(annotation)
            for key in ("doc_id", "tokens", "text", "source"):
                if key in record and key not in payload:
                    payload[key] = record[key]
            return payload
        if any(key in record for key in ("entities", "relations", "events")):
            return dict(record)
        return None

    @staticmethod
    def _diagnostic(
        payload: dict[str, Any], *, source_file: str | None = None
    ) -> dict[str, Any]:
        result = dict(payload)
        if source_file is not None:
            result.setdefault("source_file", source_file)
        return result

    def _parse_stage(
        self, config: REEConfig, *, stop_requested: Callable[[], bool]
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        documents_by_id, source_files, _ = self._document_records()
        documents: dict[str, DocumentIR] = {}
        seed_by_id: dict[str, DocumentIR] = {}
        diagnostics: list[dict[str, Any]] = []
        source_versions: dict[str, int] = {}

        def decode_annotation(
            payload: dict[str, Any], version: int, doc_id: str
        ) -> DecodedFormat:
            if version > CURRENT_SCHEMA_VERSION:
                raise unsupported_version(
                    "parse", version, CURRENT_SCHEMA_VERSION, doc_id=doc_id
                )
            if version < 1:
                raise unsupported_version(
                    "parse", version, CURRENT_SCHEMA_VERSION, doc_id=doc_id
                )
            if version == 1:
                return decode_legacy_flat_v1(payload, config)
            return decode_json_intervals(payload, config)

        for doc_id in sorted(documents_by_id):
            record = documents_by_id[doc_id]
            annotation = self._embedded_annotation(record)
            version = self._record_version(
                annotation if annotation is not None else record,
                default=self._record_version(record),
            )
            if version > CURRENT_SCHEMA_VERSION or version < 1:
                raise unsupported_version(
                    "parse", version, CURRENT_SCHEMA_VERSION, doc_id=doc_id
                )
            source_versions[doc_id] = version
            base = DocumentIR(
                doc_id=doc_id,
                tokens=list(record["tokens"]),
                text=str(record["text"]),
                source=str(record.get("source", "unknown")),
                provenance={
                    "source_annotation_version": version,
                    "target_schema_version": CURRENT_SCHEMA_VERSION,
                    "source_file": source_files[doc_id],
                },
            )
            if annotation is not None:
                decoded = decode_annotation(annotation, version, doc_id)
                base = decoded.document.model_copy(
                    update={
                        "provenance": {
                            **decoded.document.provenance,
                            "source_file": source_files[doc_id],
                        }
                    }
                )
                for diagnostic in decoded.diagnostics:
                    retained = self._diagnostic(
                        diagnostic, source_file=source_files[doc_id]
                    )
                    retained["stage"] = "parse"
                    diagnostics.append(retained)
                seed_by_id[doc_id] = base
            documents[doc_id] = base
            if stop_requested():
                return None, {
                    "status": "stopped",
                    "stage": "parse",
                    "doc_id": doc_id,
                }

        for relative in sorted(self._component_paths()["seeds"]):
            path = resolve_workspace_path(self.root, relative)
            for record in read_jsonl(path):
                doc_id = str(record.get("doc_id", ""))
                if not doc_id:
                    raise REEError(
                        "E_INVALID_JSONL",
                        "parse",
                        f"seed record in {relative} has no doc_id",
                    )
                payload = dict(record)
                annotation = self._embedded_annotation(record)
                version = self._record_version(
                    annotation if annotation is not None and "annotation" in record else record,
                    default=self._record_version(record),
                )
                if version > CURRENT_SCHEMA_VERSION or version < 1:
                    raise unsupported_version(
                        "parse", version, CURRENT_SCHEMA_VERSION, doc_id=doc_id
                    )
                if annotation is not None and "annotation" in record:
                    payload = annotation
                base_record = documents_by_id.get(doc_id)
                if base_record is not None:
                    for key in ("tokens", "text", "source"):
                        if key not in payload:
                            payload[key] = base_record[key]
                if "tokens" not in payload or "text" not in payload:
                    diagnostics.append(
                        {
                            "code": "E_MISSING_DOCUMENT",
                            "stage": "parse",
                            "doc_id": doc_id,
                            "message": "seed annotation has no matching document",
                            "source_file": relative,
                        }
                    )
                    continue
                decoded = decode_annotation(payload, version, doc_id)
                decoded_document = decoded.document.model_copy(
                    update={
                        "provenance": {
                            **decoded.document.provenance,
                            "source_file": relative,
                            "source_annotation_version": version,
                            "target_schema_version": CURRENT_SCHEMA_VERSION,
                        }
                    }
                )
                seed_by_id[doc_id] = decoded_document
                source_versions[doc_id] = version
                for diagnostic in decoded.diagnostics:
                    retained = self._diagnostic(
                        diagnostic, source_file=relative
                    )
                    retained["stage"] = "parse"
                    diagnostics.append(retained)
                if doc_id in documents and version == 1:
                    documents[doc_id] = documents[doc_id].model_copy(
                        update={
                            "provenance": {
                                **documents[doc_id].provenance,
                                "source_annotation_version": 1,
                                "upgrades": decoded.upgrades,
                            }
                        }
                    )
                if stop_requested():
                    return None, {
                        "status": "stopped",
                        "stage": "parse",
                        "doc_id": doc_id,
                    }

        diagnostics.sort(
            key=lambda item: (
                str(item.get("doc_id", "")),
                str(item.get("code", "")),
                int(item.get("offset", -1)),
                str(item.get("message", "")),
            )
        )
        diagnostic_counts: dict[str, int] = {}
        for item in diagnostics:
            code = str(item.get("code", "UNKNOWN"))
            diagnostic_counts[code] = diagnostic_counts.get(code, 0) + 1
        artifact = {
            "schema_version": 2,
            "documents": [
                documents[doc_id].model_dump(mode="json")
                for doc_id in sorted(documents)
            ],
            "seed_documents": [
                seed_by_id[doc_id].model_dump(mode="json")
                for doc_id in sorted(seed_by_id)
            ],
            "diagnostics": diagnostics,
            "summary": {
                "document_count": len(documents),
                "seed_document_count": len(seed_by_id),
                "diagnostic_count": len(diagnostics),
                "diagnostic_codes": dict(sorted(diagnostic_counts.items())),
                "source_versions": dict(sorted(source_versions.items())),
            },
        }
        return artifact, None

    def _execute_stage(
        self,
        stage: str,
        config: REEConfig,
        *,
        stop_requested: Callable[[], bool],
    ) -> tuple[CheckpointRecord | None, dict[str, Any] | None]:
        records = self.checkpoints.latest_by_stage()
        expected_components = self._expected_components(stage, records, config)
        if expected_components is None:
            raise missing_artifact(
                stage, f"missing prerequisite artifact for stage {stage}"
            )
        expected_seq = self.checkpoints.next_seq()
        artifact: dict[str, Any] | None = None
        stopped: dict[str, Any] | None = None
        if stage == "parse":
            artifact, stopped = self._parse_stage(
                config, stop_requested=stop_requested
            )
        elif stage == "estimate":
            parse_document = self._load_dependency("estimate", records)
            seed_documents = [
                DocumentIR.model_validate(item)
                for item in parse_document.get("seed_documents", [])
            ]
            artifact = estimate_counts(seed_documents, config)
        elif stage == "extract":
            parse_record = records.get("parse")
            estimate_record = records.get("estimate")
            if parse_record is None:
                raise missing_artifact("extract", "missing parse artifact")
            if estimate_record is None:
                raise missing_artifact("extract", "missing estimate artifact")
            parse_document = self.artifacts.load(
                parse_record.artifact_path, parse_record.artifact_digest
            )
            estimate_document = self.artifacts.load(
                estimate_record.artifact_path, estimate_record.artifact_digest
            )
            documents = [
                DocumentIR.model_validate(item)
                for item in parse_document.get("documents", [])
            ]
            artifact, stopped = extract_documents(
                documents,
                estimate_document,
                config,
                document_hook=lambda _doc_id: stop_requested(),
            )
        elif stage == "validate":
            extract_record = records.get("extract")
            if extract_record is None:
                raise missing_artifact("validate", "missing extract artifact")
            extract_document = self.artifacts.load(
                extract_record.artifact_path, extract_record.artifact_digest
            )
            documents = [
                DocumentIR.model_validate(item)
                for item in extract_document.get("documents", [])
            ]
            artifact, stopped = validate_roundtrips(
                documents,
                config,
                document_hook=lambda _doc_id: stop_requested(),
            )
        else:
            raise invalid_stage(stage)

        if stopped is not None:
            return None, stopped
        if artifact is None:
            raise REEError(
                "E_INTERNAL",
                stage,
                "stage finished without an artifact",
            )
        artifact_digest, artifact_path, _ = self.artifacts.save(stage, artifact)
        record = self.checkpoints.publish(
            stage=stage,
            expected_seq=expected_seq,
            input_digest=fingerprint(expected_components),
            config_version=config.digest(),
            artifact_digest=artifact_digest,
            artifact_path=artifact_path,
            input_components=expected_components,
        )
        return record, None

    def _write_stop_state(self, stopped: dict[str, Any]) -> None:
        self.workspace.write_run_state(
            {
                "status": "stopped",
                "stage": stopped.get("stage"),
                "doc_id": stopped.get("doc_id"),
                "reason": stopped.get("reason", "stop_requested"),
                "last_checkpoint_seq": self.checkpoints.latest().seq
                if self.checkpoints.latest()
                else 0,
            }
        )

    def run(
        self,
        *,
        stop_after_stage: str | None = None,
    ) -> dict[str, Any]:
        if stop_after_stage is not None and stop_after_stage not in STAGES:
            raise invalid_stage(stop_after_stage)
        config = self.load_config()
        self.evaluate(persist_invalidations=True)
        for stage in STAGES:
            state = self.evaluate(persist_invalidations=False)
            status = state["stages"][stage]
            if status["state"] == "fresh":
                continue
            if self.workspace.stop_requested():
                stopped = {
                    "status": "stopped",
                    "stage": stage,
                    "reason": "stop_requested",
                }
                self._write_stop_state(stopped)
                return {**stopped, "status_snapshot": self.status()}
            record, stopped = self._execute_stage(
                stage, config, stop_requested=self.workspace.stop_requested
            )
            if stopped is not None:
                stopped["reason"] = stopped.get("reason", "stop_requested")
                self._write_stop_state(stopped)
                return {**stopped, "status_snapshot": self.status()}
            if stop_after_stage == stage:
                stopped = {
                    "status": "stopped",
                    "stage": stage,
                    "reason": "stop_after_stage",
                    "checkpoint_seq": record.seq if record else None,
                }
                self._write_stop_state(stopped)
                return {**stopped, "status_snapshot": self.status()}
            if self.workspace.stop_requested():
                stopped = {
                    "status": "stopped",
                    "stage": stage,
                    "reason": "stop_requested",
                    "checkpoint_seq": record.seq if record else None,
                }
                self._write_stop_state(stopped)
                return {**stopped, "status_snapshot": self.status()}

        final_state = self.status()
        extract_record = self.checkpoints.latest_for_stage("extract")
        validate_record = self.checkpoints.latest_for_stage("validate")
        extract_artifact = (
            self.artifacts.load(
                extract_record.artifact_path, extract_record.artifact_digest
            )
            if extract_record
            else None
        )
        validate_artifact = (
            self.artifacts.load(
                validate_record.artifact_path, validate_record.artifact_digest
            )
            if validate_record
            else None
        )
        result = {
            "status": "complete",
            "last_checkpoint_seq": final_state["last_checkpoint_seq"],
            "summary": extract_artifact.get("summary") if extract_artifact else None,
            "validation_summary": validate_artifact.get("summary")
            if validate_artifact
            else None,
            "status_snapshot": final_state,
        }
        self.workspace.write_run_state(
            {
                "status": "complete",
                "stage": "validate",
                "last_checkpoint_seq": final_state["last_checkpoint_seq"],
            }
        )
        return result

    def resume(self, *, stop_after_stage: str | None = None) -> dict[str, Any]:
        self.workspace.clear_stop()
        return self.run(stop_after_stage=stop_after_stage)

    def request_stop(self) -> dict[str, Any]:
        return self.workspace.request_stop()

    def run_stage(self, stage: str) -> dict[str, Any]:
        if stage not in STAGES:
            raise invalid_stage(stage)
        config = self.load_config()
        state = self.evaluate(persist_invalidations=True)
        stage_index = STAGES.index(stage)
        for prerequisite in STAGES[:stage_index]:
            prerequisite_state = state["stages"][prerequisite]
            if prerequisite_state["state"] == "missing":
                raise missing_artifact(
                    stage,
                    f"cannot run stage {stage}: prerequisite {prerequisite} is missing",
                )
            if prerequisite_state["state"] == "stale":
                raise stale_artifact(
                    stage,
                    f"cannot run stage {stage}: prerequisite {prerequisite} is stale",
                )
        if state["stages"][stage]["state"] == "fresh":
            return {
                "status": "fresh",
                "stage": stage,
                "status_snapshot": state,
            }
        record, stopped = self._execute_stage(
            stage, config, stop_requested=self.workspace.stop_requested
        )
        if stopped is not None:
            self._write_stop_state(stopped)
            return {**stopped, "status_snapshot": self.status()}
        return {
            "status": "complete",
            "stage": stage,
            "checkpoint_seq": record.seq if record else None,
            "status_snapshot": self.status(),
        }

    def get_stage_artifact(self, stage: str) -> dict[str, Any]:
        if stage not in STAGES:
            raise invalid_stage(stage)
        state = self.evaluate(persist_invalidations=True)
        stage_state = state["stages"][stage]
        if stage_state["state"] == "missing":
            raise missing_artifact(
                stage,
                f"artifact for stage {stage} is missing: {stage_state['reason']}",
            )
        if stage_state["state"] == "stale":
            raise stale_artifact(
                stage,
                f"artifact for stage {stage} is stale: {stage_state['reason']}",
            )
        record = self.checkpoints.latest_for_stage(stage)
        if record is None:
            raise missing_artifact(stage, f"artifact for stage {stage} is missing")
        return self.artifacts.load(record.artifact_path, record.artifact_digest)

    def get_documents(self) -> list[DocumentIR]:
        extract = self.get_stage_artifact("extract")
        return [DocumentIR.model_validate(item) for item in extract.get("documents", [])]

    def get_document(self, doc_id: str) -> DocumentIR:
        for document in self.get_documents():
            if document.doc_id == doc_id:
                return document
        raise REEError(
            "E_DOCUMENT_NOT_FOUND",
            "query",
            f"unknown extracted document: {doc_id}",
            doc_id=doc_id,
        )
