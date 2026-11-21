"""Application service: import pipeline, snapshots, increments and diagnostics."""
from __future__ import annotations

import json
from pathlib import Path

from app import graph
from app.codecs import registry
from app.compat import negotiate
from app.errors import ErrorCode, WorkbenchError
from app.models import (
    DecodedPayload,
    FormatName,
    ImportReport,
    IncrementFragment,
    IncrementResult,
    LayerBatch,
    NegotiationDecision,
    NormalizedResource,
    RawRecord,
    RoundingRule,
    SnapshotHeader,
)
from app.normalize import normalize_records
from app.store import Store, utc_now


class OrchestrationService:
    def __init__(self, store: Store):
        self.store = store

    # ---------------------------------------------------------------- import
    def import_snapshot(
        self, data: bytes, rounding_rule: RoundingRule = RoundingRule.HALF_EVEN
    ) -> ImportReport:
        with self.store.writer_lock:
            payload = registry.decode(data)
            negotiation_report = negotiate(payload)
            normalization = normalize_records(payload.records, rounding_rule)

            active_seq = self.store.active_snapshot_seq()
            if active_seq is not None and payload.header.snapshot_seq <= active_seq:
                raise WorkbenchError(
                    ErrorCode.E_SNAPSHOT_INVALID,
                    f"snapshot_seq {payload.header.snapshot_seq} must be greater than active {active_seq}",
                    {"active_snapshot_seq": active_seq, "incoming": payload.header.snapshot_seq},
                )
            if normalization.rejected:
                raise WorkbenchError(
                    ErrorCode.E_SNAPSHOT_INVALID,
                    "one or more records failed normalization; whole batch rejected",
                    {"rejected": [entry.model_dump() for entry in normalization.rejected]},
                )

            intent_id, memoized = self.store.begin_intent(
                kind="import",
                submit_id=None,
                base_snapshot_seq=active_seq,
                detail=f"import {payload.header.format.value} seq={payload.header.snapshot_seq}",
            )
            try:
                graph_result = graph.build_graph(normalization.resources)
                self.store.commit_snapshot(
                    snapshot_seq=payload.header.snapshot_seq,
                    format_name=payload.header.format.value,
                    format_version=payload.header.format_version,
                    generated_at=payload.header.generated_at,
                    resources=graph_result.resources,
                    batches=graph_result.batches,
                )
                report = ImportReport(
                    snapshot_seq=payload.header.snapshot_seq,
                    format=payload.header.format,
                    format_version=payload.header.format_version,
                    negotiation=negotiation_report,
                    accepted=len(graph_result.resources),
                    rejected=[],
                    rounding=normalization.rounding,
                    cumulative_rounding_error=normalization.cumulative_error,
                    layer_count=len(graph_result.layers),
                    batch_count=len(graph_result.batches),
                )
                self.store.finish_intent(intent_id, "committed", report.model_dump_json())
                return report
            except Exception:
                self.store.finish_intent(intent_id, "aborted", None)
                raise

    def dry_run(self, data: bytes, rounding_rule: RoundingRule = RoundingRule.HALF_EVEN) -> dict:
        """Decode/negotiate/normalize/build without touching persisted state."""
        payload = registry.decode(data)
        negotiation_report = negotiate(payload)
        normalization = normalize_records(payload.records, rounding_rule)
        if normalization.rejected:
            raise WorkbenchError(
                ErrorCode.E_SNAPSHOT_INVALID,
                "one or more records failed normalization",
                {"rejected": [entry.model_dump() for entry in normalization.rejected]},
            )
        graph_result = graph.build_graph(normalization.resources)
        return {
            "ok": True,
            "negotiation": negotiation_report.model_dump(),
            "resource_count": len(graph_result.resources),
            "edge_count": graph_result.edge_count,
            "layer_count": len(graph_result.layers),
            "batch_count": len(graph_result.batches),
            "layers": graph_result.layers,
            "rounding": [info.model_dump() for info in normalization.rounding],
            "cumulative_rounding_error": normalization.cumulative_error,
        }

    # ---------------------------------------------------------------- export
    def export_snapshot(
        self, target_format: FormatName, snapshot_seq: int | None = None
    ) -> bytes:
        with self.store.writer_lock:
            seq = snapshot_seq if snapshot_seq is not None else self.store.active_snapshot_seq()
            if seq is None:
                raise WorkbenchError(ErrorCode.E_NOT_FOUND, "no committed snapshot to export")
            stats, resources, _ = self.store.load_snapshot(seq)
            records = [
                RawRecord(
                    resource_id=resource.resource_id,
                    parents=list(resource.parents),
                    budget=resource.budget,
                    weight_permille=resource.weight_permille,
                    weight_float=resource.weight_permille / 1000.0,
                    expected_version=resource.expected_version,
                    present_fields=["budget", "weight", "expected_version", "parents"],
                )
                for resource in resources
            ]
            header = SnapshotHeader(
                format=target_format,
                format_version=1,
                snapshot_seq=seq,
                generated_at=stats.generated_at,
            )
            return registry.encode(DecodedPayload(header=header, records=records))

    # ------------------------------------------------------------- diagnostics
    def diagnostics(self) -> dict:
        stats, resources, batches = self.store.load_active()
        if stats is None:
            raise WorkbenchError(ErrorCode.E_NOT_FOUND, "no active snapshot")
        return {
            "ok": True,
            "stats": stats.model_dump(),
            "resources": [resource.model_dump() for resource in resources],
            "batches": [batch.model_dump() for batch in batches],
        }

    def impact(self, resource_id: str) -> dict:
        stats, resources, _ = self.store.load_active()
        if stats is None:
            raise WorkbenchError(ErrorCode.E_NOT_FOUND, "no active snapshot")
        if not any(resource.resource_id == resource_id for resource in resources):
            raise WorkbenchError(ErrorCode.E_NOT_FOUND, f"unknown resource {resource_id}")
        downstream = sorted(graph.downstream_closure(resources, [resource_id]))
        by_id = {resource.resource_id: resource for resource in resources}
        affected_layers = sorted({by_id[item].layer for item in downstream})
        affected_batches = sorted({by_id[item].batch for item in downstream})
        return {
            "ok": True,
            "changed_resource": resource_id,
            "impact_downstream": downstream,
            "affected_layers": affected_layers,
            "affected_batches": affected_batches,
            "base_snapshot_seq": stats.snapshot_seq,
        }

    # ------------------------------------------------------------- increments
    def apply_increment(self, fragment: IncrementFragment) -> IncrementResult:
        with self.store.writer_lock:
            active_seq = self.store.active_snapshot_seq()
            memoized = self.store.memoized_result(fragment.submit_id)
            if memoized:
                return IncrementResult(**json.loads(memoized["result_json"]))
            if active_seq is None:
                raise WorkbenchError(ErrorCode.E_NOT_FOUND, "no base snapshot available")
            if fragment.base_snapshot_seq != active_seq:
                raise WorkbenchError(
                    ErrorCode.E_STALE_BASE,
                    f"base {fragment.base_snapshot_seq} is not current {active_seq}",
                    {"base_snapshot_seq": fragment.base_snapshot_seq, "current": active_seq},
                )

            intent_id, memoized_intent = self.store.begin_intent(
                kind="increment",
                submit_id=fragment.submit_id,
                base_snapshot_seq=active_seq,
                detail=f"increment {fragment.submit_id}",
            )
            if memoized_intent:
                return IncrementResult(**json.loads(memoized_intent["result_json"]))

            try:
                _, current_resources, _ = self.store.load_snapshot(active_seq)
                by_id = {resource.resource_id: resource for resource in current_resources}
                removed = set(fragment.removed_ids)
                for resource_id in removed:
                    by_id.pop(resource_id, None)

                normalization = normalize_records(fragment.changes, fragment.rounding_rule)
                if normalization.rejected:
                    raise WorkbenchError(
                        ErrorCode.E_SNAPSHOT_INVALID,
                        "increment contains records that failed normalization",
                        {"rejected": [entry.model_dump() for entry in normalization.rejected]},
                    )
                changed_ids: set[str] = set()
                for resource in normalization.resources:
                    changed_ids.add(resource.resource_id)
                    by_id[resource.resource_id] = resource

                merged = list(by_id.values())
                downstream = graph.downstream_closure(merged, sorted(changed_ids))
                affected = changed_ids | downstream
                graph_result = graph.build_graph(merged)

                new_seq = active_seq + 1
                self.store.commit_snapshot(
                    snapshot_seq=new_seq,
                    format_name="increment",
                    format_version=1,
                    generated_at=self.store.load_snapshot(active_seq)[0].generated_at,
                    resources=graph_result.resources,
                    batches=graph_result.batches,
                )
                layer_by_id = {resource.resource_id: resource.layer for resource in graph_result.resources}
                batch_by_id = {resource.resource_id: resource.batch for resource in graph_result.resources}
                result = IncrementResult(
                    submit_id=fragment.submit_id,
                    base_snapshot_seq=active_seq,
                    new_snapshot_seq=new_seq,
                    affected_resources=sorted(affected),
                    affected_layers=sorted({layer_by_id[item] for item in affected if item in layer_by_id}),
                    affected_batches=sorted({batch_by_id[item] for item in affected if item in batch_by_id}),
                    impact_downstream=sorted(downstream),
                )
                self.store.finish_intent(intent_id, "committed", result.model_dump_json())
                return result
            except Exception:
                self.store.finish_intent(intent_id, "aborted", None)
                raise

    def rebuild_layers(self, submit_id: str) -> dict:
        with self.store.writer_lock:
            memoized = self.store.memoized_result(submit_id)
            if memoized:
                return json.loads(memoized["result_json"])
            active_seq = self.store.active_snapshot_seq()
            if active_seq is None:
                raise WorkbenchError(ErrorCode.E_NOT_FOUND, "no snapshot to rebuild")
            intent_id, memoized_intent = self.store.begin_intent(
                kind="rebuild", submit_id=submit_id, base_snapshot_seq=active_seq, detail=f"rebuild {submit_id}"
            )
            if memoized_intent:
                return json.loads(memoized_intent["result_json"])
            try:
                stats, resources, _ = self.store.load_snapshot(active_seq)
                rebuilt = graph.build_graph(resources)
                new_seq = active_seq + 1
                self.store.commit_snapshot(
                    snapshot_seq=new_seq,
                    format_name="rebuild",
                    format_version=1,
                    generated_at=stats.generated_at,
                    resources=rebuilt.resources,
                    batches=rebuilt.batches,
                )
                result = {
                    "ok": True,
                    "submit_id": submit_id,
                    "base_snapshot_seq": active_seq,
                    "new_snapshot_seq": new_seq,
                    "layer_count": len(rebuilt.layers),
                    "batch_count": len(rebuilt.batches),
                }
                self.store.finish_intent(intent_id, "committed", json.dumps(result))
                return result
            except Exception:
                self.store.finish_intent(intent_id, "aborted", None)
                raise

    def snapshots(self) -> dict:
        return {"ok": True, "snapshots": [stats.model_dump() for stats in self.store.list_snapshots()]}

    def recovery_report(self) -> dict:
        with self.store.writer_lock:
            row = self.store._conn.execute(
                "SELECT value FROM meta WHERE key='last_recovery'"
            ).fetchone()
            if row is not None:
                return json.loads(row["value"])
            return self.store.recover().model_dump()

    def crash_during_import(self, data: bytes, crash_marker_path: str) -> None:
        """Test/demo hook: write intent + partial rows then hard-exit.

        The process is killed before COMMIT, so SQLite discards the open
        transaction on restart while the pending intent survives for recovery.
        """
        with self.store.writer_lock:
            payload = registry.decode(data)
            negotiate(payload)
            normalization = normalize_records(payload.records, RoundingRule.HALF_EVEN)
            graph_result = graph.build_graph(normalization.resources)
            active_seq = self.store.active_snapshot_seq()
            intent_id, _ = self.store.begin_intent(
                kind="import",
                submit_id=None,
                base_snapshot_seq=active_seq,
                detail=f"crashed import seq={payload.header.snapshot_seq}",
            )
            conn = self.store._conn
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO snapshots(snapshot_seq,format,format_version,generated_at,created_at) "
                "VALUES(?,?,?,?,?)",
                (
                    payload.header.snapshot_seq,
                    payload.header.format.value,
                    payload.header.format_version,
                    payload.header.generated_at,
                    utc_now(),
                ),
            )
            resource = graph_result.resources[0]
            conn.execute(
                "INSERT INTO resources(snapshot_seq,resource_id,budget,weight_permille,expected_version,"
                "layer,batch) VALUES(?,?,?,?,?,?,?)",
                (
                    payload.header.snapshot_seq,
                    resource.resource_id,
                    resource.budget,
                    resource.weight_permille,
                    resource.expected_version,
                    resource.layer,
                    resource.batch,
                ),
            )
            Path(crash_marker_path).write_text(intent_id, encoding="utf-8")
            # Hard termination WITHOUT commit: simulates SIGKILL / power loss.
            import os

            os._exit(99)
