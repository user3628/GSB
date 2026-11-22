"""Application service: single-writer commits, idempotency and read queries."""
from __future__ import annotations

import json
import threading
import time

from . import aggregates, cascade, db, rca, recovery, samples as sample_mod, topology as topo_mod
from .config import Settings, get_settings
from .errors import ConflictingBatch, PipelineError, RebuildFailed, StaleBase
from .fixedpoint import to_string


class Pipeline:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        db.initialize(self.settings.db_path)
        self._writer = threading.RLock()
        self._inflight: dict[str, threading.Event] = {}
        self._inflight_guard = threading.Condition()

    # ------------------------------------------------------------------ helpers
    def _connect(self, *, readonly: bool = False):
        return db.connect(self.settings.db_path, readonly=readonly)

    def _register_or_wait(self, commit_id: str, payload_hash: str, current_version, base_version):
        """Idempotency gate, evaluated *after* the base version check.

        Returns a replay dict when an identical in-flight/committed request won
        the race, otherwise None meaning the caller owns the commit.
        """
        with self._inflight_guard:
            event = self._inflight.get(commit_id)
            if event is None:
                self._inflight[commit_id] = threading.Event()
                return None
        # Another writer with the same commit id is active; wait and reconcile.
        event.wait(timeout=30)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT status, payload_hash FROM commits WHERE commit_id=?", (commit_id,)
            ).fetchone()
        if row is None or row["status"] != "committed":
            raise PipelineError("E_SAMPLE_INVALID", "duplicate commit did not commit",
                                {"commit_id": commit_id})
        if row["payload_hash"] != payload_hash:
            raise ConflictingBatch("commit_id reused with a different payload",
                                   {"commit_id": commit_id})
        return ("replay",)

    def _finish_inflight(self, commit_id: str) -> None:
        with self._inflight_guard:
            event = self._inflight.pop(commit_id, None)
        if event is not None:
            event.set()

    # ------------------------------------------------------------------ samples
    def submit_samples(self, payload: dict) -> dict:
        commit_id = payload.get("commit_id")
        if not commit_id:
            raise PipelineError("E_SAMPLE_INVALID", "commit_id is required", {"field": "commit_id"})
        payload_hash = sample_mod.canonical_payload_hash(payload)

        with self._writer:
            with self._connect() as conn:
                known = sample_mod.require_topology(conn)
                kept, duplicate_count = sample_mod.parse_and_dedup(
                    payload.get("records", []), known
                )
                current = sample_mod.current_sample_version(conn)
                base_version = payload.get("base_version")
                # An already-committed commit id is reconciled before any base
                # version check (replay or conflicting payload).
                committed = conn.execute(
                    "SELECT status, payload_hash FROM commits WHERE commit_id=?", (commit_id,)
                ).fetchone()
                if committed and committed["status"] == "committed":
                    if committed["payload_hash"] != payload_hash:
                        raise ConflictingBatch("commit_id already used with a different payload",
                                               {"commit_id": commit_id})
                    batch = conn.execute(
                        "SELECT * FROM sample_batches WHERE commit_id=?", (commit_id,)
                    ).fetchone()
                    return {"replayed": True, "commit_id": commit_id,
                            "new_version": batch["new_version"],
                            "record_count": batch["record_count"],
                            "duplicate_count": batch["duplicate_count"]}

                # In-flight duplicates must observe the base check first so the
                # loser of a concurrent increment race receives E_STALE_BASE.
                inflight = self._inflight.get(commit_id)
                if inflight is not None and (base_version is None or int(base_version) != current):
                    raise StaleBase(
                        "concurrent sample increment already advanced the version",
                        {"base_version": base_version, "current_version": current},
                    )
                if base_version is None or int(base_version) != current:
                    raise StaleBase(
                        "sample batch base_version is not the current sample version",
                        {"base_version": base_version, "current_version": current},
                    )

                gate = self._register_or_wait(commit_id, payload_hash, current, base_version)
                if gate is not None:
                    batch = conn.execute(
                        "SELECT * FROM sample_batches WHERE commit_id=?", (commit_id,)
                    ).fetchone()
                    return {"replayed": True, "commit_id": commit_id,
                            "new_version": batch["new_version"],
                            "record_count": batch["record_count"],
                            "duplicate_count": batch["duplicate_count"]}

            try:
                with self._connect() as conn:
                    # One-shot fault arming is cleared before the write
                    # transaction so a later rollback cannot resurrect it.
                    fault = recovery.consume_fault(conn)
                    # Phase 1: durably persist the intent in its own committed
                    # transaction.  If the process is killed afterwards, restart
                    # recovery observes this 'pending' intent and aborts it.
                    conn.execute("BEGIN IMMEDIATE")
                    intent_id = recovery.write_intent(
                        conn, "sample_batch", commit_id,
                        {"payload_hash": payload_hash, "records": len(kept),
                         "duplicates": duplicate_count},
                    )
                    conn.commit()
                    recovery.maybe_crash(fault)

                    # Phase 2: the data write + atomic cascade rebuild.
                    conn.execute("BEGIN IMMEDIATE")
                    current = sample_mod.current_sample_version(conn)
                    if int(base_version) != current:
                        conn.rollback()
                        raise StaleBase(
                            "sample version advanced concurrently",
                            {"base_version": base_version, "current_version": current},
                        )
                    old_version, new_version = sample_mod.insert_batch(
                        conn, payload, kept, duplicate_count, payload_hash
                    )
                    affected = sample_mod.affected_services(kept)
                    if fault == "fail_rebuild":
                        raise RebuildFailed("injected rebuild failure", {"fault": fault})
                    rebuild = cascade.cascade_rebuild(conn, self.settings, affected)
                    recovery.complete_intent(conn, intent_id, "done")
                    conn.commit()

                return {
                    "replayed": False,
                    "commit_id": commit_id,
                    "base_version": old_version,
                    "new_version": new_version,
                    "record_count": len(kept),
                    "duplicate_count": duplicate_count,
                    "cascade": rebuild,
                }
            except PipelineError:
                # Phase 2 rolled back; reconcile the already-durable intent in
                # its own transaction so no 'pending' record is left dangling.
                with self._connect() as cleanup:
                    cleanup.execute("BEGIN IMMEDIATE")
                    recovery.complete_intent(cleanup, intent_id, "aborted")
                    cleanup.commit()
                raise
            finally:
                self._finish_inflight(commit_id)

    # ---------------------------------------------------------------- topology
    def submit_topology(self, payload: dict) -> dict:
        commit_id = payload.get("commit_id")
        if not commit_id:
            raise PipelineError("E_TOPOLOGY_INVALID", "commit_id is required", {"field": "commit_id"})
        payload_hash = sample_mod.canonical_payload_hash(payload)

        with self._writer:
            with self._connect() as conn:
                current = topo_mod.current_version(conn)
                base_version = payload.get("base_version")
                kind = payload.get("kind")
                # Committed commit id reconciled first (replay / conflict).
                committed = conn.execute(
                    "SELECT status, payload_hash FROM commits WHERE commit_id=?", (commit_id,)
                ).fetchone()
                if committed and committed["status"] == "committed":
                    if committed["payload_hash"] != payload_hash:
                        raise ConflictingBatch("commit_id already used with a different payload",
                                               {"commit_id": commit_id})
                    return {"replayed": True, "commit_id": commit_id,
                            "topology_version": topo_mod.current_version(conn)}

                gate = self._register_or_wait(commit_id, payload_hash, current, base_version)
                if gate is not None:
                    return {"replayed": True, "commit_id": commit_id,
                            "topology_version": topo_mod.current_version(conn)}
                if kind == "increment" and (base_version is None or int(base_version) != current):
                    raise StaleBase(
                        "topology increment base_version is not the current topology version",
                        {"base_version": base_version, "current_version": current},
                    )

            try:
                with self._connect() as conn:
                    fault = recovery.consume_fault(conn)
                    # Phase 1: durable intent, independently committed.
                    conn.execute("BEGIN IMMEDIATE")
                    intent_id = recovery.write_intent(
                        conn, f"topology_{kind}", commit_id, {"payload_hash": payload_hash}
                    )
                    conn.commit()
                    recovery.maybe_crash(fault)

                    # Phase 2: topology change + atomic cascade rebuild.
                    conn.execute("BEGIN IMMEDIATE")
                    prior_services = topo_mod.declared_services(conn)
                    new_version, affected = topo_mod.apply_topology(conn, payload)
                    if kind == "snapshot":
                        # A full replacement may remove services: every prior or
                        # current service must participate in invalidation.
                        affected = set(affected) | prior_services
                    if fault == "fail_rebuild":
                        raise RebuildFailed("injected rebuild failure", {"fault": fault})
                    rebuild = cascade.cascade_rebuild(conn, self.settings, affected)
                    recovery.complete_intent(conn, intent_id, "done")
                    conn.commit()

                return {
                    "replayed": False,
                    "commit_id": commit_id,
                    "topology_version": new_version,
                    "affected_services": sorted(affected),
                    "cascade": rebuild,
                }
            except PipelineError:
                with self._connect() as cleanup:
                    cleanup.execute("BEGIN IMMEDIATE")
                    recovery.complete_intent(cleanup, intent_id, "aborted")
                    cleanup.commit()
                raise
            finally:
                self._finish_inflight(commit_id)

    # ------------------------------------------------------------- alert state
    def transition_alert(self, alert_id: str, to_status: str) -> dict:
        with self._writer:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                result = aggregates.manual_transition(conn, alert_id, to_status)
                alert = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
                rca.rebuild_alert_rca(conn, self.settings, alert)
                conn.commit()
            return result

    # ------------------------------------------------------------------ queries
    def query_alerts(self, status: str | None = None, service: str | None = None) -> dict:
        query = "SELECT * FROM alerts WHERE 1=1"
        params: list = []
        if status:
            query += " AND status=?"
            params.append(status)
        if service:
            query += " AND service=?"
            params.append(service)
        query += " ORDER BY service, metric, rule, id"
        with self._connect(readonly=True) as conn:
            rows = conn.execute(query, params).fetchall()
            sample_hi = sample_mod.current_sample_version(conn)
            topo_version = topo_mod.current_version(conn)
        return {
            "sample_version": sample_hi,
            "topology_version": topo_version,
            "alerts": [dict(row) for row in rows],
        }

    def query_quantiles(self, service: str | None = None, metric: str | None = None) -> dict:
        query = "SELECT * FROM quantile_index WHERE 1=1"
        params: list = []
        if service:
            query += " AND service=?"
            params.append(service)
        if metric:
            query += " AND metric=?"
            params.append(metric)
        query += " ORDER BY service, metric"
        with self._connect(readonly=True) as conn:
            rows = conn.execute(query, params).fetchall()
            sample_hi = sample_mod.current_sample_version(conn)
            if not rows and service is not None:
                count_row = conn.execute(
                    "SELECT COUNT(*) AS c FROM samples WHERE service=? "
                    + ("AND metric=?" if metric else ""),
                    [service, metric] if metric else [service],
                ).fetchone()
                if count_row["c"] > 0:
                    from .errors import InsufficientSamples

                    raise InsufficientSamples(
                        "fewer than the minimum window samples for this service/metric",
                        {"service": service, "metric": metric,
                         "min_samples": self.settings.min_samples, "available": count_row["c"]},
                    )
        entries = []
        for row in rows:
            entry = dict(row)
            entry["quantiles"] = json.loads(entry.pop("quantiles_json"))
            entry["good_rate"] = to_string(entry["good_rate"])
            entry["version_range"] = {"lo": entry.pop("sample_lo"), "hi": entry.pop("sample_hi")}
            entries.append(entry)
        return {
            "sample_version": sample_hi,
            "window_size": self.settings.window_size,
            "min_samples": self.settings.min_samples,
            "quantile_method": "nearest_rank",
            "entries": entries,
        }

    def query_rca(self, alert_id: str) -> dict:
        with self._connect(readonly=True) as conn:
            alert = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
            if alert is None:
                from .errors import NotFound

                raise NotFound("alert not found", {"alert_id": alert_id})
            rows = conn.execute(
                "SELECT * FROM rca_scores WHERE alert_id=? ORDER BY rank, candidate", (alert_id,)
            ).fetchall()
        return {
            "alert": dict(alert),
            "numeric_contract": rca.numeric_contract_note(),
            "candidates": [rca.render_score(row) for row in rows],
        }

    def query_suppression(self) -> dict:
        with self._connect(readonly=True) as conn:
            rows = conn.execute(
                "SELECT * FROM suppression_index ORDER BY service, fingerprint"
            ).fetchall()
        return {"entries": [dict(row) for row in rows]}

    def diagnostic_report(self) -> dict:
        with self._connect(readonly=True) as conn:
            counts = {
                table: conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
                for table in (
                    "samples", "sample_batches", "edges", "services", "quantile_index",
                    "alerts", "suppression_index", "rca_scores", "intents", "recovery_events",
                )
            }
            versions = {
                "sample_version": sample_mod.current_sample_version(conn),
                "topology_version": topo_mod.current_version(conn),
            }
            refs = conn.execute(
                "SELECT service, artifact, COUNT(*) AS c FROM service_refs GROUP BY service, artifact"
            ).fetchall()
        return {
            "generated_at_ms": int(time.time() * 1000),
            "versions": versions,
            "counts": counts,
            "service_refs": [dict(row) for row in refs],
            "numeric_contract": rca.numeric_contract_note(),
        }

    def recovery_report(self) -> dict:
        with self._connect(readonly=True) as conn:
            rows = conn.execute(
                "SELECT at_ms, intent_id, action, detail_json FROM recovery_events ORDER BY id"
            ).fetchall()
            pending = conn.execute(
                "SELECT COUNT(*) AS c FROM intents WHERE status='pending'"
            ).fetchone()["c"]
        events = []
        for row in rows:
            event = dict(row)
            event["detail"] = json.loads(event.pop("detail_json"))
            events.append(event)
        return {"pending_intents": pending, "events": events}

    # ------------------------------------------------------------------ faults
    def inject_fault(self, mode: str | None) -> dict:
        with self._writer:
            with self._connect() as conn:
                recovery.set_fault(conn, mode)
            return {"fault_mode": mode}

    def run_startup_recovery(self) -> dict:
        with self._writer:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                report = recovery.run_recovery(conn, self.settings)
                conn.commit()
            return report
