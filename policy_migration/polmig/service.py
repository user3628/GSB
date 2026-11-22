"""Migration orchestration: single-writer commits, idempotency and recovery."""

import hashlib
import json
import threading
import time

from . import migration
from .codecs import decode_binary, decode_jsonl
from .compatibility import negotiate
from .errors import (
    ConflictingBatch,
    MigrationError,
    MigrationRejected,
    StaleBase,
)
from .model import NewPolicy, parse_legacy_document
from .storage import Store


def now_ms():
    return int(time.time() * 1000)


class MigrationService:
    def __init__(self, db_path, max_parent_depth=4, crash_hook=None):
        self.store = Store(db_path)
        self.max_parent_depth = max_parent_depth
        self.crash_hook = crash_hook
        self._inflight = {}
        self._inflight_lock = threading.Lock()
        self.recovery_report = self.recover_pending_intents()

    def _payload_hash(self, payload):
        return hashlib.sha256(payload).hexdigest()

    def _decode_payload(self, fmt, data):
        if fmt == "binary":
            return decode_binary(data)
        if fmt == "jsonl":
            return decode_jsonl(data)
        if fmt == "legacy-json":
            if isinstance(data, (bytes, bytearray)):
                data = bytes(data).decode("utf-8")
            return parse_legacy_document(json.loads(data))
        raise MigrationRejected("unsupported format %s" % fmt, {"format": fmt})

    def _stored_resolvers(self):
        store = self.store

        def parent_resolver(tenant_id):
            if not store.tenant_exists_stored(tenant_id):
                raise MigrationRejected(
                    "parent chain references missing tenant %s" % tenant_id,
                    {"missing_tenant_id": tenant_id},
                )
            return store.stored_parent_ref(tenant_id)

        def policy_resolver(tenant_id):
            rows = store.active_new_policies(tenant_id)
            return [NewPolicy.from_dict(json.loads(r["payload_json"])) for r in rows]

        return parent_resolver, policy_resolver

    def _reject_decisions(self, decisions, error_code, reason, extra=None):
        for decision in decisions:
            decision["verdict"] = migration.REJECT
            decision["rejection_reasons"] = decision.get("rejection_reasons", []) + [{
                "field": "_batch",
                "reason": reason,
                "code": error_code,
                **(extra or {}),
            }]
        return decisions

    def _build_diff_against(self, policies, baseline):
        entries = []
        for policy in policies:
            before = baseline.get(policy.tenant_id)
            after = policy.to_dict()
            entries.append({
                "tenant_id": policy.tenant_id,
                "action": "ADD" if before is None else "UPDATE",
                "before": before,
                "after": after,
                "changed_fields": None if before is None else
                sorted(k for k in after if after.get(k) != before.get(k)),
            })
        return entries

    def _execute(self, batch_id, base_version, fmt, data, content_hash, request):
        store = self.store
        ts = now_ms()
        with store.write_lock:
            if store.current_version() != base_version:
                raise StaleBase(
                    "base version %d is not current version %d"
                    % (base_version, store.current_version()),
                    {"base_version": base_version,
                     "current_version": store.current_version()},
                )
            store.insert_intent(batch_id, ts, {
                "base_version": base_version, "format": fmt,
                "content_hash": content_hash,
            })
            store.insert_batch({
                "batch_id": batch_id, "base_version": base_version,
                "status": "INTENDED", "source_format": fmt,
                "source_version": None, "content_hash": content_hash,
                "verdict": None, "request": request,
                "created_at_ms": ts, "updated_at_ms": ts,
            })
            store.conn.commit()

            if self.crash_hook == "after_intent":
                import os
                os._exit(137)

            try:
                with store.conn:
                    if store.current_version() != base_version:
                        raise StaleBase(
                            "base version %d became stale before commit" % base_version,
                            {"base_version": base_version,
                             "current_version": store.current_version()},
                        )
                    if fmt == "legacy-json":
                        legacy_policies = self._decode_payload(fmt, data)
                        decisions, policies = migration.build_legacy_decisions(
                            legacy_policies, ts
                        )
                        store.insert_legacy_policies(batch_id, legacy_policies)
                    else:
                        policies = self._decode_payload(fmt, data)
                        decisions = migration.build_new_decisions(policies)

                    if any(d["verdict"] == migration.REJECT for d in decisions):
                        raise MigrationRejected(
                            "batch contains unmappable records",
                            {"batch_id": batch_id,
                             "rejected_tenants": [
                                 d["tenant_id"] for d in decisions
                                 if d["verdict"] == migration.REJECT
                             ]},
                        )

                    batch_by_tenant = {p.tenant_id: p for p in policies}
                    parent_resolver, policy_resolver = self._stored_resolvers()
                    for policy in policies:
                        migration.detect_parent_chain(
                            policy.tenant_id, batch_by_tenant,
                            parent_resolver, self.max_parent_depth
                        )
                    migration.check_lifetime_overlaps(policies, policy_resolver)

                    baseline_version = store.current_version()
                    baseline = {
                        r["tenant_id"]: json.loads(r["payload_json"])
                        for r in store.policies_at_version(baseline_version)
                    }
                    new_version = baseline_version + 1
                    store.insert_new_version(new_version, batch_id, ts, policies)
                    store.insert_decisions(batch_id, decisions)
                    plan_diff = self._build_diff_against(policies, baseline)
                    store.update_batch_status(
                        batch_id, "COMMITTED", now_ms(),
                        committed_version=new_version, verdict="ACCEPTED",
                        source_version=request.get("format_version"),
                    )
                    store.resolve_intent(batch_id, "COMMITTED", now_ms(),
                                         {"committed_version": new_version})

                    if self.crash_hook == "after_commit_inside":
                        import os
                        os._exit(137)

                if self.crash_hook == "after_commit_outside":
                    import os
                    os._exit(137)

                return {
                    "batch_id": batch_id,
                    "status": "COMMITTED",
                    "base_version": base_version,
                    "committed_version": new_version,
                    "decisions": decisions,
                    "plan_diff": plan_diff,
                }
            except MigrationError as exc:
                self._record_failure(
                    batch_id, decisions if "decisions" in locals() else [], exc
                )
                raise
            except Exception:
                self._record_failure(
                    batch_id, decisions if "decisions" in locals() else [],
                    MigrationError("unexpected migration failure"),
                )
                raise

    def _record_failure(self, batch_id, decisions, exc):
        store = self.store
        now = now_ms()
        with store.conn:
            if decisions:
                decisions = self._reject_decisions(
                    decisions, exc.code, exc.message,
                    exc.details if isinstance(exc.details, dict) else None,
                )
                store.insert_decisions(batch_id, decisions)
            store.update_batch_status(
                batch_id, "REJECTED", now, verdict="REJECTED",
                error_code=exc.code, error_message=exc.message,
            )
            store.resolve_intent(batch_id, "ABORTED", now, {"error_code": exc.code})

    def submit_batch(self, request, raw_bytes):
        batch_id = request["batch_id"]
        base_version = int(request["base_version"])
        negotiation = negotiate(raw_bytes)
        fmt = negotiation["detected_format"]
        content_hash = self._payload_hash(raw_bytes)

        with self._inflight_lock:
            existing = self.store.get_batch(batch_id)
            if existing is not None:
                if existing["content_hash"] != content_hash:
                    raise ConflictingBatch(
                        "batch id %s reused with different content" % batch_id,
                        {"batch_id": batch_id},
                    )
                if existing["status"] == "COMMITTED":
                    return self._batch_result(existing)
                event = self._inflight.get(batch_id)
                if event is None:
                    event = threading.Event()
                    self._inflight[batch_id] = event
                should_run = False
            else:
                event = threading.Event()
                self._inflight[batch_id] = event
                should_run = True

        if not should_run:
            event.wait()
            row = self.store.get_batch(batch_id)
            if row is not None and row["content_hash"] != content_hash:
                raise ConflictingBatch(
                    "batch id %s reused with different content" % batch_id,
                    {"batch_id": batch_id},
                )
            return self._batch_result(row)

        try:
            result = self._execute(
                batch_id, base_version, fmt, raw_bytes, content_hash, request
            )
            result["negotiation"] = negotiation
            return result
        finally:
            with self._inflight_lock:
                event.set()
                self._inflight.pop(batch_id, None)

    def _batch_result(self, row):
        if row["status"] != "COMMITTED":
            raise MigrationError(
                "batch %s is not in a committed state: %s"
                % (row["batch_id"], row["status"]),
                {"batch_id": row["batch_id"], "status": row["status"]},
            )
        return {
            "batch_id": row["batch_id"],
            "status": row["status"],
            "base_version": row["base_version"],
            "committed_version": row["committed_version"],
            "decisions": self.store.get_decisions(row["batch_id"]),
            "plan_diff": self.plan_diff(row["batch_id"]),
            "reused": True,
        }

    def get_decisions(self, batch_id):
        row = self.store.get_batch(batch_id)
        if row is None:
            from .errors import BatchNotFound
            raise BatchNotFound("unknown batch %s" % batch_id, {"batch_id": batch_id})
        return self.store.get_decisions(batch_id)

    def plan_diff(self, batch_id):
        row = self.store.get_batch(batch_id)
        if row is None:
            from .errors import BatchNotFound
            raise BatchNotFound("unknown batch %s" % batch_id, {"batch_id": batch_id})
        decisions = self.store.get_decisions(batch_id)
        tenant_to_digest = {d["tenant_id"]: d["canonical_digest"] for d in decisions}
        target_version = row["committed_version"]
        if target_version is None:
            return []
        target_rows = self.store.policies_at_version(target_version)
        base_rows = (
            self.store.policies_at_version(row["base_version"])
            if row["base_version"] >= 0 else []
        )
        base = {r["tenant_id"]: json.loads(r["payload_json"]) for r in base_rows}
        entries = []
        for r in target_rows:
            if r["canonical_digest"] not in tenant_to_digest.values():
                continue
            payload = json.loads(r["payload_json"])
            before = base.get(r["tenant_id"])
            entries.append({
                "tenant_id": r["tenant_id"],
                "action": "ADD" if before is None else "UPDATE",
                "before": before,
                "after": payload,
                "changed_fields": None if before is None else
                sorted(k for k in payload if payload.get(k) != before.get(k)),
            })
        return entries

    def parent_chain_view(self, tenant_id, version=None):
        version = self.store.current_version() if version is None else version
        rows = {r["tenant_id"]: r for r in self.store.policies_at_version(version)}
        if tenant_id not in rows:
            from .errors import BatchNotFound
            raise BatchNotFound(
                "tenant %s not found at version %d" % (tenant_id, version),
                {"tenant_id": tenant_id, "version": version},
            )
        chain = []
        current = tenant_id
        seen = set()
        while current is not None:
            if current in seen:
                from .errors import ParentCycle
                raise ParentCycle("cycle in committed data", {"cycle_tenant_ids": chain})
            seen.add(current)
            row = rows.get(current)
            if row is None:
                chain.append({"tenant_id": current, "status": "REFERENCE_OUTSIDE_VERSION"})
                break
            payload = json.loads(row["payload_json"])
            chain.append({
                "tenant_id": current,
                "parent_ref": payload["parent_ref"],
                "concurrency_limit": payload["concurrency_limit"],
                "preempt_tier": payload["preempt_tier"],
                "lifetime": payload["lifetime"],
                "canonical_digest": row["canonical_digest"],
            })
            current = payload["parent_ref"]
        return {"tenant_id": tenant_id, "version": version,
                "depth": max(len(chain) - 1, 0), "chain": chain}

    def recover_pending_intents(self):
        store = self.store
        events = []
        with store.write_lock:
            pending = store.unfinished_intents()
            for row in pending:
                detail = json.loads(row["detail_json"]) if row["detail_json"] else {}
                batch = store.get_batch(row["batch_id"])
                if batch is not None and batch["status"] == "COMMITTED":
                    store.resolve_intent(row["batch_id"], "COMMITTED", now_ms(),
                                         {"note": "commit was durable"})
                    action = "CONFIRMED_COMMIT"
                else:
                    if batch is not None:
                        store.update_batch_status(
                            row["batch_id"], "ABORTED", now_ms(),
                            error_code="E_RECOVERY_ABORT",
                            error_message="aborted from durable facts after hard kill",
                        )
                    store.resolve_intent(row["batch_id"], "ABORTED", now_ms(), detail)
                    action = "ROLLED_BACK_TO_LAST_COMPLETE_VERSION"
                event = {
                    "batch_id": row["batch_id"],
                    "action": action,
                    "current_version": store.current_version(),
                    "facts": {"intent_state": row["state"], "detail": detail},
                }
                store.insert_recovery_event(now_ms(), row["batch_id"], action, event)
                events.append(event)
            if pending:
                store.conn.commit()
            durable_events = [
                {
                    "batch_id": r["batch_id"],
                    "action": r["action"],
                    "current_version": store.current_version(),
                    "facts": json.loads(r["detail_json"]).get("facts", {}),
                }
                for r in store.recovery_events()
            ]
        return {
            "recovered_at_ms": now_ms(),
            "current_version": store.current_version(),
            "pending_intents_found": len(pending),
            "events": events,
            "durable_recovery_events": durable_events,
            "policy_set_rolled_back": any(
                e["action"] == "ROLLED_BACK_TO_LAST_COMPLETE_VERSION"
                for e in durable_events
            ),
        }

    def recovery_report_view(self):
        events = [
            {
                "recovered_at_ms": r["recovered_at_ms"],
                "batch_id": r["batch_id"],
                "action": r["action"],
                "detail": json.loads(r["detail_json"]),
            }
            for r in self.store.recovery_events()
        ]
        return {
            "current_version": self.store.current_version(),
            "last_startup_report": self.recovery_report,
            "events": events,
        }
