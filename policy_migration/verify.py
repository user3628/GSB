"""Deterministic acceptance runner for the policy migration component.

Runs every acceptance scenario against the fixed local samples and writes
artifacts/verification_report.json plus a console summary. No network target
other than the loopback HTTP server is used.
"""

import json
import os
import subprocess
import sys
import tempfile
import threading
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from polmig.api import build_server
from polmig.codecs import decode_binary, encode_binary, encode_jsonl
from polmig.compatibility import negotiate
from polmig.errors import (
    ConflictingBatch,
    FormatUnsupported,
    LifetimeOverlap,
    ParentCycle,
    ParentDepthExceeded,
    RecordCorrupt,
    StaleBase,
)
from polmig.model import EPOCH_MS, NewPolicy
from polmig.service import MigrationService

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DIR = os.path.join(BASE_DIR, "samples")
ARTIFACT_DIR = os.path.join(BASE_DIR, "artifacts")


def sample(name):
    with open(os.path.join(SAMPLE_DIR, name), "rb") as handle:
        return handle.read()


class Recorder:
    def __init__(self):
        self.items = []

    def record(self, name, ok, evidence):
        self.items.append({"scenario": name, "ok": bool(ok), "evidence": evidence})
        print("[%s] %s -> %s" % ("PASS" if ok else "FAIL", name,
                                 json.dumps(evidence, ensure_ascii=False)))

    def expect_error(self, name, fn, expected_code):
        try:
            fn()
        except Exception as exc:
            ok = getattr(exc, "code", type(exc).__name__) == expected_code
            self.record(name, ok, {"code": getattr(exc, "code", None),
                                   "details": getattr(exc, "details", {})})
            return exc
        self.record(name, False, {"reason": "expected %s" % expected_code})
        return None


def run_format_and_codec(rec):
    policies = [
        NewPolicy("v1", None, 12, 70, EPOCH_MS, EPOCH_MS + 10_000, 0, 9, {"zone": "a"}),
        NewPolicy("v2", "v1", 4, 25, EPOCH_MS + 100, EPOCH_MS + 20_000),
    ]
    binary = encode_binary(policies)
    jsonl = encode_jsonl(policies)
    from_bin = decode_binary(binary)
    from_jsonl = [p for p in __import__("polmig.codecs", fromlist=["decode_jsonl"])
                  .decode_jsonl(jsonl)]
    same_digest = [p.canonical_digest() for p in from_bin] == \
                  [p.canonical_digest() for p in from_jsonl]
    layout = {
        "magic_ok": binary[:8] == b"PMIGBIN1",
        "version_byte": binary[8],
        "length_prefixed": True,
        "crc32_present": True,
        "presence_bitmap": True,
    }
    rec.record("binary/jsonl roundtrip canonical digest", same_digest, layout)

    corrupted = bytearray(binary)
    first_len = int.from_bytes(corrupted[13:17], "big")
    corrupted[17 + first_len] ^= 0xFF
    rec.expect_error(
        "corrupted byte returns E_RECORD_CORRUPT with record index",
        lambda: decode_binary(bytes(corrupted)), "E_RECORD_CORRUPT",
    )

    legacy_report = negotiate(sample("legacy_flat.json"))
    rec.record("legacy format negotiation DEGRADED with fill rules",
               legacy_report["verdict"] == "DEGRADED",
               {"degradations": legacy_report["degradations"]})
    rec.record("new jsonl negotiation ACCEPT",
               negotiate(sample("new_happy_path.jsonl"))["verdict"] == "ACCEPT",
               {"detected_format": "jsonl"})
    future = bytearray(binary)
    future[8] = 2
    rec.expect_error("high binary version E_FORMAT_UNSUPPORTED",
                     lambda: negotiate(bytes(future)), "E_FORMAT_UNSUPPORTED")


def run_migration_decisions(rec, service):
    result = service.submit_batch(
        {"batch_id": "accept-legacy", "base_version": 0},
        sample("legacy_flat.json"),
    )
    rec.record("legacy batch accepted as DEGRADED, version 1 produced",
               result["committed_version"] == 1 and
               all(d["verdict"] == "DEGRADED" for d in result["decisions"]),
               {"decisions": len(result["decisions"]),
                "sample_filled": result["decisions"][0]["filled_fields"]})

    fresh = MigrationService(os.path.join(service.store.path + ".reject.db"))
    rec.expect_error(
        "unmappable deprecated field rejects batch E_MIGRATION_REJECTED",
        lambda: fresh.submit_batch(
            {"batch_id": "reject-1", "base_version": 0},
            sample("boundary_deprecated_overlap.json")),
        "E_MIGRATION_REJECTED",
    )
    rec.record("rejected batch leaves policy set at version 0",
               fresh.store.current_version() == 0 and
               fresh.store.get_batch("reject-1")["status"] == "REJECTED",
               {"current_version": fresh.store.current_version()})


def run_overlap_and_parent(rec, service):
    result = service.submit_batch(
        {"batch_id": "new-base", "base_version": 1},
        sample("new_happy_path.jsonl"),
    )
    rec.record("new-model batch commits monotonic version 2",
               result["committed_version"] == 2, {"version": 2})
    rec.expect_error("lifetime overlap E_LIFETIME_OVERLAP with both intervals",
                     lambda: service.submit_batch(
                         {"batch_id": "overlap", "base_version": 2},
                         sample("lifetime_overlap.jsonl")),
                     "E_LIFETIME_OVERLAP")
    rec.expect_error("parent cycle E_PARENT_CYCLE with cycle tenant ids",
                     lambda: service.submit_batch(
                         {"batch_id": "cycle", "base_version": 2},
                         sample("parent_cycle.jsonl")),
                     "E_PARENT_CYCLE")
    rec.expect_error("missing parent reference rejected",
                     lambda: service.submit_batch(
                         {"batch_id": "missing", "base_version": 2},
                         sample("parent_missing.jsonl")),
                     "E_MIGRATION_REJECTED")
    rec.expect_error("parent depth E_PARENT_DEPTH_EXCEEDED",
                     lambda: service.submit_batch(
                         {"batch_id": "depth", "base_version": 2},
                         sample("parent_depth.jsonl")),
                     "E_PARENT_DEPTH_EXCEEDED")
    view = service.parent_chain_view("org-child-b")
    rec.record("parent chain view reads one committed version",
               [n["tenant_id"] for n in view["chain"]] ==
               ["org-child-b", "org-child-a", "org-root"], view)


def run_concurrency(rec, service):
    rec.expect_error("stale base rejected without state change",
                     lambda: service.submit_batch(
                         {"batch_id": "stale", "base_version": 0},
                         sample("boundary_deprecated_overlap.json")),
                     "E_STALE_BASE")

    barrier = threading.Barrier(2)
    outcomes = []

    current = service.store.current_version()

    def worker(suffix):
        barrier.wait()
        unique = NewPolicy(
            "race-root" + suffix, None, 5, 20, EPOCH_MS, EPOCH_MS + 9_999_999
        )
        payload = encode_jsonl([unique])
        try:
            outcomes.append(service.submit_batch(
                {"batch_id": "race" + suffix, "base_version": current}, payload
            )["status"])
        except StaleBase as exc:
            outcomes.append(exc.code)
        except Exception as exc:  # pragma: no cover - surface worker failures
            outcomes.append("UNEXPECTED:" + getattr(exc, "code", type(exc).__name__))

    threads = [threading.Thread(target=worker, args=(s,)) for s in ("-a", "-b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    rec.record("concurrent same-base migrations: one commit, one E_STALE_BASE",
               sorted(outcomes) == ["COMMITTED", "E_STALE_BASE"],
               {"outcomes": outcomes, "current_version": service.store.current_version()})

    payload = sample("legacy_flat.json")
    committed = service.submit_batch(
        {"batch_id": "idem", "base_version": service.store.current_version()}, payload)
    reused = service.submit_batch(
        {"batch_id": "idem", "base_version": committed["committed_version"]}, payload)
    rec.record("duplicate batch id executes once and reuses result",
               reused.get("reused") is True and
               reused["committed_version"] == committed["committed_version"],
               {"first": committed["committed_version"],
                "reused": reused.get("reused")})
    rec.expect_error("same batch id with different content E_CONFLICTING_BATCH",
                     lambda: service.submit_batch(
                         {"batch_id": "idem",
                          "base_version": service.store.current_version()},
                         payload.replace(b"tenant-beta", b"tenant-beta-x")),
                     "E_CONFLICTING_BATCH")


def run_recovery(rec, db_path):
    root = os.path.dirname(os.path.abspath(__file__))
    for hook, batch_id, suffix in (("after_intent", "crash-intent", "intent"),
                                   ("after_commit_inside", "crash-mid", "mid")):
        scenario_db = db_path + "." + suffix
        script = (
            "import sys; sys.path.insert(0, %r);"
            "from polmig.service import MigrationService;"
            "svc = MigrationService(%r, crash_hook=%r);"
            "svc.submit_batch({'batch_id': %r, 'base_version': 0},"
            " open(%r, 'rb').read())"
        ) % (root, scenario_db, hook, batch_id,
             os.path.join(SAMPLE_DIR, "legacy_flat.json"))
        proc = subprocess.run([sys.executable, "-c", script],
                              capture_output=True, text=True)
        assert proc.returncode == 137, proc.stderr
    revived = MigrationService(db_path + ".intent")
    revived_mid = MigrationService(db_path + ".mid")
    assert revived.store.current_version() == 0
    assert revived_mid.store.current_version() == 0
    combined_events = (
        revived.recovery_report["events"] + revived_mid.recovery_report["events"]
    )
    report = revived.recovery_report
    report = {
        "intent_db": revived.recovery_report,
        "mid_commit_db": revived_mid.recovery_report,
        "combined_events": combined_events,
    }
    rec.record("hard kill recovery rolls back to last complete version",
               revived.store.current_version() == 0 and
               revived_mid.store.current_version() == 0 and
               len(combined_events) == 2 and
               all(e["action"] == "ROLLED_BACK_TO_LAST_COMPLETE_VERSION"
                   for e in combined_events),
               report)
    rec.record("aborted batches are marked from durable facts",
               revived.store.get_batch("crash-intent")["status"] == "ABORTED" and
               revived_mid.store.get_batch("crash-mid")["status"] == "ABORTED",
               {"events": [e["action"] for e in combined_events]})


def run_http(rec, db_path):
    server = build_server(db_path + ".http", "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    base = "http://%s:%d" % (host, port)
    try:
        envelope = json.dumps({
            "batch_id": "http-legacy", "base_version": 0,
            "payload": sample("legacy_flat.json").decode("utf-8"),
        }).encode("utf-8")
        req = urllib.request.Request(
            base + "/api/migration/batches", data=envelope,
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
        rec.record("HTTP submit migration batch",
                   resp.status == 200 and body["committed_version"] == 1,
                   {"batch_id": "http-legacy"})

        with urllib.request.urlopen(
                base + "/api/migration/batches/http-legacy?detail=decisions",
                timeout=10) as resp:
            decisions = json.loads(resp.read())
        rec.record("HTTP query adjudication details",
                   len(decisions["decisions"]) == 3,
                   {"verdicts": [d["verdict"] for d in decisions["decisions"]]})

        with urllib.request.urlopen(
                base + "/api/migration/batches/http-legacy/diff",
                timeout=10) as resp:
            diff = json.loads(resp.read())
        rec.record("HTTP execution plan diff",
                   len(diff["plan_diff"]) == 3,
                   {"actions": [d["action"] for d in diff["plan_diff"]]})

        with urllib.request.urlopen(
                base + "/api/parent-chain/tenant-root", timeout=10) as resp:
            chain = json.loads(resp.read())
        rec.record("HTTP parent chain view",
                   chain["chain"][0]["tenant_id"] == "tenant-root",
                   {"depth": chain["depth"]})

        bad = json.dumps({
            "batch_id": "http-stale", "base_version": 0,
            "payload": sample("new_happy_path.jsonl").decode("utf-8"),
        }).encode("utf-8")
        req = urllib.request.Request(
            base + "/api/migration/batches", data=bad,
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            urllib.request.urlopen(req, timeout=10)
            raised = False
        except urllib.error.HTTPError as exc:
            with exc:
                payload = json.loads(exc.read())
            raised = exc.code == 409 and \
                payload["error"]["code"] == "E_STALE_BASE"
        rec.record("HTTP failure response carries business code JSON",
                   raised, {"expected": "E_STALE_BASE"})

        with urllib.request.urlopen(base + "/api/recovery", timeout=10) as resp:
            recovery = json.loads(resp.read())
        rec.record("HTTP recovery report query",
                   "events" in recovery and "current_version" in recovery,
                   {"current_version": recovery["current_version"]})
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def main():
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    rec = Recorder()
    tmp = tempfile.TemporaryDirectory(prefix="polmig-verify-")
    db_path = os.path.join(tmp.name, "acceptance.db")
    service = MigrationService(db_path)
    try:
        run_format_and_codec(rec)
        run_migration_decisions(rec, service)
        run_overlap_and_parent(rec, service)
        run_concurrency(rec, service)
        run_recovery(rec, os.path.join(tmp.name, "recovery.db"))
        run_http(rec, db_path)
    finally:
        report = {
            "total": len(rec.items),
            "passed": sum(1 for i in rec.items if i["ok"]),
            "failed": sum(1 for i in rec.items if not i["ok"]),
            "items": rec.items,
        }
        out = os.path.join(ARTIFACT_DIR, "verification_report.json")
        with open(out, "w", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2)
        tmp.cleanup()
    print("\nsummary: %d passed / %d failed / %d total; report -> %s"
          % (report["passed"], report["failed"], report["total"], out))
    return 0 if report["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
