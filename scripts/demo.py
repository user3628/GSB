#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import tempfile
import time
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"
ARTIFACTS = ROOT / "artifacts"


def load(name: str) -> dict[str, Any]:
    return json.loads((SAMPLES / name).read_text())


def request(client: httpx.Client, method: str, path: str, **kwargs) -> dict[str, Any]:
    response = client.request(method, path, **kwargs)
    if response.status_code >= 400:
        raise RuntimeError(f"{method} {path} -> {response.status_code}: {response.text}")
    return response.json()


def wait_health(base_url: str, timeout: float = 30.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with httpx.Client(base_url=base_url, timeout=1.0) as client:
                response = client.get("/api/health")
                if response.status_code == 200:
                    return response.json()
        except Exception as exc:  # process restart window
            last_error = exc
        time.sleep(0.2)
    raise RuntimeError(f"service did not become healthy: {last_error}")


def expect_error(client: httpx.Client, method: str, path: str, expected_code: str, **kwargs) -> dict[str, Any]:
    response = client.request(method, path, **kwargs)
    body = response.json()
    if response.status_code < 400 or body.get("code") != expected_code:
        raise AssertionError(f"expected {expected_code}, got HTTP {response.status_code}: {body}")
    return body


def compact_windows(windows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = [
        "window_id",
        "window_kind",
        "spec_hash",
        "start_ms",
        "end_ms",
        "group_key",
        "state",
        "boundary",
        "version",
        "created_batch",
        "updated_batch",
        "last_event_ts",
        "group_current_type",
        "count",
        "non_null_count",
        "sum",
        "sum_scaled",
        "min",
        "max",
        "scale",
        "aggregate_type",
        "input_types",
    ]
    return [{key: window.get(key) for key in keys} for window in windows]


def print_windows(label: str, windows: list[dict[str, Any]]) -> None:
    print(f"\n[{label}]")
    print(f"{'start':>6} {'end':>6} {'group':<7} {'state':<8} {'cnt':>3} {'sum':>8} {'min':>8} {'max':>8} {'type':<8} {'scale':>5}")
    for window in windows:
        print(
            f"{window['start_ms']:>6} {window['end_ms']:>6} {window['group_key']:<7} "
            f"{window['state']:<8} {window['count']:>3} {str(window['sum']):>8} "
            f"{str(window['min']):>8} {str(window['max']):>8} {window['aggregate_type']:<8} {window['scale']:>5}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Repeatable HTTP demo using local deterministic samples")
    parser.add_argument("--base-url", default="http://127.0.0.1:8765")
    parser.add_argument("--db", required=True, help="SQLite file used by the demo service, for the baseline comparison")
    args = parser.parse_args()
    ARTIFACTS.mkdir(exist_ok=True)

    health = wait_health(args.base_url)
    if health["state_version"] != 0:
        raise RuntimeError(
            "demo requires a fresh database; run scripts/run_demo.sh or remove the configured demo DB first"
        )
    print(f"service healthy: {json.dumps(health, ensure_ascii=False)}")

    observed_errors: dict[str, dict[str, Any]] = {}
    summary: dict[str, Any] = {}

    with httpx.Client(base_url=args.base_url, timeout=15.0) as client:
        # 1. Integer -> fixed-point decimal -> integer on the same sliding spec.
        integer_batch = load("events_integer.json")
        decimal_batch = load("events_decimal.json")
        integer_again = load("events_integer_again.json")
        for batch in (integer_batch, decimal_batch, integer_again):
            result = request(client, "POST", "/api/batches", json=batch)
            print(
                f"ingested {batch['batch_id']}: events={result['event_count']} "
                f"late={result['late_event_count']} state_version={result['state_version']}"
            )

        evolution = request(client, "GET", "/api/type-evolution")
        print("\n[type evolution]")
        for group in evolution["groups"]:
            sequence = " -> ".join(
                f"{item['to']['kind']}({item['to']['scale']})" for item in group["evolution"]
            )
            print(f"{group['group_key']}: {sequence} | current={group['current_group_type'] if 'current_group_type' in group else group['current_type']}")
        summary["type_evolution"] = evolution

        windows_payload = request(
            client,
            "GET",
            "/api/windows",
            params={"from_ms": -1000, "to_ms": 3000, "include_type_history": "true"},
        )
        print_windows("sliding windows after type evolution", windows_payload["windows"])
        mixed = [
            window
            for window in windows_payload["windows"]
            if {item["kind"] for item in window["input_types"]} >= {"integer", "decimal"}
        ]
        if not mixed:
            raise AssertionError("expected at least one window mixing integer and decimal inputs")
        print(
            f"\nmixed-type windows: {len(mixed)} "
            f"(example sum={mixed[0]['sum']} scale={mixed[0]['scale']} rounding={mixed[0]['rounding']})"
        )
        summary["mixed_type_window"] = compact_windows(mixed)[0]

        # Error probes for the required business codes.
        mismatch = copy.deepcopy(integer_again)
        mismatch["batch_id"] = "demo-type-mismatch"
        mismatch["events"] = [
            {"event_id": "bad-1", "timestamp_ms": 2400, "group_key": "north", "measure": "1.25", "labels": {}}
        ]
        observed_errors["E_MEASURE_TYPE_MISMATCH"] = expect_error(
            client, "POST", "/api/batches", "E_MEASURE_TYPE_MISMATCH", json=mismatch
        )

        conflicting = copy.deepcopy(integer_batch)
        conflicting["events"] = [
            {"event_id": "conflict-1", "timestamp_ms": 2500, "group_key": "north", "measure": 101, "labels": {}}
        ]
        observed_errors["E_CONFLICTING_BATCH"] = expect_error(
            client, "POST", "/api/batches", "E_CONFLICTING_BATCH", json=conflicting
        )

        # 2. Close, treat a late event, then emit.
        closed = request(client, "POST", "/api/windows/close", json={"through_ms": 2500})
        print(f"\nclosed windows: {closed['transition_count']} transitions, state_version={closed['state_version']}")
        before_late = request(
            client, "GET", "/api/windows", params={"from_ms": 0, "to_ms": 2500, "group_key": "north"}
        )
        late = load("events_late.json")
        late_result = request(client, "POST", "/api/batches", json=late)
        after_late = request(
            client, "GET", "/api/windows", params={"from_ms": 0, "to_ms": 2500, "group_key": "north"}
        )
        if compact_windows(before_late["windows"]) != compact_windows(after_late["windows"]):
            raise AssertionError("queued late event mutated closed window aggregates")
        print(
            f"late batch queued: late_event_count={late_result['late_event_count']} "
            f"(closed aggregates unchanged)"
        )
        summary["late_queue"] = request(client, "GET", "/api/late-events")

        late_reject = load("events_late_reject.json")
        version_before_reject = request(client, "GET", "/api/status")["state_version"]
        observed_errors["E_LATE_EVENT"] = expect_error(
            client, "POST", "/api/batches", "E_LATE_EVENT", json=late_reject
        )
        if request(client, "GET", "/api/status")["state_version"] != version_before_reject:
            raise AssertionError("rejected late batch changed state_version")

        emitted = request(client, "POST", "/api/windows/emit", json={"through_ms": 2500})
        print(f"emitted windows: {emitted['transition_count']} transitions")
        frozen = request(
            client, "GET", "/api/windows", params={"from_ms": 0, "to_ms": 2500, "group_key": "north"}
        )
        emitted_window = next(window for window in frozen["windows"] if window["state"] == "EMITTED")
        observed_errors["E_INVALID_TRANSITION"] = expect_error(
            client,
            "POST",
            "/api/windows/transition",
            "E_INVALID_TRANSITION",
            json={"window_id": emitted_window["window_id"], "target_state": "OPEN"},
        )

        # 3. Snapshot, advance state, then load the older snapshot version. The
        # loaded-version sequence is monotonic (0 -> 1) even though state is restored.
        pre_snapshot = request(
            client, "GET", "/api/windows", params={"from_ms": -1000, "to_ms": 6000}
        )
        metadata = request(client, "POST", "/api/snapshots", json={})
        snapshot = request(client, "GET", f"/api/snapshots/{metadata['version']}")
        (ARTIFACTS / f"demo-snapshot-v{metadata['version']}.json").write_text(
            json.dumps(snapshot, indent=2, ensure_ascii=False)
        )
        post_snapshot_batch = {
            "batch_id": "demo-post-snapshot",
            "window_spec": integer_batch["window_spec"],
            "measure_type": {"kind": "integer"},
            "timestamp_order": "sort",
            "late_policy": "queue",
            "events": [
                {"event_id": "post-5000", "timestamp_ms": 5000, "group_key": "north", "measure": 7, "labels": {}}
            ],
        }
        request(client, "POST", "/api/batches", json=post_snapshot_batch)
        changed = request(client, "GET", "/api/windows", params={"from_ms": 4500, "to_ms": 6000})
        if not changed["windows"]:
            raise AssertionError("post-snapshot batch did not create windows")
        loaded = request(client, "POST", "/api/snapshots/load", json={"snapshot": snapshot})
        restored = request(
            client, "GET", "/api/windows", params={"from_ms": -1000, "to_ms": 6000}
        )
        snapshot_diff = []
        if compact_windows(pre_snapshot["windows"]) != compact_windows(restored["windows"]):
            snapshot_diff.append("window state mismatch after snapshot replacement")
        post_restore = request(client, "GET", "/api/windows", params={"from_ms": 4500, "to_ms": 6000})
        if post_restore["windows"]:
            snapshot_diff.append("post-snapshot windows survived replacement")
        print(
            f"\nsnapshot v{loaded['snapshot_version']} loaded: windows={loaded['window_count']} "
            f"replacement_diff={snapshot_diff}"
        )
        summary["snapshot_load"] = {"load": loaded, "diff": snapshot_diff}

        tampered = copy.deepcopy(snapshot)
        tampered["checksum"] = "sha256:tampered"
        version_before_bad_snapshot = request(client, "GET", "/api/status")["state_version"]
        observed_errors["E_SNAPSHOT_INVALID"] = expect_error(
            client,
            "POST",
            "/api/snapshots/load",
            "E_SNAPSHOT_INVALID",
            json={"snapshot": tampered},
        )
        if request(client, "GET", "/api/status")["state_version"] != version_before_bad_snapshot:
            raise AssertionError("invalid snapshot changed state_version")

        # 4. Force-terminate the server while the crash batch transaction is open.
        crash_batch = load("events_crash.json")
        print("\nforcing process termination during batch commit...")
        try:
            client.post("/api/demo/crash-ingest", json={**crash_batch, "crash_point": "mid_transaction"})
        except Exception as exc:
            print(f"expected transport failure from forced termination: {type(exc).__name__}")

        restarted = wait_health(args.base_url, timeout=45.0)
        print(f"service restarted: {json.dumps(restarted, ensure_ascii=False)}")
        report = request(client, "GET", "/api/recovery/report")
        print(f"\n[recovery report]\n{json.dumps(report, indent=2, ensure_ascii=False)}")
        (ARTIFACTS / "demo-recovery-report.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False)
        )
        if crash_batch["batch_id"] not in {item["batch_id"] for item in report.get("aborted_batches", [])}:
            raise AssertionError("crashed batch was not reported as aborted")

        continuation = load("events_continuation.json")
        continuation_response = request(client, "POST", "/api/batches", json=continuation)
        recovered_windows = request(
            client,
            "GET",
            "/api/windows",
            params={"from_ms": 3001, "to_ms": 4500},
        )
        print_windows("windows after restart and continuation", recovered_windows["windows"])

        baseline_db = Path(tempfile.mkstemp(prefix="streamagg-demo-baseline-", suffix=".db")[1])
        from app.engine import Engine

        baseline = Engine(f"sqlite:///{baseline_db}")
        try:
            from app.schemas import BatchRequest

            baseline.ingest_batch(BatchRequest.model_validate(continuation))
            baseline_windows = baseline.query_windows(from_ms=3001, to_ms=4500)
        finally:
            baseline.close()
            for suffix in ("", "-wal", "-shm"):
                try:
                    Path(str(baseline_db) + suffix).unlink()
                except FileNotFoundError:
                    pass

        recovery_diff: list[Any] = []
        recovered_compact = compact_windows(recovered_windows["windows"])
        baseline_compact = compact_windows(baseline_windows["windows"])
        if recovered_compact != baseline_compact:
            recovery_diff.append(
                {
                    "recovered": recovered_compact,
                    "uninterrupted_baseline": baseline_compact,
                }
            )
        print(f"\nrecovery_diff={json.dumps(recovery_diff, ensure_ascii=False)}")
        if recovery_diff:
            raise AssertionError("recovered continuation does not match uninterrupted baseline")
        summary["recovery"] = {
            "report": report,
            "continuation_state_version": continuation_response["state_version"],
            "recovered_windows": recovered_compact,
            "diff": recovery_diff,
        }

    summary["errors"] = {
        code: {"message": value["message"], "context": value["context"]}
        for code, value in observed_errors.items()
    }
    (ARTIFACTS / "demo-summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print("\n[business error codes observed]")
    for code in sorted(observed_errors):
        print(f"{code}: {observed_errors[code]['message']}")
    print("\nDEMO PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
