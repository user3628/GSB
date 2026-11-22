#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kvstore.errors import StoreError  # noqa: E402
from kvstore.formats import decode_binary, decode_jsonl  # noqa: E402
from kvstore.store import Store  # noqa: E402


BASE_MS = 1_900_000_000_123


def semantic_index(store: Store) -> dict[str, list[object]]:
    state = store.read_view().state
    return {
        key: [ref.version, ref.write_ts_ms, ref.expires_at_ms]
        for key, ref in sorted(state.index.items())
    }


def exact_ref_index(store: Store) -> dict[str, dict[str, object]]:
    state = store.read_view().state
    return {key: ref.public() for key, ref in sorted(state.index.items())}


def error_code(exc: StoreError) -> dict[str, object]:
    return {"code": exc.code, "context": exc.context}


def direct_scenarios(runtime: Path, sample_dir: Path) -> dict[str, object]:
    result: dict[str, object] = {}
    binary_dir = runtime / "direct_binary"
    binary_store = Store(binary_dir)
    try:
        binary_report = binary_store.import_data(
            (sample_dir / "sample.kvs.bin").read_bytes(), batch_id="demo-full-binary"
        )
        initial_semantics = semantic_index(binary_store)
        initial_refs = exact_ref_index(binary_store)
        result["binary_import"] = {
            "status": binary_report["status"],
            "negotiation": {
                key: binary_report["format"].get(key)
                for key in (
                    "detected_format",
                    "selected_format",
                    "format_version",
                    "endianness",
                    "declared_record_count",
                    "actual_record_count",
                    "compatible",
                )
            },
            "summary": binary_report["summary"],
            "state": binary_store.stats()["state"],
        }

        updated = binary_store.put(
            "alpha",
            "domain-alpha-v2-incremental",
            version=2,
            write_ts_ms=BASE_MS + 10_000,
            expires_at_ms=BASE_MS + 172_800_000,
        )
        after_refs = exact_ref_index(binary_store)
        changed_keys = [
            key
            for key in sorted(set(initial_refs) | set(after_refs))
            if initial_refs.get(key) != after_refs.get(key)
        ]
        heap_before_cleanup = binary_store.heap_check()
        cleanup = binary_store.expire(now_ms=BASE_MS + 5_000)
        result["incremental_write_and_expiry"] = {
            "write": {
                "key": updated["key"],
                "version": updated["version"],
                "segment": updated["segment"],
                "state_version": updated["state_version"],
            },
            "changed_keys": changed_keys,
            "unchanged_key_count": len(set(initial_refs) - set(changed_keys)),
            "heap_consistent_before_cleanup": heap_before_cleanup["ok"],
            "heap_entries_before_cleanup": heap_before_cleanup["entries"],
            "cleanup": {
                "cleaned_count": cleanup["cleaned_count"],
                "cleaned_keys": cleanup["cleaned_keys"],
                "tombstones_recorded": cleanup["tombstones_recorded"],
            },
            "state_after": binary_store.stats()["state"],
        }

        binary_export = binary_store.export_binary()
        jsonl_export = binary_store.export_jsonl(shard_count=2)
        result["exports"] = {
            "binary_bytes": len(binary_export),
            "binary_record_count": len(decode_binary(binary_export)),
            "jsonl_bytes": len(jsonl_export),
            "jsonl_record_count": len(
                decode_jsonl(jsonl_export, import_time_ms=BASE_MS)
            ),
        }
        snapshot = binary_store.export_snapshot(snapshot_seq=1)
        result["snapshot_export"] = {
            "snapshot_seq": snapshot["snapshot_seq"],
            "record_count": len(snapshot["records"]),
            "checksum": snapshot["checksum"],
        }
    finally:
        binary_store.close()

    json_dir = runtime / "direct_json"
    json_store = Store(json_dir)
    try:
        json_report = json_store.import_data(
            (sample_dir / "sample.jsonl").read_bytes(), batch_id="demo-full-json"
        )
        result["jsonl_interop"] = {
            "status": json_report["status"],
            "semantic_index_matches_binary": semantic_index(json_store) == initial_semantics,
            "cumulative_rounding_error_us": json_report[
                "cumulative_rounding_error_us"
            ],
            "rounding_samples": [
                {
                    "key": item["key"],
                    "write_remainder_us": item["write"]["rounding_remainder_us"],
                    "expires_remainder_us": None
                    if item["expires"] is None
                    else item["expires"]["rounding_remainder_us"],
                }
                for item in json_report["rounding_report"]
            ],
            "state": json_store.stats()["state"],
        }
    finally:
        json_store.close()

    snapshot_target = Store(runtime / "snapshot_target")
    try:
        snapshot_target.put("old-snapshot-key", "old", write_ts_ms=BASE_MS)
        invalid = json.loads(json.dumps(snapshot))
        invalid["records"][0]["value"] = "tampered"
        invalid_error = None
        try:
            snapshot_target.load_snapshot(invalid)
        except StoreError as exc:
            invalid_error = error_code(exc)
        old_state_still_present = bool(
            snapshot_target.get(
                "old-snapshot-key", now_ms=BASE_MS + 1
            )["value"]
            == "old"
        )
        switched = snapshot_target.load_snapshot(snapshot)
        stale_error = None
        try:
            snapshot_target.load_snapshot(snapshot)
        except StoreError as exc:
            stale_error = error_code(exc)
        result["snapshot_switch"] = {
            "invalid_snapshot_error": invalid_error,
            "old_state_usable_after_invalid": old_state_still_present,
            "valid_switch": switched,
            "stale_switch_error": stale_error,
            "state_after": snapshot_target.stats()["state"],
        }
    finally:
        snapshot_target.close()
    return result


def integrity_scenario(runtime: Path) -> dict[str, object]:
    store = Store(runtime / "integrity")
    try:
        store.put("a", "one", write_ts_ms=BASE_MS, expires_at_ms=9_000_000_000_000)
        store.put("b", "two", write_ts_ms=BASE_MS + 1)
        summary = store.segment_summary()["segments"]
        target = Path(summary[0]["filename"])
        segment_path = store.data_dir / "segments" / target
        blob = bytearray(segment_path.read_bytes())
        blob[-4] ^= 0x5A
        segment_path.write_bytes(blob)
        verification = store.verify_segments()
        return {
            "corrupted_segment": summary[0]["filename"],
            "status": verification["status"],
            "invalidated_keys": verification["invalidated_keys"],
            "invalidated_key_count": verification["invalidated_key_count"],
            "state_rebuilt": verification["state_rebuilt"],
            "remaining_keys": sorted(store.read_view().state.index),
            "heap_consistent": store.heap_check()["ok"],
        }
    finally:
        store.close()


def run_crash_worker(data_dir: Path, crash_point: str, sample: Path, batch_id: str):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    return subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/crash_worker.py"),
            str(data_dir),
            "--crash-point",
            crash_point,
            "--import-file",
            str(sample),
            "--batch-id",
            batch_id,
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )


def crash_scenario(runtime: Path) -> dict[str, object]:
    sample = runtime / "crash_import.jsonl"
    sample.write_bytes(
        (
            json.dumps(
                {
                    "key": "crash-a",
                    "value": "recovered-one",
                    "version": 1,
                    "write_timestamp_us": (BASE_MS + 20_000) * 1000,
                    "expires_at_us": None,
                },
                separators=(",", ":"),
            )
            + "\n"
            + json.dumps(
                {
                    "key": "crash-b",
                    "value": "recovered-two",
                    "version": 1,
                    "write_timestamp_us": (BASE_MS + 20_001) * 1000,
                    "expires_at_us": None,
                },
                separators=(",", ":"),
            )
            + "\n"
        ).encode()
    )
    after_segment_dir = runtime / "crash_after_segment"
    base_store = Store(after_segment_dir)
    base_store.put("baseline", "still-here", write_ts_ms=BASE_MS)
    base_store.close()
    killed_durable = run_crash_worker(
        after_segment_dir, "after_segment", sample, "crash-after-segment"
    )
    recovered = Store(after_segment_dir)
    try:
        recovered_keys = {
            key: recovered.get(key)["value"]
            for key in ("baseline", "crash-a", "crash-b")
        }
        recovery_report = recovered.recovery_report()["report"]
    finally:
        recovered.close()

    after_intent_dir = runtime / "crash_after_intent"
    base_store = Store(after_intent_dir)
    base_store.put("baseline", "unchanged", write_ts_ms=BASE_MS)
    base_store.close()
    killed_before = run_crash_worker(
        after_intent_dir, "after_intent", sample, "crash-after-intent"
    )
    rolled_back = Store(after_intent_dir)
    try:
        rolled_back_report = rolled_back.recovery_report()["report"]
        absent_after_rollback = "crash-a" not in rolled_back.read_view().state.index
    finally:
        rolled_back.close()
    return {
        "after_segment": {
            "exit_code": killed_durable.returncode,
            "shell_exit_code": 137 if killed_durable.returncode == -9 else killed_durable.returncode,
            "recovered_values": recovered_keys,
            "recovered_count": recovery_report["recovered_count"],
            "rolled_back_count": recovery_report["rolled_back_count"],
            "intent_actions": [
                {
                    "batch_id": item["batch_id"],
                    "action": item["action"],
                }
                for item in recovery_report["intents"]
            ],
            "facts_only": recovery_report["facts_only"],
            "process_exit_report_available": recovery_report[
                "process_exit_report_available"
            ],
        },
        "after_intent": {
            "exit_code": killed_before.returncode,
            "shell_exit_code": 137 if killed_before.returncode == -9 else killed_before.returncode,
            "rolled_back_count": rolled_back_report["rolled_back_count"],
            "batch_absent_after_restart": absent_after_rollback,
            "intent_actions": [
                {
                    "batch_id": item["batch_id"],
                    "action": item["action"],
                }
                for item in rolled_back_report["intents"]
            ],
        },
    }


def http_scenario(runtime: Path, sample_dir: Path) -> dict[str, object]:
    http_dir = runtime / "http"
    port_file = runtime / "http_port.json"
    port_file.unlink(missing_ok=True)
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "kvstore",
            "--data-dir",
            str(http_dir),
            "--host",
            "127.0.0.1",
            "--port",
            "0",
            "--port-file",
            str(port_file),
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.time() + 15
        while time.time() < deadline and not port_file.exists():
            if process.poll() is not None:
                stdout, stderr = process.communicate(timeout=2)
                raise RuntimeError(f"HTTP server exited: {stdout}\n{stderr}")
            time.sleep(0.05)
        if not port_file.exists():
            raise RuntimeError("HTTP server did not publish its port")
        base_url = json.loads(port_file.read_text())["base_url"]
        checks: dict[str, object] = {}
        health = requests.get(f"{base_url}/health", timeout=5)
        checks["health"] = health.json()
        imported = requests.post(
            f"{base_url}/import",
            params={"batch_id": "http-full", "format": "binary"},
            data=(sample_dir / "sample.kvs.bin").read_bytes(),
            headers={"Content-Type": "application/octet-stream"},
            timeout=5,
        )
        checks["import"] = {
            "status_code": imported.status_code,
            "status": imported.json()["report"]["status"],
            "summary": imported.json()["report"]["summary"],
        }
        alpha = requests.get(f"{base_url}/keys/alpha", timeout=5)
        checks["read_after_import"] = {
            "status_code": alpha.status_code,
            "value": alpha.json()["record"]["value"],
            "segment": alpha.json()["record"]["segment"],
        }
        updated = requests.put(
            f"{base_url}/keys/alpha",
            json={
                "value": "http-incremental",
                "version": 2,
                "write_timestamp_ms": BASE_MS + 30_000,
                "expires_at_ms": BASE_MS + 200_000_000,
            },
            timeout=5,
        )
        checks["incremental_write"] = {
            "status_code": updated.status_code,
            "version": updated.json()["record"]["version"],
            "segment": updated.json()["record"]["segment"],
        }
        binary_export = requests.get(f"{base_url}/export/binary", timeout=5)
        checks["binary_export"] = {
            "status_code": binary_export.status_code,
            "bytes": len(binary_export.content),
            "records": len(decode_binary(binary_export.content)),
        }
        jsonl_export = requests.get(f"{base_url}/export/jsonl?shards=2", timeout=5)
        checks["jsonl_export"] = {
            "status_code": jsonl_export.status_code,
            "bytes": len(jsonl_export.content),
            "shard_count": jsonl_export.headers.get("X-KV-Shard-Count"),
        }
        corrupt = requests.post(
            f"{base_url}/import",
            params={"batch_id": "http-corrupt"},
            data=(sample_dir / "sample_corrupt.kvs.bin").read_bytes(),
            headers={"Content-Type": "application/octet-stream"},
            timeout=5,
        )
        checks["corrupt_import"] = {
            "status_code": corrupt.status_code,
            "code": corrupt.json()["code"],
            "record_index": corrupt.json()["context"]["record_index"],
        }
        duplicate_batch = requests.post(
            f"{base_url}/import",
            params={"batch_id": "http-full", "format": "binary"},
            data=(sample_dir / "sample.kvs.bin").read_bytes(),
            headers={"Content-Type": "application/octet-stream"},
            timeout=5,
        )
        checks["duplicate_batch"] = {
            "status_code": duplicate_batch.status_code,
            "code": duplicate_batch.json()["code"],
        }
        stale_write = requests.put(
            f"{base_url}/keys/alpha",
            json={"value": "stale", "base_state_version": 0},
            timeout=5,
        )
        checks["stale_base"] = {
            "status_code": stale_write.status_code,
            "code": stale_write.json()["code"],
        }
        snapshot = requests.get(f"{base_url}/snapshot/export", timeout=5).json()[
            "snapshot"
        ]
        invalid_snapshot = json.loads(json.dumps(snapshot))
        invalid_snapshot["records"][0]["value"] = "tampered"
        invalid = requests.post(
            f"{base_url}/snapshot/switch", json=invalid_snapshot, timeout=5
        )
        checks["invalid_snapshot"] = {
            "status_code": invalid.status_code,
            "code": invalid.json()["code"],
        }
        switched = requests.post(
            f"{base_url}/snapshot/switch", json=snapshot, timeout=5
        )
        checks["snapshot_switch"] = {
            "status_code": switched.status_code,
            "snapshot_seq": switched.json()["snapshot_seq"],
        }
        stale_snapshot = requests.post(
            f"{base_url}/snapshot/switch", json=snapshot, timeout=5
        )
        checks["stale_snapshot"] = {
            "status_code": stale_snapshot.status_code,
            "code": stale_snapshot.json()["code"],
        }
        recovery = requests.get(f"{base_url}/recovery/report", timeout=5)
        checks["recovery_report"] = {
            "status_code": recovery.status_code,
            "facts_only": recovery.json()["report"]["facts_only"],
            "recovered_count": recovery.json()["report"]["recovered_count"],
        }
        checks["error_codes_covered"] = sorted(
            {
                checks["corrupt_import"]["code"],
                checks["duplicate_batch"]["code"],
                checks["stale_base"]["code"],
                checks["invalid_snapshot"]["code"],
                checks["stale_snapshot"]["code"],
            }
        )
        return checks
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def main() -> None:
    sys.path.insert(0, str(ROOT / "scripts"))
    import generate_samples

    generate_samples.main()
    sample_dir = ROOT / "samples"
    runtime = ROOT / "artifacts" / "demo_runtime"
    if runtime.exists():
        shutil.rmtree(runtime)
    runtime.mkdir(parents=True)

    output: dict[str, object] = {
        "ok": True,
        "generated_at_ms": int(time.time() * 1000),
        "workspace": str(ROOT),
        "sample_manifest": json.loads((sample_dir / "manifest.json").read_text()),
        "scenarios": {
            "direct": direct_scenarios(runtime, sample_dir),
            "segment_integrity": integrity_scenario(runtime),
            "forced_termination_recovery": crash_scenario(runtime),
            "http_api": http_scenario(runtime, sample_dir),
        },
    }
    output_path = ROOT / "artifacts" / "demo_output.json"
    output_path.write_text(
        json.dumps(output, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"ok": True, "output": str(output_path)}, indent=2))


if __name__ == "__main__":
    main()

