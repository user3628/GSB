from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from .errors import GatewayError
from .gateway import Gateway
from .models import FrameSubmission
from .store import Store
from .transport import LocalTestNetwork, Topology


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


def run_demo(output_dir: Path, *, keep_db: bool = True) -> int:
    output_dir = output_dir.resolve()
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        summary = asyncio.run(_run_demo_async(output_dir, keep_db=keep_db))
    except Exception as exc:
        failure = {
            "ok": False,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
                "code": getattr(exc, "code", None),
                "context": getattr(exc, "context", None),
            },
        }
        (output_dir / "summary.json").write_text(
            json.dumps(failure, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(f"demo failed: {exc}", file=sys.stderr)
        return 1
    print_summary(summary)
    return 0 if summary["ok"] else 1


async def _run_demo_async(output_dir: Path, *, keep_db: bool) -> dict[str, Any]:
    snapshot_v1 = json.loads((SAMPLES / "snapshot_v1.json").read_text(encoding="utf-8"))
    patch_v2 = json.loads((SAMPLES / "patch_v2.json").read_text(encoding="utf-8"))
    frames_doc = json.loads((SAMPLES / "frames.json").read_text(encoding="utf-8"))
    expected = json.loads((SAMPLES / "expected.json").read_text(encoding="utf-8"))
    topology = Topology.from_file(SAMPLES / "topology.json")
    network = LocalTestNetwork(topology)
    await network.start()
    demo_db = output_dir / "gateway.db"
    store = Store(demo_db)
    gateway = Gateway(store)
    report: dict[str, Any] = {
        "ok": False,
        "generated_at": _now(),
        "environment": {
            "python": sys.version.split()[0],
            "platform": sys.platform,
            "loopback_only": True,
            "containerized": False,
            "topology": topology.as_dict(),
        },
        "samples": {
            "snapshot": str(SAMPLES / "snapshot_v1.json"),
            "patch": str(SAMPLES / "patch_v2.json"),
            "frames": str(SAMPLES / "frames.json"),
            "expected": str(SAMPLES / "expected.json"),
        },
    }
    try:
        startup_recovery = await gateway.start()
        load_result = await gateway.load_snapshot(snapshot_v1)
        report["full_route_load"] = {
            **load_result,
            "bindings": {
                name: {
                    "channel": binding["channel"],
                    "priority": binding["priority"],
                    "egress": binding["egress"],
                    "address": snapshot_v1["egress_endpoints"][binding["egress"]],
                }
                for name, binding in gateway.public_state()["snapshot"]["bindings"].items()
            },
            "startup_recovery_found_work": startup_recovery.get("found_work"),
        }

        parallel_round_1 = await _gather(gateway, frames_doc["parallel"][:3])
        parallel_round_2 = await _gather(gateway, frames_doc["parallel"][3:])
        parallel_results = parallel_round_1 + parallel_round_2
        report["parallel_forwarding"] = {
            "rounds": [
                _compact_results(parallel_round_1),
                _compact_results(parallel_round_2),
            ],
            "all_acknowledged": all(item["ok"] for item in parallel_results),
            "sequence_check": _sequence_check(gateway, expected["parallel"]),
            "max_concurrent_channels_observed": _max_concurrent_channels(gateway),
        }

        beta_before = gateway.public_state()["sessions"]["beta"]
        patch_result = await gateway.apply_patch(patch_v2)
        beta_after_patch = gateway.public_state()["sessions"]["beta"]
        beta_probe = await gateway.submit_frame(
            FrameSubmission.model_validate(frames_doc["post_patch"][0])
        )
        report["incremental_rebuild"] = {
            "patch": patch_result,
            "before": {
                "egress": beta_before["egress"],
                "egress_label": beta_before["egress_label"],
                "stats": beta_before["stats"],
            },
            "after": {
                "egress": beta_after_patch["egress"],
                "egress_label": beta_after_patch["egress_label"],
                "stats": beta_after_patch["stats"],
            },
            "post_patch_probe": _compact_result(beta_probe),
            "cascade_verified": (
                set(patch_result["affected_sessions"]) == {"beta"}
                and beta_probe["response"]["endpoint"] == "decap-east"
                and beta_after_patch["stats"]["invalidations"]
                > (beta_before["stats"]["invalidations"] if beta_before["stats"] else 0)
            ),
        }

        pressure = frames_doc["pressure"]
        active_tasks = [
            asyncio.create_task(gateway.submit_frame(FrameSubmission.model_validate(item)))
            for item in pressure["active"]
        ]
        await asyncio.sleep(0.06)
        waiting_tasks = [
            asyncio.create_task(gateway.submit_frame(FrameSubmission.model_validate(item)))
            for item in pressure["waiting"]
        ]
        await asyncio.sleep(0.06)
        overflow_tasks = [
            asyncio.create_task(gateway.submit_frame(FrameSubmission.model_validate(item)))
            for item in pressure["overflow"]
        ]
        pressure_results = await asyncio.gather(
            *active_tasks, *waiting_tasks, *overflow_tasks, return_exceptions=True
        )
        submitted_pressure = (
            pressure["active"] + pressure["waiting"] + pressure["overflow"]
        )
        classified = [
            _classify(item, submitted)
            for item, submitted in zip(pressure_results, submitted_pressure)
        ]
        report["queue_pressure"] = {
            "queue_capacity": snapshot_v1["queue_capacity"],
            "results": classified,
            "overflow_code": next(
                (item.get("error", {}).get("code") for item in classified if not item["ok"]),
                None,
            ),
            "overflow_expected": expected["pressure"]["overflow_error"],
            "sequence_check": _sequence_check(gateway, expected["pressure"]["accepted"]),
        }

        report["channel_trajectory"] = _channel_trajectory(gateway)
        report["persisted_leases"] = gateway.latest_recovery() is not None

        crash = await _run_crash_recovery(output_dir, snapshot_v1, network)
        report["crash_recovery"] = crash

        report["frames"] = gateway.list_frames(limit=200)
        report["traces"] = gateway.list_traces(limit=2000)
        report["intents"] = gateway.list_intents(limit=500)
        report["final_state"] = gateway.public_state()
        report["overall_sequence_verification"] = _overall_sequence_verification(
            report["frames"]
        )
        report["ok"] = all(
            [
                report["full_route_load"]["accepted"],
                report["parallel_forwarding"]["all_acknowledged"],
                all(item["ok"] for item in report["parallel_forwarding"]["sequence_check"]),
                report["incremental_rebuild"]["cascade_verified"],
                report["queue_pressure"]["overflow_code"]
                == expected["pressure"]["overflow_error"],
                all(item["ok"] for item in report["queue_pressure"]["sequence_check"]),
                report["crash_recovery"]["retransmit_marked"],
                report["crash_recovery"]["lease_released"],
                report["crash_recovery"]["facts_only"],
                report["crash_recovery"]["egress_observed_before_kill"],
            ]
        )
        _write_artifacts(output_dir, report)
        return report
    finally:
        await gateway.stop()
        store.close()
        await network.stop()
        if not keep_db:
            try:
                demo_db.unlink(missing_ok=True)
                Path(str(demo_db) + "-wal").unlink(missing_ok=True)
                Path(str(demo_db) + "-shm").unlink(missing_ok=True)
            except OSError:
                pass


async def _gather(gateway: Gateway, frames: list[dict[str, Any]]) -> list[dict[str, Any]]:
    async def one(raw: dict[str, Any]) -> dict[str, Any]:
        try:
            result = await gateway.submit_frame(FrameSubmission.model_validate(raw))
            return {"ok": True, "result": result}
        except GatewayError as exc:
            return {"ok": False, **exc.as_payload(), "frame_id": raw["frame_id"]}

    return list(await asyncio.gather(*(one(raw) for raw in frames)))


async def _run_crash_recovery(
    output_dir: Path, snapshot: dict[str, Any], network: LocalTestNetwork
) -> dict[str, Any]:
    crash_dir = output_dir / "crash"
    crash_dir.mkdir(parents=True, exist_ok=True)
    db_path = crash_dir / "crash.db"
    marker_path = crash_dir / "dispatch.marker.json"
    stdout_path = crash_dir / "worker.stdout.log"
    stderr_path = crash_dir / "worker.stderr.log"
    command = [
        sys.executable,
        "-m",
        "vgw",
        "crash-worker",
        "--db",
        str(db_path),
        "--snapshot",
        str(SAMPLES / "snapshot_v1.json"),
        "--marker",
        str(marker_path),
        "--frame-id",
        "crash-alpha-1",
        "--session",
        "alpha",
        "--sequence",
        "1",
        "--delay-ms",
        "5000",
        "--timeout-ms",
        "10000",
    ]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT) + os.pathsep + environment.get("PYTHONPATH", "")
    egress_baseline = int(network.status()["egress_decoded_total"])
    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr:
        process = subprocess.Popen(
            command,
            cwd=str(ROOT),
            env=environment,
            stdout=stdout,
            stderr=stderr,
        )
        marker: dict[str, Any] | None = None
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            if marker_path.exists():
                marker = json.loads(marker_path.read_text(encoding="utf-8"))
                break
            if process.poll() is not None:
                break
            await asyncio.sleep(0.04)
        if marker is None:
            process.kill()
            process.wait(timeout=5)
            raise RuntimeError(
                f"crash worker did not persist a dispatch marker; stderr={stderr_path}"
            )
        egress_observation: dict[str, Any] | None = None
        egress_deadline = time.monotonic() + 3
        while time.monotonic() < egress_deadline:
            status = network.status()
            if int(status["egress_decoded_total"]) > egress_baseline:
                candidates = [
                    item
                    for item in status.get("recent_egress_observations", [])
                    if item["session_id"] == "alpha" and item["sequence"] == 1
                ]
                if candidates:
                    egress_observation = candidates[-1]
                    break
            await asyncio.sleep(0.02)
        facts_before = _read_crash_facts(db_path, "crash-alpha-1", "alpha")
        process.kill()
        process.wait(timeout=8)
    facts_after_kill = _read_crash_facts(db_path, "crash-alpha-1", "alpha")

    store = Store(db_path)
    recovered_gateway = Gateway(store)
    try:
        await recovered_gateway.start()
        report = recovered_gateway.latest_recovery()
        frame = recovered_gateway.get_frame("crash-alpha-1")
        leases = {item["session_id"]: item for item in recovered_gateway.public_state()["leases"]}
        lease = leases.get("alpha")
    finally:
        await recovered_gateway.stop()
        store.close()
    facts_after_recovery = _read_crash_facts(db_path, "crash-alpha-1", "alpha")
    retransmit_marked = frame["status"] == "retransmit_required"
    lease_released = bool(
        lease is not None
        and lease.get("channel_id") is None
        and lease.get("sent_unacked_seq") is None
    )
    return {
        "marker": marker,
        "facts_before_kill": facts_before,
        "egress_observed_before_kill": egress_observation is not None,
        "egress_observation": egress_observation,
        "facts_after_kill": facts_after_kill,
        "facts_after_recovery": facts_after_recovery,
        "recovery_report": report,
        "recovered_frame_status": frame["status"],
        "recovered_lease": lease,
        "retransmit_marked": retransmit_marked,
        "lease_released": lease_released,
        "facts_only": bool(report and report.get("facts_only")),
        "graceful_exit_report_available": report.get("graceful_exit_report_available")
        if report
        else None,
        "worker_exit_code": process.returncode,
    }


def _read_crash_facts(db_path: Path, frame_id: str, session_id: str) -> dict[str, Any]:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        frame = conn.execute("SELECT * FROM frames WHERE frame_id = ?", (frame_id,)).fetchone()
        lease = conn.execute("SELECT * FROM leases WHERE session_id = ?", (session_id,)).fetchone()
        intents = conn.execute(
            "SELECT * FROM intents WHERE frame_id = ? ORDER BY created_at", (frame_id,)
        ).fetchall()
        return {
            "frame": dict(frame) if frame else None,
            "lease": dict(lease) if lease else None,
            "pending_intents": [dict(row) for row in intents if row["status"] == "pending"],
            "all_intents": [dict(row) for row in intents],
        }
    finally:
        conn.close()


def _compact_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "frame_id": result["frame_id"],
        "session_id": result["session_id"],
        "sequence": result["sequence"],
        "channel_id": result["channel_id"],
        "snapshot_version": result["snapshot_version"],
        "endpoint": result["response"].get("endpoint"),
        "elapsed_ms": result["transport"].get("elapsed_ms"),
        "checksum": result["request_checksum"],
    }


def _compact_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for item in results:
        if item["ok"]:
            output.append({"ok": True, **_compact_result(item["result"])})
        else:
            output.append(
                {
                    "ok": False,
                    "frame_id": item.get("frame_id"),
                    "error": item.get("error"),
                }
            )
    return output


def _classify(value: Any, submitted: dict[str, Any] | None = None) -> dict[str, Any]:
    if isinstance(value, GatewayError):
        identity = {
            "frame_id": submitted.get("frame_id") if submitted else None,
            "session_id": submitted.get("session_id") if submitted else None,
            "sequence": submitted.get("sequence") if submitted else None,
        }
        return {"ok": False, **identity, **value.as_payload()}
    if isinstance(value, Exception):
        identity = {
            "frame_id": submitted.get("frame_id") if submitted else None,
            "session_id": submitted.get("session_id") if submitted else None,
            "sequence": submitted.get("sequence") if submitted else None,
        }
        return {
            "ok": False,
            **identity,
            "error": {
                "code": "E_INTERNAL",
                "message": str(value),
                "context": {"exception": type(value).__name__},
            },
        }
    return {"ok": True, **_compact_result(value)}


def _sequence_check(gateway: Gateway, expected: dict[str, list[int]]) -> list[dict[str, Any]]:
    rows = gateway.list_frames(limit=1000)
    acked: dict[str, list[int]] = defaultdict(list)
    for row in reversed(rows):
        if row["status"] == "acked":
            acked[row["session_id"]].append(int(row["sequence"]))
    checks = []
    for session_id, wanted in sorted(expected.items()):
        observed = acked.get(session_id, [])
        checks.append(
            {
                "session_id": session_id,
                "expected": wanted,
                "observed": observed,
                "ok": observed == wanted,
            }
        )
    return checks


def _overall_sequence_verification(frames: list[dict[str, Any]]) -> dict[str, Any]:
    acked: dict[str, list[int]] = defaultdict(list)
    for row in reversed(frames):
        if row["status"] == "acked":
            acked[row["session_id"]].append(int(row["sequence"]))
    result = {}
    for session_id, sequences in sorted(acked.items()):
        unique_sorted = sorted(set(sequences))
        contiguous = unique_sorted == list(
            range(1, unique_sorted[-1] + 1)
        ) if unique_sorted else True
        result[session_id] = {
            "acked_sequences": unique_sorted,
            "contiguous_from_one": contiguous,
            "no_duplicate_acked_sequence": len(unique_sorted) == len(sequences),
            "ok": contiguous and len(unique_sorted) == len(sequences),
        }
    return result


def _channel_trajectory(gateway: Gateway) -> list[dict[str, Any]]:
    events = [
        item
        for item in gateway.list_traces(limit=5000)
        if item["kind"] in {"channel_acquired", "channel_released"}
    ]
    trajectory: list[dict[str, Any]] = []
    open_by_channel: dict[int, dict[str, Any]] = {}
    for event in events:
        channel_id = event["channel_id"]
        if channel_id is None:
            continue
        if event["kind"] == "channel_acquired":
            open_by_channel[channel_id] = {
                "channel_id": channel_id,
                "session_id": event["session_id"],
                "frame_id": event["frame_id"],
                "sequence": event["sequence"],
                "egress": event["egress"],
                "acquired_at": event["at"],
            }
        else:
            item = open_by_channel.pop(channel_id, None)
            if item is not None:
                item["released_at"] = event["at"]
                item["release_reason"] = (event.get("detail") or {}).get("reason")
                trajectory.append(item)
    for item in open_by_channel.values():
        item["released_at"] = None
        trajectory.append(item)
    return trajectory


def _max_concurrent_channels(gateway: Gateway) -> int:
    intervals = _channel_trajectory(gateway)
    points = []
    for item in intervals:
        start = _parse_iso(item["acquired_at"])
        end = _parse_iso(item.get("released_at")) if item.get("released_at") else start + 10
        points.append((start, 1))
        points.append((end, -1))
    points.sort()
    current = maximum = 0
    for _, delta in points:
        current += delta
        maximum = max(maximum, current)
    return maximum


def _parse_iso(value: str) -> float:
    from datetime import datetime

    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _write_artifacts(output_dir: Path, report: dict[str, Any]) -> None:
    (output_dir / "summary.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (output_dir / "traces.json").write_text(
        json.dumps(report["traces"], ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (output_dir / "recovery_diff.json").write_text(
        json.dumps(report["crash_recovery"], ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (output_dir / "report.md").write_text(_markdown(report), encoding="utf-8")


def _markdown(report: dict[str, Any]) -> str:
    parallel = report["parallel_forwarding"]
    rebuild = report["incremental_rebuild"]
    pressure = report["queue_pressure"]
    crash = report["crash_recovery"]
    lines = [
        "# Loopback Tunnel Gateway · Verification Report",
        "",
        f"- Generated: `{report['generated_at']}`",
        f"- Python: `{report['environment']['python']}` · platform `{report['environment']['platform']}`",
        "- Transport: real loopback UDP via carrier relay to egress decapsulator",
        "- Persistence: SQLite WAL, `synchronous=FULL`, pending intent before dispatch",
        "",
        "## 1. Full route load",
        "",
        f"- Snapshot version: **{report['full_route_load']['version']}**",
        f"- Digest: `{report['full_route_load']['digest']}`",
        f"- Sessions: {', '.join(sorted(report['full_route_load']['bindings']))}",
        "",
        "## 2. Multi-session parallel forwarding",
        "",
        f"- All frames acknowledged: **{parallel['all_acknowledged']}**",
        f"- Maximum simultaneous channels observed: **{parallel['max_concurrent_channels_observed']}**",
        "",
        "| Session | Expected | Observed | Result |",
        "|---|---|---|---|",
    ]
    for check in parallel["sequence_check"]:
        lines.append(
            f"| {check['session_id']} | {check['expected']} | {check['observed']} | {'PASS' if check['ok'] else 'FAIL'} |"
        )
    lines += [
        "",
        "## 3. Incremental rebuild",
        "",
        f"- Patch: v{rebuild['patch']['base_version']} → v{rebuild['patch']['version']}",
        f"- Affected scope: {', '.join(rebuild['patch']['affected_sessions'])}",
        f"- Before: `{rebuild['before']['egress']}` / label `{rebuild['before']['egress_label']}`",
        f"- After: `{rebuild['after']['egress']}` / label `{rebuild['after']['egress_label']}`",
        f"- Post-patch probe endpoint: `{rebuild['post_patch_probe']['endpoint']}`",
        f"- Stats invalidations: {rebuild['after']['stats']['invalidations']}",
        f"- Atomic cascade verified: **{rebuild['cascade_verified']}**",
        "",
        "## 4. Channel pressure and bounded queue",
        "",
        f"- Queue capacity: {pressure['queue_capacity']}",
        f"- Overflow business code: `{pressure['overflow_code']}` (expected `{pressure['overflow_expected']}`)",
        "",
        "| Frame | Session | Seq | Result | Channel | Error |",
        "|---|---|---|---|---|---|",
    ]
    for item in pressure["results"]:
        lines.append(
            f"| {item.get('frame_id', '—')} | {item.get('session_id', '—')} | {item.get('sequence', '—')} | "
            f"{'ACK' if item['ok'] else 'REJECT'} | {item.get('channel_id', '—')} | "
            f"{item.get('error', {}).get('code', '—')} |"
        )
    lines += [
        "",
        "## 5. Forced termination and recovery",
        "",
        f"- Worker exit code after kill: `{crash['worker_exit_code']}`",
        f"- Frame after recovery: `{crash['recovered_frame_status']}`",
        f"- Datagram observed at egress before kill: **{crash['egress_observed_before_kill']}**",
        f"- Retransmit marked from persisted facts: **{crash['retransmit_marked']}**",
        f"- Lease channel cleared: **{crash['lease_released']}**",
        f"- Facts-only report: **{crash['facts_only']}**",
        f"- Graceful exit report claimed: **{crash['graceful_exit_report_available']}**",
        "",
        "### Lease difference",
        "",
        "```json",
        json.dumps(crash["recovery_report"]["lease_differences"], ensure_ascii=False, indent=2),
        "```",
        "",
        "### Retransmit actions",
        "",
        "```json",
        json.dumps(crash["recovery_report"]["retransmit_required"], ensure_ascii=False, indent=2),
        "```",
        "",
        "## 6. Final contiguous sequence verification",
        "",
        "| Session | Acked sequences | Contiguous | Duplicate-free |",
        "|---|---|---|---|",
    ]
    for session_id, item in report["overall_sequence_verification"].items():
        lines.append(
            f"| {session_id} | {item['acked_sequences']} | {item['contiguous_from_one']} | {item['no_duplicate_acked_sequence']} |"
        )
    lines += [
        "",
        "## Channel occupancy trajectory (first 30 intervals)",
        "",
        "| Channel | Session | Seq | Egress | Acquired | Released |",
        "|---|---|---|---|---|---|",
    ]
    for item in report["channel_trajectory"][:30]:
        lines.append(
            f"| {item['channel_id']} | {item['session_id']} | {item['sequence']} | {item['egress']} | {item['acquired_at']} | {item['released_at']} |"
        )
    lines += [
        "",
        "Recovery is intentionally limited to committed SQLite facts; no complete exit report is claimed for the forcibly terminated process.",
        "",
    ]
    return "\n".join(lines)


def print_summary(report: dict[str, Any]) -> None:
    parallel = report["parallel_forwarding"]
    rebuild = report["incremental_rebuild"]
    pressure = report["queue_pressure"]
    crash = report["crash_recovery"]
    print("=" * 78)
    print("LOOPBACK TUNNEL GATEWAY · DETERMINISTIC DEMO")
    print("=" * 78)
    print(f"snapshot        v{report['full_route_load']['version']}  {report['full_route_load']['digest']}")
    print(f"parallel         all_ack={parallel['all_acknowledged']}  max_channels={parallel['max_concurrent_channels_observed']}")
    for check in parallel["sequence_check"]:
        print(f"  {check['session_id']:<10} expected={check['expected']} observed={check['observed']} ok={check['ok']}")
    print(f"route rebuild   v{rebuild['patch']['base_version']} -> v{rebuild['patch']['version']} scope={rebuild['patch']['affected_sessions']}")
    print(f"  beta egress   {rebuild['before']['egress']} -> {rebuild['after']['egress']}  endpoint={rebuild['post_patch_probe']['endpoint']}")
    print(f"queue pressure  overflow={pressure['overflow_code']} expected={pressure['overflow_expected']} capacity={pressure['queue_capacity']}")
    print(f"crash recovery  frame={crash['recovered_frame_status']} retransmit={crash['retransmit_marked']} lease_released={crash['lease_released']}")
    print(f"  egress seen   {crash['egress_observed_before_kill']}  observation={crash['egress_observation']}")
    print(f"  facts_only    {crash['facts_only']}  graceful_exit_report={crash['graceful_exit_report_available']}")
    print(f"verification    {'PASS' if report['ok'] else 'FAIL'}")
    print("=" * 78)
    print("artifacts: artifacts/demo/current/{summary.json,report.md,traces.json,recovery_diff.json}")


def _now() -> str:
    from .store import utc_now

    return utc_now()

