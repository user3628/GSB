#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from replica_repair.http_client import LocalHttpClient  # noqa: E402
from replica_repair.numerics import canonical_json  # noqa: E402


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_report(name: str, round_id: str) -> dict[str, Any]:
    report = copy.deepcopy(load_json(ROOT / "samples" / "reports" / name))
    report["report_id"] = f"{round_id}:{report['report_id']}"
    return report


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def expect_success(result: tuple[int, dict[str, Any]], context: str) -> dict[str, Any]:
    status, payload = result
    expect(status in (200, 202), f"{context} 失败: HTTP {status} {payload}")
    expect(payload.get("ok") is True, f"{context} 业务失败: {payload}")
    return payload


def start_server(
    db_path: Path,
    log_path: Path,
    *,
    port: int,
    delay_ms: int = 0,
) -> tuple[subprocess.Popen[bytes], Any]:
    env = os.environ.copy()
    env["REPAIR_AGGREGATION_DELAY_MS"] = str(delay_ms)
    env["REPAIR_CLOCK_BASE_ISO"] = "2000-01-01T00:00:03Z"
    log_handle = log_path.open("ab")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "replica_repair.server",
            "--db",
            str(db_path),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=ROOT,
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    return process, log_handle


def stop_server(process: subprocess.Popen[bytes], log_handle: Any) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    log_handle.close()


def remove_database(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix)
        if candidate.exists():
            candidate.unlink()


def create_round(
    client: LocalHttpClient,
    *,
    round_id: str,
    group_id: str,
    group_version: int,
    deadline: str,
    target_progress: float,
    tolerance: float,
) -> dict[str, Any]:
    payload = {
        "round_id": round_id,
        "group_id": group_id,
        "group_version": group_version,
        "deadline": deadline,
        "target_progress": target_progress,
        "relative_tolerance": tolerance,
    }
    return expect_success(client.post("/v1/rounds", payload), f"创建轮次 {round_id}")


def submit(
    client: LocalHttpClient,
    *,
    round_id: str,
    group_id: str,
    group_version: int,
    reports: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "group_id": group_id,
        "group_version": group_version,
        "reports": reports,
    }
    return expect_success(
        client.post(f"/v1/rounds/{round_id}/reports", payload),
        f"提交 {round_id} 上报",
    )


def aggregate(
    client: LocalHttpClient,
    *,
    round_id: str,
    normalization: str,
    strict_wait: bool = False,
    grace_ms: int = 0,
) -> dict[str, Any]:
    payload = {
        "normalization": normalization,
        "strict_wait": strict_wait,
        "grace_ms": grace_ms,
        "max_wait_ms": 5000,
    }
    return expect_success(
        client.post(f"/v1/rounds/{round_id}/aggregate", payload),
        f"聚合 {round_id}/{normalization}",
    )


def compact_conclusion(conclusion: dict[str, Any]) -> dict[str, Any]:
    estimates = conclusion["estimates"]
    convergence = conclusion["convergence"]
    diagnostics = conclusion["sum_diagnostics"]["weighted_block_count"]
    return {
        "normalization": conclusion["normalization"]["mode"],
        "normalized_weights": conclusion["normalization"]["normalized_weights"],
        "present_replicas": conclusion["normalization"]["present_replicas"],
        "missing_replicas": conclusion["normalization"]["missing_replicas"],
        "estimated_total_bytes": estimates["estimated_total_bytes"],
        "estimated_synced_bytes": estimates["estimated_synced_bytes"],
        "pending_repair_bytes": estimates["pending_repair_bytes"],
        "progress": estimates["progress"],
        "target_progress": conclusion["round"]["target_progress"],
        "relative_tolerance": conclusion["round"]["relative_tolerance"],
        "converged": convergence["converged"],
        "delta": convergence["comparison"]["delta"],
        "allowed_delta": convergence["comparison"]["allowed_delta"],
        "late_report_count_at_seal": conclusion["reports"]["late_report_count_at_seal"],
        "participating_late_report_ids": conclusion["reports"][
            "participating_late_report_ids"
        ],
        "kahan_abs_error": diagnostics["kahan_abs_error"],
        "kahan_error_bound": diagnostics["kahan_error_bound"],
        "naive_abs_error": diagnostics["naive_abs_error"],
        "naive_error_bound": diagnostics["naive_error_bound"],
        "rounding_error_bound": conclusion["rounding"][
            "cumulative_relative_error_bound"
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fixed-sample end-to-end demo")
    parser.add_argument("--port", type=int, default=18765)
    parser.add_argument(
        "--json-output",
        default=str(ROOT / "artifacts" / "demo_output.json"),
    )
    args = parser.parse_args(argv)

    db_path = ROOT / "data" / "repair_demo.sqlite3"
    log_path = ROOT / "data" / "demo_server.log"
    remove_database(db_path)
    if log_path.exists():
        log_path.unlink()

    process, log_handle = start_server(db_path, log_path, port=args.port)
    client = LocalHttpClient(f"http://127.0.0.1:{args.port}")
    try:
        health = client.wait_until_healthy()
        group_doc = load_json(ROOT / "samples" / "groups.json")
        group = group_doc["groups"][0]
        group_payload = expect_success(
            client.post("/v1/groups", group), "创建固定副本组"
        )
        group_id = group_payload["group"]["group_id"]
        group_version = group_payload["group"]["version"]
        deadline = load_json(ROOT / "samples" / "rounds.json")["round_defaults"][
            "deadline"
        ]

        mode_results: list[dict[str, Any]] = []
        created_rounds: dict[str, dict[str, Any]] = {}
        for round_id, _normalization in (
            ("round-equal", "ALL_EQUAL"),
            ("round-missing", "MISSING_ZERO"),
            ("round-reported", "REPORTED_RATIO"),
        ):
            created_rounds[round_id] = create_round(
                client,
                round_id=round_id,
                group_id=group_id,
                group_version=group_version,
                deadline=deadline,
                target_progress=0.25,
                tolerance=1e-8,
            )
        for round_id, normalization in (
            ("round-equal", "ALL_EQUAL"),
            ("round-reported", "REPORTED_RATIO"),
            ("round-missing", "MISSING_ZERO"),
        ):
            created = created_rounds[round_id]
            reports = [
                load_report("r0_ontime.json", round_id),
                load_report("r2_ontime.json", round_id),
            ]
            if normalization == "REPORTED_RATIO":
                reports.append(load_report("r1_late.json", round_id))
            submit_result = submit(
                client,
                round_id=round_id,
                group_id=group_id,
                group_version=group_version,
                reports=reports,
            )
            expect(
                len(submit_result["late_report_ids"])
                == (1 if normalization == "REPORTED_RATIO" else 0),
                f"{round_id} 迟到分类不符合固定样本预期",
            )
            first = aggregate(
                client,
                round_id=round_id,
                normalization=normalization,
            )
            second = aggregate(
                client,
                round_id=round_id,
                normalization=normalization,
            )
            expect(first["reused"] is False, f"{round_id} 首次聚合不应复用")
            expect(second["reused"] is True, f"{round_id} 重复聚合应复用")
            expect(
                canonical_json(first["conclusion"])
                == canonical_json(second["conclusion"]),
                f"{round_id} 重复聚合结论不是逐位一致",
            )
            query = expect_success(
                client.get(f"/v1/rounds/{round_id}/conclusion"),
                f"查询 {round_id} 结论",
            )
            expect(
                query["conclusion_digest"] == first["conclusion_digest"],
                f"{round_id} 查询摘要与聚合摘要不一致",
            )
            compact = compact_conclusion(first["conclusion"])
            compact.update(
                {
                    "round_id": round_id,
                    "conclusion_digest": first["conclusion_digest"],
                    "repeat_aggregation_identical": True,
                    "adopted_late_report_ids": created.get(
                        "adopted_late_report_ids", []
                    ),
                }
            )
            mode_results.append(compact)

        conflict_status, conflict_payload = client.post(
            "/v1/rounds/round-equal/aggregate",
            {"normalization": "REPORTED_RATIO", "strict_wait": False},
        )
        expect(
            conflict_status == 409
            and conflict_payload.get("code") == "E_CONFLICTING_ROUND",
            "不同归一化方案未返回 E_CONFLICTING_ROUND",
        )

        late_round = create_round(
            client,
            round_id="round-late-next",
            group_id=group_id,
            group_version=group_version,
            deadline=deadline,
            target_progress=0.25,
            tolerance=1e-8,
        )
        expect(
            any(
                report_id.endswith("r1-late")
                for report_id in late_round.get("adopted_late_report_ids", [])
            ),
            "下一轮未采纳迟到上报",
        )
        submit(
            client,
            round_id="round-late-next",
            group_id=group_id,
            group_version=group_version,
            reports=[
                load_report("r0_next_round.json", "round-late-next"),
                load_report("r2_next_round.json", "round-late-next"),
            ],
        )
        late_next = aggregate(
            client,
            round_id="round-late-next",
            normalization="REPORTED_RATIO",
        )
        late_next_compact = compact_conclusion(late_next["conclusion"])
        reported_base = next(
            item for item in mode_results if item["round_id"] == "round-reported"
        )
        late_difference = {
            "base_round": "round-reported",
            "late_round": "round-late-next",
            "base_pending_repair_bytes": reported_base["pending_repair_bytes"],
            "late_pending_repair_bytes": late_next_compact["pending_repair_bytes"],
            "pending_delta_bytes": (
                late_next_compact["pending_repair_bytes"]
                - reported_base["pending_repair_bytes"]
            ),
            "base_progress": reported_base["progress"],
            "late_progress": late_next_compact["progress"],
            "adopted_late_report_ids": late_round.get(
                "adopted_late_report_ids", []
            ),
        }

        create_round(
            client,
            round_id="round-tight-tolerance",
            group_id=group_id,
            group_version=group_version,
            deadline=deadline,
            target_progress=0.25,
            tolerance=1e-12,
        )
        submit(
            client,
            round_id="round-tight-tolerance",
            group_id=group_id,
            group_version=group_version,
            reports=[
                load_report("r0_ontime.json", "round-tight-tolerance"),
                load_report("r2_ontime.json", "round-tight-tolerance"),
            ],
        )
        tight = aggregate(
            client,
            round_id="round-tight-tolerance",
            normalization="ALL_EQUAL",
        )
        tight_compact = compact_conclusion(tight["conclusion"])
        equal_base = next(
            item for item in mode_results if item["round_id"] == "round-equal"
        )
        tolerance_contract = {
            "loose_round": "round-equal",
            "loose_tolerance": equal_base["relative_tolerance"],
            "loose_progress": equal_base["progress"],
            "loose_converged": equal_base["converged"],
            "tight_round": "round-tight-tolerance",
            "tight_tolerance": tight_compact["relative_tolerance"],
            "tight_progress": tight_compact["progress"],
            "tight_converged": tight_compact["converged"],
            "progress_identical": (
                equal_base["progress"] == tight_compact["progress"]
            ),
            "tight_delta": tight_compact["delta"],
            "tight_allowed_delta": tight_compact["allowed_delta"],
        }
        expect(
            tolerance_contract["progress_identical"]
            and tolerance_contract["loose_converged"]
            and not tolerance_contract["tight_converged"],
            "容差对照没有产生稳定的真/假结论",
        )

        after_seal = submit(
            client,
            round_id="round-late-next",
            group_id=group_id,
            group_version=group_version,
            reports=[load_report("r3_late_after_seal.json", "late-after-seal")],
        )
        expect(
            "ARRIVED_AFTER_SEAL" in after_seal["late_reasons"].values(),
            "封版后上报未标记 ARRIVED_AFTER_SEAL",
        )

        logical_now = datetime.fromisoformat(
            client.wait_until_healthy()["now"].replace("Z", "+00:00")
        )
        wait_deadline = (
            logical_now + timedelta(milliseconds=1200)
        ).isoformat().replace("+00:00", "Z")
        wait_round = create_round(
            client,
            round_id="round-wait",
            group_id=group_id,
            group_version=group_version,
            deadline=wait_deadline,
            target_progress=0.5,
            tolerance=0.1,
        )
        expect(
            any(
                report_id.endswith("r3-late-after-seal")
                for report_id in wait_round.get("adopted_late_report_ids", [])
            ),
            "严格等待轮次未采纳封版后迟到上报",
        )
        submit(
            client,
            round_id="round-wait",
            group_id=group_id,
            group_version=group_version,
            reports=[
                load_report("r0_next_round.json", "round-wait-extra"),
                load_report("r2_next_round.json", "round-wait-extra"),
            ],
        )
        waited = aggregate(
            client,
            round_id="round-wait",
            normalization="MISSING_ZERO",
            strict_wait=True,
            grace_ms=300,
        )
        timing = waited["conclusion"]["timing"]
        expect(
            "WAIT_FOR_DEADLINE" in timing["delay_sources"]
            and "WAIT_FOR_MISSING_REPLICAS" in timing["delay_sources"],
            "严格等待没有标注完整延迟来源",
        )

        invalid_round_id = "round-invalid-batch"
        create_round(
            client,
            round_id=invalid_round_id,
            group_id=group_id,
            group_version=group_version,
            deadline=deadline,
            target_progress=0.25,
            tolerance=1e-8,
        )
        bad_report = load_report("r2_ontime.json", invalid_round_id)
        bad_report["block_count"] = -1
        invalid_status, invalid_payload = client.post(
            f"/v1/rounds/{invalid_round_id}/reports",
            {
                "group_id": group_id,
                "group_version": group_version,
                "reports": [
                    load_report("r0_ontime.json", invalid_round_id),
                    bad_report,
                ],
            },
        )
        expect(
            invalid_status == 422
            and invalid_payload.get("code") == "E_REPORT_INVALID",
            "非法批次未返回 E_REPORT_INVALID",
        )
        invalid_round_status = expect_success(
            client.get(f"/v1/rounds/{invalid_round_id}"),
            "查询非法批次轮次",
        )
        expect(
            invalid_round_status["round"]["eligible_report_count"] == 0,
            "非法批次发生了部分入库",
        )

        summary = {
            "service_url": f"http://127.0.0.1:{args.port}",
            "database": str(db_path.relative_to(ROOT)),
            "samples": "samples/",
            "three_normalizations": mode_results,
            "conflicting_round": {
                "http_status": conflict_status,
                "code": conflict_payload.get("code"),
            },
            "late_report_estimate_difference": late_difference,
            "tolerance_contract": tolerance_contract,
            "late_after_seal": {
                "http_status": 202,
                "late_reasons": after_seal["late_reasons"],
                "queued_for_next_round": True,
            },
            "strict_wait": {
                "delay_sources": timing["delay_sources"],
                "delay_ms": timing["delay_ms"],
                "closed_early": timing["closed_early"],
            },
            "invalid_batch_atomicity": {
                "code": invalid_payload.get("code"),
                "eligible_report_count_after_rejection": invalid_round_status[
                    "round"
                ]["eligible_report_count"],
            },
            "repeat_aggregation": {
                "all_rounds_reused": all(
                    item["repeat_aggregation_identical"] for item in mode_results
                )
            },
        }
        output_path = Path(args.json_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print(f"\n演示摘要已写入: {output_path}")
        return 0
    finally:
        stop_server(process, log_handle)


if __name__ == "__main__":
    raise SystemExit(main())

