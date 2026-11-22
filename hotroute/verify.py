from __future__ import annotations

import json
import os
import platform
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import numpy as np

from . import samples
from .utils import pretty_json, utc_now


class VerificationFailure(AssertionError):
    pass


def require(condition: bool, message: str, context: Any | None = None) -> None:
    if not condition:
        raise VerificationFailure(message if context is None else f"{message}: {context}")


class LocalHttpClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        timeout: float = 20.0,
    ) -> tuple[int, dict[str, Any]]:
        data = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = json.loads(response.read().decode("utf-8"))
                return int(response.status), body
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                body = json.loads(raw)
            except json.JSONDecodeError:
                body = {"ok": False, "raw": raw}
            return int(exc.code), body

    def success(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        expected_status: int | tuple[int, ...] = 200,
        timeout: float = 20.0,
    ) -> dict[str, Any]:
        status, body = self.request(method, path, payload, timeout=timeout)
        expected = (expected_status,) if isinstance(expected_status, int) else expected_status
        require(status in expected, f"{method} {path} returned HTTP {status}", body)
        require(body.get("ok") is True, f"{method} {path} did not return ok=true", body)
        return body["data"]

    def error(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        status, body = self.request(method, path, payload)
        require(body.get("ok") is False, f"{method} {path} unexpectedly succeeded", body)
        return status, body


def free_loopback_port() -> int:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])
    finally:
        probe.close()


def port_available(port: int) -> bool:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        probe.close()


class LocalServer:
    def __init__(
        self,
        db_path: Path,
        port: int,
        log_path: Path,
        env_extra: dict[str, str] | None = None,
    ) -> None:
        self.db_path = db_path
        self.port = port
        self.log_path = log_path
        self.env_extra = env_extra or {}
        self.process: subprocess.Popen[str] | None = None
        self.client = LocalHttpClient(f"http://127.0.0.1:{port}")
        self._log_handle = None

    def start(self) -> None:
        require(port_available(self.port), f"loopback port {self.port} is not available")
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_handle = self.log_path.open("w", encoding="utf-8")
        env = os.environ.copy()
        env.update(self.env_extra)
        env["PYTHONUNBUFFERED"] = "1"
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "hotroute",
                "serve",
                "--db",
                str(self.db_path),
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
            ],
            cwd=str(Path(__file__).resolve().parent.parent),
            env=env,
            stdout=self._log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.time() + 20
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise VerificationFailure(
                    f"HTTP server exited during startup; log={self.log_path}\n{self.log_tail()}"
                )
            try:
                status, body = self.client.request("GET", "/health", timeout=1)
                if status == 200 and body.get("ok"):
                    return
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                pass
            time.sleep(0.1)
        raise VerificationFailure(f"HTTP server did not become ready; log={self.log_path}")

    def log_tail(self, lines: int = 80) -> str:
        if not self.log_path.exists():
            return ""
        content = self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(content[-lines:])

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=8)
        if self._log_handle is not None:
            self._log_handle.flush()
            self._log_handle.close()
            self._log_handle = None

    def crash(self) -> None:
        require(self.process is not None, "server process does not exist")
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=8)
        if self._log_handle is not None:
            self._log_handle.flush()
            self._log_handle.close()
            self._log_handle = None

    def __enter__(self) -> "LocalServer":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()


def _switch_record(name: str, response: dict[str, Any]) -> dict[str, Any]:
    manifest = response["migrations"][0]
    return {
        "name": name,
        "snapshot_seq": response["snapshot_seq"],
        "state_version": response["state_version"],
        "active_models": response["active_models"],
        "model_id": manifest["model_id"],
        "old_version": manifest["old_version"],
        "new_version": manifest["new_version"],
        "diff_mask": manifest["diff"]["mask"],
        "diff_names": manifest["diff"]["names"],
        "in_place_migratable": manifest["diff"]["in_place_migratable"],
        "mode": manifest["diff"]["mode"],
        "rebuild": manifest["rebuild"],
        "migrate": manifest["migrate"],
        "retained": manifest["retained"],
        "created": manifest["created"],
        "updated": manifest["updated"],
        "artifacts": manifest["artifacts"],
        "atomic_transaction": manifest["atomic_transaction"],
    }


def _binding_consistency(
    first: dict[str, Any],
    second: dict[str, Any],
) -> dict[str, Any]:
    def output_summaries(response: dict[str, Any]) -> dict[str, Any]:
        return {
            node_id: {
                "shape": summary["shape"],
                "dtype": summary["dtype"],
                "content_sha256": summary["content_sha256"],
            }
            for node_id, summary in response["result"]["outputs"].items()
        }

    return {
        "request_id": first["request_id"],
        "bound_versions": [first["bound_version"], second["bound_version"]],
        "first_digest": first["result"]["result_digest"],
        "second_digest": second["result"]["result_digest"],
        "bitwise_identical": first["result"]["result_digest"] == second["result"]["result_digest"],
        "first_outputs": output_summaries(first),
        "second_outputs": output_summaries(second),
        "first_cached": first.get("cached", False),
        "second_cached": second.get("cached", False),
    }


def _write_markdown(path: Path, result: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# HotRoute 本地验证报告",
        "",
        f"- 生成时间：`{result['generated_at']}`",
        f"- 总体结论：**{'PASS' if result['passed'] else 'FAIL'}**",
        f"- 数据库：`{result['run']['db_path']}`",
        f"- 执行环境：`{result['run']['platform']}` / Python `{result['run']['python']}` / "
        f"NumPy `{result['run']['numpy']}` / SQLite `{result['run']['sqlite']}`",
        "- 验证方式：真实回环 HTTP 服务 + SQLite + NumPy；强制终止步骤由外部 `process.kill()` 完成。",
        "",
        "## 热切换差异与迁移清单",
        "",
        "| 场景 | 新版本 | diff mask | 差异类别 | 原地迁移 | 重建 | 迁移 | 保留 |",
        "|---|---|---:|---|---|---|---|---|",
    ]
    for switch in result.get("switches", []):
        lines.append(
            "| {name} | `{version}` | `{mask}` | {names} | {in_place} | {rebuild} | {migrate} | {retained} |".format(
                name=switch["name"],
                version=switch["new_version"],
                mask=switch["diff_mask"],
                names=", ".join(switch["diff_names"]) or "(none)",
                in_place="是" if switch["in_place_migratable"] else "否",
                rebuild=", ".join(switch["rebuild"]) or "-",
                migrate=", ".join(switch["migrate"]) or "-",
                retained=", ".join(switch["retained"]) or "-",
            )
        )
    gray = result.get("gray", {})
    lines.extend(
        [
            "",
            "## 灰度与在途绑定",
            "",
            f"- 定点比例：`{gray.get('fixed_point')}`，归一化舍入误差：`{gray.get('ratio_rounding_error')}`",
            f"- 裁决序列：`{gray.get('decisions')}`",
            f"- 同一请求重复裁决一致：`{gray.get('replay_same_target')}`",
            f"- 切换前绑定、切换后完成的版本：`{result.get('in_flight', {}).get('bound_version')}`",
            f"- 在途请求与切换前参考推理逐位一致：`{result.get('in_flight', {}).get('bitwise_identical')}`",
            "",
            "## 并发基础版本冲突",
            "",
            f"- 成功者：`{result.get('concurrency', {}).get('winner_version')}`",
            f"- 失败者业务码：`{result.get('concurrency', {}).get('loser_code')}`",
            f"- 同一基础版本仅一个成功：`{result.get('concurrency', {}).get('single_winner')}`",
            "",
            "## 强制终止恢复",
            "",
            f"- 恢复前快照序号：`{result.get('recovery', {}).get('before_snapshot_seq')}`",
            f"- 恢复后快照序号：`{result.get('recovery', {}).get('after_snapshot_seq')}`",
            f"- 未完成意图数：`{result.get('recovery', {}).get('aborted_intent_count')}`",
            f"- 被中断在途绑定数：`{result.get('recovery', {}).get('interrupted_binding_count')}`",
            f"- 退出报告可用：`{result.get('recovery', {}).get('exit_report_available')}`",
            f"- 恢复后二次热切换成功：`{result.get('recovery', {}).get('post_recovery_switch')}`",
            f"- 恢复后路由与缓存快照一致：`{result.get('recovery', {}).get('route_cache_consistent')}`",
            "",
            "## 推理摘要一致性",
            "",
        ]
    )
    for item in result.get("inference_consistency", []):
        lines.append(
            f"- `{item['request_id']}`：`{item['first_digest'][:16]}` vs `{item['second_digest'][:16]}` → "
            f"逐位一致 = `{item['bitwise_identical']}`"
        )
    lines.extend(
        [
            "",
            "## 结论项",
            "",
        ]
    )
    for key, value in result.get("conclusions", {}).items():
        lines.append(f"- `{key}` = `{value}`")
    if result.get("error"):
        lines.extend(["", "## 失败信息", "", "```text", result["error"], "```"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_verification(
    output_path: Path,
    markdown_path: Path,
    keep_run_dir: bool = False,
) -> dict[str, Any]:
    output_path = Path(output_path)
    markdown_path = Path(markdown_path)
    run_dir = Path("artifacts") / "run"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    db_path = run_dir / "hotroute.db"
    marker_path = run_dir / "intent.marker"
    port = free_loopback_port()

    result: dict[str, Any] = {
        "generated_at": utc_now(),
        "passed": False,
        "run": {
            "db_path": str(db_path.resolve()),
            "loopback_port": port,
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "numpy": np.__version__,
            "sqlite": sqlite3.sqlite_version,
        },
        "steps": [],
        "switches": [],
        "inference_consistency": [],
        "conclusions": {},
    }
    normal_server: LocalServer | None = None
    fault_server: LocalServer | None = None
    recovery_server: LocalServer | None = None
    try:
        # 1. Full snapshot file submission and monotonic sequence check.
        snapshot_payload = samples.initial_snapshot()
        snapshot_file = samples.write_snapshot_file(run_dir / "snapshot-0001.json", snapshot_payload)
        require(snapshot_file.exists(), "snapshot file was not written")
        normal_server = LocalServer(db_path, port, run_dir / "server-normal.log")
        normal_server.start()
        client = normal_server.client
        snapshot_result = client.success(
            "POST", "/api/snapshots", samples.read_snapshot_file(snapshot_file), 201
        )
        state_1 = client.success("GET", "/api/state")
        require(state_1["snapshot_seq"] == 1, "initial snapshot sequence mismatch", state_1)
        result["steps"].append({"name": "snapshot_file_submit", "state_version": snapshot_result["state_version"]})

        status, stale_snapshot = client.error("POST", "/api/snapshots", snapshot_payload)
        require(status == 409 and stale_snapshot["code"] == "E_SNAPSHOT_STALE",
                "non-monotonic snapshot was not rejected", stale_snapshot)
        result["snapshot_monotonic"] = {
            "rejected_code": stale_snapshot["code"],
            "current_snapshot_seq": stale_snapshot["context"]["current_snapshot_seq"],
        }

        # 2. Bind an in-flight request before the quant-only switch.
        inflight_old = client.success(
            "POST",
            "/api/grays/decide",
            {"request_id": "inflight-before-quant", "model_id": samples.MODEL_ID},
        )
        require(inflight_old["target_version"] == "v1", "initial binding target mismatch", inflight_old)

        # 3. Quant-only switch: graph in-place, no shape cache rebuild.
        quant_body = samples.quant_increment(state_1["state_version"], snapshot_seq=2)
        quant_result = client.success("POST", "/api/increments", quant_body, 201)
        quant_switch = _switch_record("quant_only", quant_result)
        result["switches"].append(quant_switch)
        require(quant_switch["diff_mask"] == 4, "quant switch diff mask must be 4", quant_switch)
        require(quant_switch["in_place_migratable"], "quant switch must be in-place", quant_switch)
        require("preprocess_cache" in quant_switch["migrate"], "preprocess cache was not migrated", quant_switch)
        require("inflight_index" in quant_switch["migrate"], "inflight index was not migrated", quant_switch)
        require("preprocess_cache" not in quant_switch["rebuild"], "preprocess cache was needlessly rebuilt", quant_switch)
        require("inflight_index" not in quant_switch["rebuild"], "inflight index was needlessly rebuilt", quant_switch)
        state_2 = client.success("GET", "/api/state")
        require(state_2["snapshot_seq"] == 2, "quant switch sequence mismatch", state_2)
        inflight_old_inference = client.success(
            "POST",
            "/api/infer",
            {
                "request_id": "inflight-before-quant",
                "model_id": samples.MODEL_ID,
                "seed": 111,
            },
        )
        require(
            inflight_old_inference["bound_version"] == "v1",
            "in-flight request was rebound after quant switch",
            inflight_old_inference,
        )

        # Bind a v2 request before the shape switch, and make a reference inference.
        inflight_shape = client.success(
            "POST",
            "/api/grays/decide",
            {"request_id": "inflight-before-shape", "model_id": samples.MODEL_ID},
        )
        require(inflight_shape["target_version"] == "v2-quant", "post-quant route target mismatch", inflight_shape)
        v2_reference = client.success(
            "POST",
            "/api/infer",
            {
                "request_id": "v2-reference",
                "model_id": samples.MODEL_ID,
                "version": "v2-quant",
                "seed": 321,
            },
        )

        # 4. Shape switch: preprocess cache and in-flight layout must rebuild.
        shape_body = samples.shape_increment(state_2["state_version"], snapshot_seq=3)
        shape_result = client.success("POST", "/api/increments", shape_body, 201)
        shape_switch = _switch_record("shape_change", shape_result)
        result["switches"].append(shape_switch)
        require(bool(shape_switch["diff_mask"] & 1), "shape switch must include shape bit", shape_switch)
        require(not shape_switch["in_place_migratable"], "shape switch must not be in-place", shape_switch)
        require("preprocess_cache" in shape_switch["rebuild"], "shape switch did not rebuild preprocess cache", shape_switch)
        require("inflight_index" in shape_switch["rebuild"], "shape switch did not rebuild in-flight index", shape_switch)
        state_3 = client.success("GET", "/api/state")
        require(state_3["snapshot_seq"] == 3, "shape switch sequence mismatch", state_3)
        shape_inflight_inference = client.success(
            "POST",
            "/api/infer",
            {
                "request_id": "inflight-before-shape",
                "model_id": samples.MODEL_ID,
                "seed": 321,
            },
        )
        require(
            shape_inflight_inference["bound_version"] == "v2-quant",
            "shape-switch in-flight request was rebound",
            shape_inflight_inference,
        )
        in_flight_consistency = _binding_consistency(v2_reference, shape_inflight_inference)
        require(in_flight_consistency["bitwise_identical"], "pinned version inference changed bitwise", in_flight_consistency)
        result["in_flight"] = {
            "request_id": "inflight-before-shape",
            "bound_version": shape_inflight_inference["bound_version"],
            **in_flight_consistency,
        }
        result["inference_consistency"].append(
            {
                "request_id": "v2-reference-vs-inflight-before-shape",
                **in_flight_consistency,
            }
        )

        # 5. Kahan-compensated gray split and deterministic re-decision.
        gray_targets: list[str] = []
        for index in range(6):
            request_id = f"gray-{index:02d}"
            decision = client.success(
                "POST",
                "/api/grays/decide",
                {"request_id": request_id, "model_id": samples.MODEL_ID},
            )
            gray_targets.append(decision["target_version"])
            inference = client.success(
                "POST",
                "/api/infer",
                {"request_id": request_id, "model_id": samples.MODEL_ID, "seed": 700 + index},
            )
            require(
                inference["bound_version"] == decision["target_version"],
                "gray decision and inference version differ",
                {"decision": decision, "inference": inference},
            )
        replay = client.success(
            "POST",
            "/api/grays/decide",
            {"request_id": "gray-00", "model_id": samples.MODEL_ID},
        )
        require(replay["target_version"] == gray_targets[0], "repeated gray decision changed target", replay)
        require(set(gray_targets) == {"v2-quant", "v3-shape"}, "gray split did not hit both versions", gray_targets)
        state_3 = client.success("GET", "/api/state")
        plan = state_3["gray_plans"][samples.MODEL_ID]
        result["gray"] = {
            "fixed_point": {"units": plan["ratio_ppm"], "scale": 1_000_000},
            "ratio_rounding_error": plan["ratio_rounding_error"],
            "decisions": gray_targets,
            "replay_target": replay["target_version"],
            "replay_same_target": replay["target_version"] == gray_targets[0],
            "replayed": replay.get("replayed"),
            "accumulator": plan["accumulator"],
            "compensation": plan["compensation"],
        }

        # 6. Sequential stale base rejection.
        stale_status, stale_base = client.error(
            "POST",
            "/api/increments",
            samples.quant_increment(state_2["state_version"], snapshot_seq=4),
        )
        require(stale_status == 409 and stale_base["code"] == "E_STALE_BASE",
                "stale increment was not rejected", stale_base)
        state_after_stale = client.success("GET", "/api/state")
        require(state_after_stale["snapshot_seq"] == 3, "stale increment changed state", state_after_stale)

        # 7. Two increments based on the same token: exactly one may win.
        state_3_token = state_3["state_version"]
        race_a = samples.race_increment(state_3_token, "race-a", 4)
        race_b = samples.race_increment(state_3_token, "race-b", 4)
        race_a["active_models"][samples.MODEL_ID]["gray_ratio"] = 1.0
        race_b["active_models"][samples.MODEL_ID]["gray_ratio"] = 1.0
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(client.request, "POST", "/api/increments", race_a),
                executor.submit(client.request, "POST", "/api/increments", race_b),
            ]
            race_responses = [future.result(timeout=30) for future in futures]
        winners = [(status, body) for status, body in race_responses if status == 201 and body.get("ok")]
        losers = [(status, body) for status, body in race_responses if status == 409]
        require(len(winners) == 1, "concurrent increments did not produce exactly one winner", race_responses)
        require(len(losers) == 1 and losers[0][1]["code"] == "E_STALE_BASE",
                "losing concurrent increment did not return E_STALE_BASE", race_responses)
        winner_result = winners[0][1]["data"]
        winner_switch = _switch_record("concurrent_winner", winner_result)
        result["switches"].append(winner_switch)
        winner_version = winner_result["active_models"][samples.MODEL_ID]["version"]
        state_4 = client.success("GET", "/api/state")
        require(state_4["snapshot_seq"] == 4, "concurrent winner sequence mismatch", state_4)
        result["concurrency"] = {
            "base_version": state_3_token,
            "winner_version": winner_version,
            "winner_snapshot_seq": winner_result["snapshot_seq"],
            "loser_code": losers[0][1]["code"],
            "loser_context": losers[0][1]["context"],
            "single_winner": len(winners) == 1 and len(losers) == 1,
        }
        result["steps"].append({"name": "concurrent_stale_base", **result["concurrency"]})

        # 8. Stop the normal process, then create a pending binding and crash the next process.
        normal_server.stop()
        normal_server = None
        require(port_available(port), "port was not released after normal server stop")
        fault_env = {
            "HOTROUTE_PAUSE_AFTER_INTENT_KIND": "increment_apply",
            "HOTROUTE_INTENT_MARKER": str(marker_path),
            "HOTROUTE_PAUSE_SECONDS": "60",
        }
        fault_server = LocalServer(db_path, port, run_dir / "server-fault.log", fault_env)
        fault_server.start()
        crash_decision = fault_server.client.success(
            "POST",
            "/api/grays/decide",
            {"request_id": "crash-pending", "model_id": samples.MODEL_ID},
        )
        crash_body = samples.dtype_increment(state_4["state_version"], snapshot_seq=5)
        crash_body["active_models"][samples.MODEL_ID]["previous_version"] = winner_version
        crash_body["active_models"][samples.MODEL_ID]["gray_ratio"] = 0

        crash_response: dict[str, Any] = {}

        def send_crashing_increment() -> None:
            try:
                status, body = fault_server.client.request(
                    "POST", "/api/increments", crash_body, timeout=90
                )
                crash_response.update({"status": status, "body": body})
            except Exception as exc:  # connection is expected to disappear
                crash_response.update({"exception": type(exc).__name__, "message": str(exc)})

        crash_thread = threading.Thread(target=send_crashing_increment, daemon=True)
        crash_thread.start()
        deadline = time.time() + 20
        while time.time() < deadline and not marker_path.exists():
            if fault_server.process is not None and fault_server.process.poll() is not None:
                raise VerificationFailure(
                    f"fault server exited before intent marker; log={fault_server.log_path}\n"
                    f"{fault_server.log_tail()}"
                )
            time.sleep(0.1)
        require(marker_path.exists(), "intent marker was not written before crash window")
        marker_data = json.loads(marker_path.read_text(encoding="utf-8"))
        fault_server.crash()
        crash_thread.join(timeout=10)
        require(
            fault_server.process is not None and fault_server.process.returncode is not None,
            "fault server process did not terminate",
        )
        result["steps"].append(
            {
                "name": "forced_termination_after_intent",
                "marker": marker_data,
                "process_returncode": fault_server.process.returncode,
                "request_observation": crash_response,
            }
        )
        fault_server = None

        # 9. Restart and inspect persisted-fact recovery report.
        time.sleep(0.5)
        require(port_available(port), "port was not released after crash")
        recovery_server = LocalServer(db_path, port, run_dir / "server-recovery.log")
        recovery_server.start()
        recovery_report = recovery_server.client.success("GET", "/api/recovery")
        recovered_state = recovery_server.client.success("GET", "/api/state")
        require(recovered_state["snapshot_seq"] == 4, "recovery changed the committed snapshot", recovered_state)
        require(recovered_state["active_models"][samples.MODEL_ID]["version"] == winner_version,
                "recovery changed the active route version", recovered_state)
        require(recovery_report["details"]["startup_scan_complete"], "recovery scan did not complete", recovery_report)
        require(not recovery_report["details"]["exit_report_available"],
                "recovery incorrectly claimed an exit report", recovery_report)
        require(any(item["kind"] == "increment_apply" for item in recovery_report["aborted_intents"]),
                "unfinished increment intent was not recorded as aborted", recovery_report)
        require(any(item["request_id"] == "crash-pending" for item in recovery_report["interrupted_bindings"]),
                "pending binding was not interrupted", recovery_report)
        require(crash_decision["target_version"] == winner_version, "crash binding target mismatch", crash_decision)

        # 10. A later hot switch must advance the sequence and replace route/cache atomically.
        post_body = samples.dtype_increment(recovered_state["state_version"], snapshot_seq=6)
        post_body["active_models"][samples.MODEL_ID]["previous_version"] = winner_version
        post_body["active_models"][samples.MODEL_ID]["gray_ratio"] = 1.0
        post_result = recovery_server.client.success("POST", "/api/increments", post_body, 201)
        post_switch = _switch_record("post_recovery_dtype_change", post_result)
        result["switches"].append(post_switch)
        final_state = recovery_server.client.success("GET", "/api/state")
        require(final_state["snapshot_seq"] == 6, "post-recovery switch sequence mismatch", final_state)
        active_version = final_state["active_models"][samples.MODEL_ID]["version"]
        require(active_version == "v4-dtype", "post-recovery active version mismatch", final_state)
        artifact_snapshots = {artifact["snapshot_seq"] for artifact in final_state["artifacts"]}
        require(artifact_snapshots == {6}, "route/cache artifacts are not all on the new snapshot", final_state)
        route_versions = {(route["model_id"], route["version"]) for route in final_state["routes"]}
        require(route_versions == {(samples.MODEL_ID, "v4-dtype")}, "route table was not replaced atomically", final_state)
        post_inference_1 = recovery_server.client.success(
            "POST",
            "/api/infer",
            {"request_id": "post-recovery-a", "model_id": samples.MODEL_ID, "seed": 900},
        )
        post_inference_2 = recovery_server.client.success(
            "POST",
            "/api/infer",
            {"request_id": "post-recovery-a", "model_id": samples.MODEL_ID, "seed": 900},
        )
        post_consistency = _binding_consistency(post_inference_1, post_inference_2)
        require(post_consistency["bitwise_identical"], "post-recovery inference was not bitwise stable", post_consistency)
        result["inference_consistency"].append({"request_id": "post-recovery-repeat", **post_consistency})
        require(post_inference_1["bound_version"] == "v4-dtype", "post-recovery inference version mismatch", post_inference_1)

        result["recovery"] = {
            "before_snapshot_seq": 4,
            "after_snapshot_seq": recovered_state["snapshot_seq"],
            "report_id": recovery_report["report_id"],
            "aborted_intent_count": len(recovery_report["aborted_intents"]),
            "interrupted_binding_count": len(recovery_report["interrupted_bindings"]),
            "exit_report_available": recovery_report["details"]["exit_report_available"],
            "persisted_facts_only": recovery_report["details"]["persisted_facts_only"],
            "last_committed_state_version": recovery_report["state_version"],
            "post_recovery_switch": post_result["snapshot_seq"] == 6,
            "route_cache_consistent": artifact_snapshots == {6} and route_versions == {(samples.MODEL_ID, "v4-dtype")},
            "post_recovery_inference_version": post_inference_1["bound_version"],
        }
        result["steps"].append({"name": "post_recovery_hot_switch", **result["recovery"]})
        result["final_state"] = {
            "snapshot_seq": final_state["snapshot_seq"],
            "state_version": final_state["state_version"],
            "active_models": final_state["active_models"],
            "artifact_snapshots": sorted(artifact_snapshots),
            "gray_plan": final_state["gray_plans"][samples.MODEL_ID],
        }

        result["conclusions"] = {
            "quant_only_in_place_migration": quant_switch["in_place_migratable"]
            and "preprocess_cache" in quant_switch["migrate"]
            and "inflight_index" in quant_switch["migrate"]
            and "preprocess_cache" not in quant_switch["rebuild"],
            "shape_change_rebuilds_derived_artifacts": "preprocess_cache" in shape_switch["rebuild"]
            and "inflight_index" in shape_switch["rebuild"],
            "route_cache_gray_plan_atomic": quant_switch["atomic_transaction"]
            and shape_switch["atomic_transaction"]
            and result["recovery"]["route_cache_consistent"],
            "in_flight_version_pinned": result["in_flight"]["bound_version"] == "v2-quant"
            and result["in_flight"]["bitwise_identical"],
            "gray_split_and_replay_deterministic": set(gray_targets) == {"v2-quant", "v3-shape"}
            and replay["target_version"] == gray_targets[0],
            "stale_base_rejected_without_mutation": stale_base["code"] == "E_STALE_BASE"
            and state_after_stale["snapshot_seq"] == 3,
            "concurrent_increment_single_winner": len(winners) == 1
            and losers[0][1]["code"] == "E_STALE_BASE",
            "forced_termination_recovered_from_persisted_facts": recovered_state["snapshot_seq"] == 4
            and recovery_report["details"]["startup_scan_complete"]
            and not recovery_report["details"]["exit_report_available"],
            "post_recovery_switch_consistent": result["recovery"]["route_cache_consistent"],
            "inference_outputs_bitwise_repeatable": all(
                item["bitwise_identical"] for item in result["inference_consistency"]
            ),
        }
        require(all(result["conclusions"].values()), "one or more acceptance conclusions failed", result["conclusions"])
        result["passed"] = True
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}\n\n{traceback.format_exc()}"
        for server in (normal_server, fault_server, recovery_server):
            if server is not None:
                result.setdefault("server_logs", {})[server.log_path.name] = server.log_tail()
    finally:
        for server in (normal_server, fault_server, recovery_server):
            if server is not None:
                server.stop()
        result["finished_at"] = utc_now()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(pretty_json(result) + "\n", encoding="utf-8")
        _write_markdown(markdown_path, result)
        result["output"] = str(output_path)
        result["markdown"] = str(markdown_path)
        if not keep_run_dir:
            shutil.rmtree(run_dir, ignore_errors=True)
    return result

