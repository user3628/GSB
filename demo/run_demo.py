#!/usr/bin/env python3
"""mmpipe 本地确定性演示与验收脚本。

覆盖：
  1. 完整管线 + 验证工具检查；
  2. 人为触发优雅停止 -> 恢复 -> 与完整运行逐字节比对；
  3. 版本变化检测与失效传播（文本 / 投影 / 融合权重 / 算子置信度）；
  4. 业务错误码链路（E_MISSING_INPUT / E_SHAPE_MISMATCH / E_NUMERIC / E_INVALID_STAGE）；
  5. 并发发布保护（E_STALE_CHECKPOINT）与损坏检查点忽略；
  6. HTTP 接口冒烟（导入 / 运行 / 状态 / 聚合 / 停止）。

所有输出写入 demo_output/，全程不访问外部服务。
"""

import copy
import json
import shutil
import sys
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import uvicorn  # noqa: E402

from mmpipe import STAGES, text_encoding, visual_alignment  # noqa: E402
from mmpipe.api import create_app  # noqa: E402
from mmpipe.codec import canonical_dumps  # noqa: E402
from mmpipe.errors import (  # noqa: E402
    E_MISSING_INPUT,
    E_NUMERIC,
    E_SHAPE_MISMATCH,
    E_STALE_CHECKPOINT,
    E_INVALID_STAGE,
    PipeError,
    StageFailure,
)
from mmpipe.pipeline import (  # noqa: E402
    StopController,
    expected_inputs,
    load_images,
    load_texts,
    run_pipeline,
)
from mmpipe.samplegen import generate  # noqa: E402
from mmpipe.storage import CP_DIR, Workspace  # noqa: E402
from mmpipe.verifier import verify_workspace  # noqa: E402

OUT = ROOT / "demo_output"
SAMPLES = OUT / "samples"
RESULTS: list[dict] = []


def record(name: str, ok: bool, detail: str) -> None:
    RESULTS.append({"scenario": name, "ok": ok, "detail": detail})
    flag = "PASS" if ok else "FAIL"
    print(f"[{flag}] {name} :: {detail}")


def fresh_ws(name: str) -> Workspace:
    path = OUT / name
    if path.exists():
        shutil.rmtree(path)
    return Workspace(str(path))


def load_inputs():
    config = json.loads((SAMPLES / "pipeline_config.json").read_bytes())
    texts = load_texts(str(SAMPLES / "texts.jsonl"))
    images = load_images(str(SAMPLES / "images.json"))
    return config, texts, images


def scenario_full_run(config, texts, images):
    ws = fresh_ws("ws_full")
    report = run_pipeline(ws, config, texts, images, StopController(ws))
    verify = verify_workspace(ws)
    record("1. 完整管线+验证",
           report["head_seq"] == 4 and verify["ok"],
           f"head_seq={report['head_seq']}, verify_checks={len(verify['checks'])}")
    return ws


def scenario_stop_resume(config, texts, images, reference: Workspace):
    ws = fresh_ws("ws_stop_resume")
    stop = StopController(ws)

    def trigger():
        time.sleep(0.08)
        stop.request()

    threading.Thread(target=trigger, daemon=True).start()
    stopped_stage = None
    try:
        run_pipeline(ws, config, texts, images, stop=stop, unit_delay=0.06)
    except PipeError as exc:
        stopped_stage = exc.stage if exc.code == "E_STOP_REQUESTED" else None
    mid_seqs = [cp["seq"] for cp in ws.load_valid_checkpoints()]

    stop2 = StopController(ws)
    stop2.clear()
    report = run_pipeline(ws, config, texts, images, stop=stop2)

    identical = all(
        canonical_dumps(ws.load_current_product(stage))
        == canonical_dumps(reference.load_current_product(stage))
        for stage in STAGES)
    seqs = [cp["seq"] for cp in ws.load_valid_checkpoints()]
    continuous = seqs == list(range(1, len(seqs) + 1))
    record("2. 优雅停止后恢复逐字节一致",
           bool(stopped_stage) and identical and continuous
           and report["head_seq"] == 4,
           f"stopped_at={stopped_stage}, mid_seqs={mid_seqs}, "
           f"final_seqs={seqs}, byte_identical={identical}")


def scenario_invalidation(config, texts, images):
    ws = fresh_ws("ws_invalidation")
    run_pipeline(ws, config, texts, images, StopController(ws))
    changed_texts = copy.deepcopy(texts)
    changed_texts[0]["text"] += " revised"
    run_pipeline(ws, config, changed_texts, images, StopController(ws))
    text_inv = [i["stage"] for i in ws.invalidations()]
    text_ok = text_inv == list(STAGES)

    ws2 = fresh_ws("ws_weight_change")
    run_pipeline(ws2, config, texts, images, StopController(ws2))
    before = {s: ws2.stage_state(s)["seq"] for s in STAGES}
    old_fusion = canonical_dumps(ws2.load_current_product("fusion_scoring"))
    cfg2 = copy.deepcopy(config)
    cfg2["fusion_weights"] = {"similarity": 0.8, "operator": 0.2}
    run_pipeline(ws2, cfg2, texts, images, StopController(ws2))
    after = {s: ws2.stage_state(s)["seq"] for s in STAGES}
    new_fusion = canonical_dumps(ws2.load_current_product("fusion_scoring"))
    weight_ok = (after["text_encoding"] == before["text_encoding"]
                 and after["visual_alignment"] == before["visual_alignment"]
                 and after["fusion_scoring"] > before["fusion_scoring"]
                 and after["aggregation"] > before["aggregation"]
                 and old_fusion != new_fusion)

    ws3 = fresh_ws("ws_projection_change")
    run_pipeline(ws3, config, texts, images, StopController(ws3))
    before3 = {s: ws3.stage_state(s)["seq"] for s in STAGES}
    cfg3 = copy.deepcopy(config)
    cfg3["projection_matrix"][0][0] += 0.321
    run_pipeline(ws3, cfg3, texts, images, StopController(ws3))
    after3 = {s: ws3.stage_state(s)["seq"] for s in STAGES}
    proj_ok = (after3["text_encoding"] == before3["text_encoding"]
               and all(after3[s] > before3[s] for s in STAGES[1:]))

    record("3a. 文本变化全链路失效", text_ok, f"invalidated={text_inv}")
    record("3b. 权重变化仅下游失效且结果改变", weight_ok,
           f"upstream_reused=True, fusion_seqs {before['fusion_scoring']}->"
           f"{after['fusion_scoring']}")
    record("3c. 投影变化自视觉阶段失效", proj_ok,
           "text_reused=True, downstream_rebuilt=True")


def scenario_error_codes(config, texts, images):
    text_product = text_encoding.run(texts, config)

    try:
        text_encoding.run([{"id": "bad", "text": "  ", "lang": "en"}], config)
        ok_missing = False
    except StageFailure as exc:
        ok_missing = exc.errors[0]["code"] == E_MISSING_INPUT

    bad_shape = dict(images[0])
    bad_shape["feature"] = [0.1] * 7
    try:
        visual_alignment.run([bad_shape], text_product, config)
        ok_shape = False
    except StageFailure as exc:
        ok_shape = (exc.errors[0]["code"] == E_SHAPE_MISMATCH
                    and exc.errors[0]["sample_id"] == bad_shape["id"])

    bad_nan = dict(images[0])
    bad_nan["feature"] = [float("nan")] * len(bad_nan["feature"])
    try:
        visual_alignment.run([bad_nan], text_product, config)
        ok_nan = False
    except StageFailure as exc:
        ok_nan = exc.errors[0]["code"] == E_NUMERIC

    bad_ref = dict(images[0])
    bad_ref["text_id"] = "missing-text"
    try:
        visual_alignment.run([bad_ref], text_product, config)
        ok_ref = False
    except StageFailure as exc:
        ok_ref = exc.errors[0]["code"] == E_MISSING_INPUT

    ws = fresh_ws("ws_errors")
    try:
        ws.publish("fusion_scoring", {}, "fp", "cv")
        ok_stage = False
    except PipeError as exc:
        ok_stage = exc.code == E_INVALID_STAGE

    record("4a. E_MISSING_INPUT 文本缺失", ok_missing, "empty text rejected")
    record("4b. E_SHAPE_MISMATCH 维度不匹配", ok_shape, "feature dim 7 rejected")
    record("4c. E_NUMERIC NaN 拦截", ok_nan, "NaN feature rejected")
    record("4d. E_MISSING_INPUT 引用缺失", ok_ref, "unknown text_id rejected")
    record("4e. E_INVALID_STAGE 非法跳转", ok_stage,
           "fusion_scoring without predecessors rejected")


def scenario_concurrency_and_corruption(config, texts, images):
    ws = fresh_ws("ws_concurrency")
    text_product = text_encoding.run(texts, config)
    exp = expected_inputs(config, texts, images)
    ws.publish("text_encoding", text_product,
               exp["text_encoding"]["input"],
               exp["text_encoding"]["config"])
    head = ws.head_seq()
    visual_product = visual_alignment.run(images, text_product, config)
    outcomes = []
    barrier = threading.Barrier(2)

    def worker(slot):
        local = Workspace(ws.root)
        barrier.wait()
        try:
            cp = local.publish("visual_alignment", visual_product,
                               exp["visual_alignment"]["input"],
                               exp["visual_alignment"]["config"],
                               expected_base_seq=head)
            outcomes.append((slot, "ok", cp["seq"]))
        except PipeError as exc:
            outcomes.append((slot, exc.code, None))

    threads = [threading.Thread(target=worker, args=(i,)) for i in (1, 2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    codes = sorted(o[1] for o in outcomes)
    concurrency_ok = codes == [E_STALE_CHECKPOINT, "ok"] and ws.head_seq() == head + 1

    cp_dir = ws.path(CP_DIR)
    (Path(cp_dir) / "cp_000002.json").write_text("{broken", encoding="utf-8")
    (Path(cp_dir) / "cp_000099.json").write_text("", encoding="utf-8")
    reopened = Workspace(ws.root)
    total, valid = reopened.corrupt_checkpoint_count()
    corrupt_ok = [c["seq"] for c in reopened.load_valid_checkpoints()] == [1]

    record("5a. 并发发布 E_STALE_CHECKPOINT 保护", concurrency_ok,
           f"outcomes={codes}, head={ws.head_seq()}")
    record("5b. 损坏/不完整检查点被忽略", corrupt_ok,
           "files={}, valid={}, seqs=[1]".format(total, valid))


def http_call(base, method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def scenario_http(config, texts, images):
    ws_path = OUT / "ws_http"
    if ws_path.exists():
        shutil.rmtree(ws_path)
    app = create_app(str(ws_path))
    server_cfg = uvicorn.Config(app, host="127.0.0.1", port=0,
                                log_level="error")
    server = uvicorn.Server(server_cfg)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.05)
    port = server.servers[0].sockets[0].getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    try:
        s, _ = http_call(base, "GET", "/health")
        s, _ = http_call(base, "POST", "/import", {
            "texts": str(SAMPLES / "texts.jsonl"),
            "images": str(SAMPLES / "images.json"),
            "config": str(SAMPLES / "pipeline_config.json")})
        http_call(base, "POST", "/run", {})
        body = {"running": True}
        for _ in range(100):
            _, body = http_call(base, "GET", "/run/status")
            if not body["running"]:
                break
            time.sleep(0.05)
        _, status = http_call(base, "GET", "/status")
        _, agg = http_call(base, "GET", "/aggregate")
        _, stop_body = http_call(base, "POST", "/stop")
        ok = (body["last_error"] is None and status["head_seq"] == 4
              and "t003" in agg["aggregate"]["summary"]["conflict_sample_ids"]
              and stop_body["stop_requested"] is True)
        record("6. HTTP 接口端到端", ok,
               f"head_seq={status['head_seq']}, conflict=retained, stop=true")
    finally:
        server.should_exit = True
        thread.join(timeout=10)



def scenario_crash_recovery(config, texts, images, reference):
    from pathlib import Path
    ws = fresh_ws("ws_crash")
    run_pipeline(ws, config, texts, images, StopController(ws))
    corrupt = Path(ws.path(CP_DIR)) / "cp_000003.json"
    corrupt.write_text('{"seq":3,"broken"', encoding="utf-8")
    (Path(ws.path("products")) / ".fusion_scoring_000003.json.x.tmp").write_text("x")

    reopened = Workspace(ws.root)
    healed_head = reopened.head_seq()
    reopened.cleanup_stale_tmp()
    stop = StopController(reopened)
    stop.clear()
    run_pipeline(reopened, config, texts, images, stop=stop)
    identical = all(
        canonical_dumps(reopened.load_current_product(stage))
        == canonical_dumps(reference.load_current_product(stage))
        for stage in STAGES)
    verify = verify_workspace(reopened)
    record("7. 写入中断自愈+恢复一致",
           healed_head == 2 and reopened.head_seq() == 4
           and identical and verify["ok"],
           f"healed_head={healed_head}, quarantined="
           f"{[Path(q).name for q in reopened.quarantined]}, "
           f"final_head=4, byte_identical={identical}")

def main():
    OUT.mkdir(exist_ok=True)
    generate(str(SAMPLES))
    print(f"== 确定性样本已生成: {SAMPLES} ==")
    config, texts, images = load_inputs()
    reference = scenario_full_run(config, texts, images)
    scenario_stop_resume(config, texts, images, reference)
    scenario_invalidation(config, texts, images)
    scenario_error_codes(config, texts, images)
    scenario_concurrency_and_corruption(config, texts, images)
    scenario_http(config, texts, images)
    scenario_crash_recovery(config, texts, images, reference)

    report_path = OUT / "demo_report.json"
    report_path.write_bytes(canonical_dumps({"results": RESULTS}))
    print(f"\n== 报告已写入 {report_path} ==")
    passed = sum(1 for r in RESULTS if r["ok"])
    print(f"总计 {len(RESULTS)} 项，通过 {passed} 项，失败 {len(RESULTS) - passed} 项")
    return 0 if passed == len(RESULTS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
