"""mmpipe 命令行交互接口。"""

import argparse
import json
import sys

from . import STAGES
from .codec import short
from .errors import E_STOP_REQUESTED, PipeError
from .importer import import_samples, load_workspace_inputs
from .pipeline import StopController, expected_inputs, run_pipeline
from .storage import Workspace


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def _open(args) -> Workspace:
    return Workspace(args.workspace)


def cmd_init(args) -> int:
    ws = Workspace(args.workspace)
    ws.cleanup_stale_tmp()
    ws.log_event("workspace_init", root=ws.root)
    _print({"ok": True, "workspace": ws.root,
            "cleaned_tmp_files": ws.cleaned_tmp})
    return 0


def cmd_import(args) -> int:
    ws = _open(args)
    manifest = import_samples(ws, args.texts, args.images, args.config)
    _print({"ok": True, "manifest": manifest})
    return 0


def _run(ws: Workspace, args, stop: StopController | None = None,
         unit_gate=None) -> dict:
    config, texts, images = load_workspace_inputs(ws)
    return run_pipeline(ws, config, texts, images, stop=stop,
                        max_workers=args.workers, unit_delay=args.unit_delay,
                        unit_gate=unit_gate)


def cmd_run(args) -> int:
    ws = _open(args)
    stop = StopController(ws)
    if args.stop_after is not None:
        stop.clear()
        report = _run_with_stop_after(ws, args, stop)
    else:
        report = _run(ws, args, stop)
    _print({"ok": True, "report": report})
    return 0


def _run_with_stop_after(ws: Workspace, args, stop: StopController) -> dict:
    """演示用：完成 N 个样本单元后在单元边界自动请求优雅停止。"""
    import threading as _threading
    processed = {"n": 0}
    gate_lock = _threading.Lock()

    def counting_gate():
        with gate_lock:
            index = processed["n"]
            processed["n"] += 1
            if index >= args.stop_after and not stop.requested():
                stop.request()
                ws.log_event("auto_stop_requested", units=index)
                raise PipeError(E_STOP_REQUESTED,
                                "stop requested at unit boundary",
                                stage="pipeline")

    try:
        report = _run(ws, args, stop, unit_gate=counting_gate)
    except PipeError as exc:
        if exc.code != "E_STOP_REQUESTED":
            raise
        stage = exc.stage if exc.stage in (
            "text_encoding", "visual_alignment",
            "fusion_scoring", "aggregation") else "stage_boundary"
        report = {"stopped": True, "stage": stage,
                  "units_processed": processed["n"],
                  "head_seq": ws.head_seq()}
    return report


def cmd_stop(args) -> int:
    ws = _open(args)
    ws.request_stop()
    ws.log_event("stop_requested_via_cli")
    _print({"ok": True, "stop_requested": True})
    return 0


def cmd_resume(args) -> int:
    ws = _open(args)
    stop = StopController(ws)
    stop.clear()
    report = _run(ws, args, stop)
    _print({"ok": True, "report": report})
    return 0


def cmd_status(args) -> int:
    ws = _open(args)
    state = ws.state()
    total, valid = ws.corrupt_checkpoint_count()
    out = {"workspace": ws.root, "head_seq": ws.head_seq(),
           "stop_requested": ws.stop_requested(),
           "checkpoint_files": total, "valid_checkpoints": valid,
           "ignored_corrupt": total - valid, "stages": {}}
    expected = None
    if ws.config_exists():
        try:
            config, texts, images = load_workspace_inputs(ws)
            expected = expected_inputs(config, texts, images)
        except (FileNotFoundError, OSError):
            expected = None
    for stage in STAGES:
        cur = state["current"].get(stage)
        entry = {"status": cur.get("status") if cur else "absent"}
        if cur:
            entry["seq"] = cur["seq"]
            entry["input_fingerprint"] = cur["input_fingerprint"]
            entry["config_version"] = cur["config_version"]
            entry["product_digest"] = cur["product_digest"]
            entry["short_input_fingerprint"] = short(cur["input_fingerprint"])
            if cur.get("status") == "invalidated":
                entry["invalidation"] = cur.get("invalidation")
        if expected:
            entry["expected_input_fingerprint"] = expected[stage]["input"]
            entry["expected_config_version"] = expected[stage]["config"]
            entry["matches_expected"] = bool(
                cur and cur.get("status") == "published"
                and cur["input_fingerprint"] == expected[stage]["input"]
                and cur["config_version"] == expected[stage]["config"])
        out["stages"][stage] = entry
    out["invalidations"] = ws.invalidations()
    _print(out)
    return 0


def cmd_aggregate(args) -> int:
    ws = _open(args)
    product = ws.load_current_product("aggregation")
    if product is None:
        raise PipeError("E_INVALID_STAGE",
                        "aggregation product is not published",
                        stage="aggregation")
    _print(product)
    return 0


def cmd_product(args) -> int:
    ws = _open(args)
    product = ws.load_current_product(args.stage)
    if product is None:
        raise PipeError("E_INVALID_STAGE",
                        f"stage {args.stage!r} product is not published",
                        stage=args.stage)
    _print(product)
    return 0


def cmd_events(args) -> int:
    ws = _open(args)
    _print({"events": ws.events()})
    return 0


def cmd_publish_guard(args) -> int:
    """并发发布保护演示：两个发布者竞争同一序号。"""
    import concurrent.futures
    ws = _open(args)
    config, texts, images = load_workspace_inputs(ws)
    from mmpipe import text_encoding
    product = text_encoding.run(texts, config)
    expected = expected_inputs(config, texts, images)
    base = ws.head_seq()

    def publisher(slot: int):
        local = Workspace(ws.root)
        try:
            cp = local.publish(
                "text_encoding" if args.stage is None else args.stage,
                product, expected["text_encoding"]["input"],
                expected["text_encoding"]["config"],
                expected_base_seq=base)
            return {"slot": slot, "ok": True, "seq": cp["seq"]}
        except PipeError as exc:
            return {"slot": slot, "ok": False, "code": exc.code,
                    "message": exc.message}

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(publisher, (1, 2)))
    _print({"base_seq": base, "results": results, "head_seq": ws.head_seq()})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mmpipe",
                                     description="多模态协同推理数据管线")
    parser.add_argument("-w", "--workspace", default="mmpipe_workspace")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init").set_defaults(func=cmd_init)

    p_import = sub.add_parser("import")
    p_import.add_argument("--texts", default="samples/texts.jsonl")
    p_import.add_argument("--images", default="samples/images.json")
    p_import.add_argument("--config", default="samples/pipeline_config.json")
    p_import.set_defaults(func=cmd_import)

    p_run = sub.add_parser("run")
    p_run.add_argument("--workers", type=int, default=None)
    p_run.add_argument("--unit-delay", type=float, default=0.0)
    p_run.add_argument("--stop-after", type=int, default=None,
                       help="处理 N 个单元后自动请求停止（演示优雅停止）")
    p_run.set_defaults(func=cmd_run)

    sub.add_parser("stop").set_defaults(func=cmd_stop)

    p_resume = sub.add_parser("resume")
    p_resume.add_argument("--workers", type=int, default=None)
    p_resume.add_argument("--unit-delay", type=float, default=0.0)
    p_resume.set_defaults(func=cmd_resume)

    sub.add_parser("status").set_defaults(func=cmd_status)
    sub.add_parser("aggregate").set_defaults(func=cmd_aggregate)
    sub.add_parser("events").set_defaults(func=cmd_events)

    p_product = sub.add_parser("product")
    p_product.add_argument("stage", choices=STAGES)
    p_product.set_defaults(func=cmd_product)

    p_guard = sub.add_parser("publish-guard")
    p_guard.add_argument("--stage", default=None)
    p_guard.set_defaults(func=cmd_publish_guard)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except PipeError as exc:
        print(json.dumps({"ok": False, "error": exc.to_dict()},
                         ensure_ascii=False, indent=2), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
