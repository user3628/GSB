"""命令行交互入口。

示例：
    python -m layout_order.cli generate --workspace workspace
    python -m layout_order.cli analyze --workspace workspace
    python -m layout_order.cli status --workspace workspace --doc-id doc_double_en
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from .config import DEFAULT_CONFIG, STAGES, merge_config
from .engine import PipelineEngine
from .errors import LayoutError
from .generator import generate_documents
from .hashing import canonical_json
from .stages import normalize as normalize_stage


def _load_documents(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict):
        data = data.get("documents", [])
    return data


def _load_config(path: str | None) -> dict[str, Any]:
    if not path:
        return DEFAULT_CONFIG
    with open(path, "r", encoding="utf-8") as handle:
        overrides = json.load(handle)
    return merge_config(overrides)


def _stop_file(workspace: str) -> str:
    return os.path.join(workspace, "STOP_REQUESTED")


def _print(payload: Any) -> None:
    print(canonical_json(payload))


def cmd_generate(args: argparse.Namespace) -> int:
    docs = generate_documents()
    sample_dir = os.path.join(args.workspace, "samples")
    os.makedirs(sample_dir, exist_ok=True)
    path = os.path.join(sample_dir, "documents.json")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(canonical_json({"seed": 20261121, "documents": docs}) + "\n")
    _print({"generated": len(docs), "path": os.path.abspath(path),
            "doc_ids": [doc["doc_id"] for doc in docs]})
    return 0


def _build_engine(args: argparse.Namespace) -> PipelineEngine:
    os.makedirs(args.workspace, exist_ok=True)
    return PipelineEngine(args.workspace, _load_config(args.config))


def cmd_import(args: argparse.Namespace) -> int:
    engine = _build_engine(args)
    docs = _load_documents(args.input)
    saved = []
    for doc in docs:
        normalize_stage.validate_raw_document(doc)
        engine.store.save_input(doc)
        saved.append(doc["doc_id"])
    _print({"imported": len(saved), "doc_ids": saved})
    return 0


def cmd_analyze(args: argparse.Namespace) -> int:
    engine = _build_engine(args)
    sample_path = args.input or os.path.join(args.workspace, "samples", "documents.json")
    docs = _load_documents(sample_path)
    if args.doc_id:
        docs = [doc for doc in docs if doc["doc_id"] == args.doc_id]
        if not docs:
            raise LayoutError("E_DOC_NOT_FOUND", f"样本中不存在文档 {args.doc_id}",
                              {"doc_id": args.doc_id})

    def stop_check() -> bool:
        if args.stop_after:
            return False  # 单文档阶段级停止由下方标志处理
        return os.path.exists(_stop_file(args.workspace))

    reports: list[dict[str, Any]] = []
    stop_after = args.stop_after
    for doc in docs:
        if stop_check():
            reports.append({"doc_id": doc["doc_id"], "status": "stopped_before_start"})
            continue

        def stage_stop(document=doc, marker=stop_after) -> bool:
            if marker is None:
                return os.path.exists(_stop_file(args.workspace))
            record = engine.store.load_stage(document["doc_id"], marker)
            return record is not None

        reports.append(engine.run_document(doc, stage_stop))
    _print({"reports": reports, "stop_file": _stop_file(args.workspace)})
    return 0


def cmd_request_stop(args: argparse.Namespace) -> int:
    os.makedirs(args.workspace, exist_ok=True)
    with open(_stop_file(args.workspace), "w", encoding="utf-8") as handle:
        handle.write("stop\n")
    _print({"stop_requested": True, "stop_file": _stop_file(args.workspace)})
    return 0


def cmd_resume(args: argparse.Namespace) -> int:
    engine = _build_engine(args)
    stop_path = _stop_file(args.workspace)
    if os.path.exists(stop_path):
        os.remove(stop_path)
    sample_path = args.input or os.path.join(args.workspace, "samples", "documents.json")
    reports = engine.run(_load_documents(sample_path))
    _print({"reports": reports, "resumed": True})
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    engine = _build_engine(args)
    if args.doc_id:
        _print(engine.document_status(args.doc_id))
        return 0
    _print({"documents": [engine.document_status(doc_id)
                          for doc_id in engine.store.list_documents()]})
    return 0


def cmd_sequence(args: argparse.Namespace) -> int:
    engine = _build_engine(args)
    record = engine.store.load_stage(args.doc_id, "order")
    if record is None:
        raise LayoutError("E_NOT_FOUND", f"文档 {args.doc_id} 尚无顺序阶段产物",
                          {"doc_id": args.doc_id})
    _print(record["product"])
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    engine = _build_engine(args)
    record = engine.store.load_stage(args.doc_id, "summary")
    if record is None:
        raise LayoutError("E_NOT_FOUND", f"文档 {args.doc_id} 尚无汇总阶段产物",
                          {"doc_id": args.doc_id})
    path = engine.store.export_result(args.doc_id, record["product"])
    _print({"doc_id": args.doc_id, "path": os.path.abspath(path),
            "result_digest": record["product"]["result_digest"]})
    return 0


def cmd_update(args: argparse.Namespace) -> int:
    engine = _build_engine(args)
    docs = _load_documents(args.input)
    target = next((doc for doc in docs if doc["doc_id"] == args.doc_id), None)
    if target is None:
        raise LayoutError("E_DOC_NOT_FOUND", f"样本中不存在文档 {args.doc_id}",
                          {"doc_id": args.doc_id})
    _print(engine.update_document(target))
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    engine = _build_engine(args)
    sample_path = args.input or os.path.join(args.workspace, "samples", "documents.json")
    docs = _load_documents(sample_path)
    verification = []
    for doc in docs:
        report = engine.update_document(doc)
        verification.append({
            "doc_id": doc["doc_id"],
            "verified": report["verified_against_full_build"],
            "result_digest": report["result_digest"],
        })
    _print({"verification": verification})
    return 0


def cmd_invalidations(args: argparse.Namespace) -> int:
    engine = _build_engine(args)
    _print({"events": engine.store.read_invalidation_log()})
    return 0


def cmd_fingerprints(args: argparse.Namespace) -> int:
    engine = _build_engine(args)
    status = engine.document_status(args.doc_id)
    _print({"doc_id": args.doc_id,
            "config_version": status["config_version"],
            "stage_fingerprints": status["stage_fingerprints"]})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="layout_order",
                                     description="本地确定性文档版面阅读顺序分析引擎")
    parser.add_argument("--workspace", default="workspace")
    parser.add_argument("--config", default=None, help="JSON 配置覆盖文件")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("generate", help="生成确定性版面样本")
    p.set_defaults(func=cmd_generate)

    p = sub.add_parser("import", help="导入版面样本到工作区")
    p.add_argument("--input", required=True)
    p.set_defaults(func=cmd_import)

    p = sub.add_parser("analyze", help="触发分阶段分析（支持优雅停止）")
    p.add_argument("--input", default=None)
    p.add_argument("--doc-id", default=None)
    p.add_argument("--stop-after", choices=STAGES, default=None,
                   help="完成该阶段后停止（演示阶段间停止恢复）")
    p.set_defaults(func=cmd_analyze)

    p = sub.add_parser("request-stop", help="请求优雅停止")
    p.set_defaults(func=cmd_request_stop)

    p = sub.add_parser("resume", help="清除停止标记并恢复分析")
    p.add_argument("--input", default=None)
    p.set_defaults(func=cmd_resume)

    p = sub.add_parser("status", help="查询各阶段状态与版本指纹")
    p.add_argument("--doc-id", default=None)
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("fingerprints", help="单独查看阶段指纹")
    p.add_argument("--doc-id", required=True)
    p.set_defaults(func=cmd_fingerprints)

    p = sub.add_parser("sequence", help="查看块序列、裁决与置信度")
    p.add_argument("--doc-id", required=True)
    p.set_defaults(func=cmd_sequence)

    p = sub.add_parser("export", help="导出最终结果")
    p.add_argument("--doc-id", required=True)
    p.set_defaults(func=cmd_export)

    p = sub.add_parser("update", help="单文档增量更新并与全量构建比对")
    p.add_argument("--input", required=True)
    p.add_argument("--doc-id", required=True)
    p.set_defaults(func=cmd_update)

    p = sub.add_parser("verify", help="全部文档增量/全量一致性校验")
    p.add_argument("--input", default=None)
    p.set_defaults(func=cmd_verify)

    p = sub.add_parser("invalidations", help="查看失效传播事件")
    p.set_defaults(func=cmd_invalidations)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except LayoutError as exc:
        payload = exc.to_dict()
        print(canonical_json(payload), file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(canonical_json({"code": "E_NOT_FOUND", "message": str(exc), "details": {}}),
              file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
