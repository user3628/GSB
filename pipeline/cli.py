"""命令行接口。"""

import argparse
import sys

from pipeline import STAGES
from pipeline.atomicio import canonical_json_bytes
from pipeline.data_ops import edit_training_sample
from pipeline.errors import PipelineError
from pipeline.generator import generate_workspace
from pipeline.pipeline import Pipeline
from pipeline.shards import read_jsonl, read_json_array, write_jsonl, write_json_array


def _print(payload):
    sys.stdout.buffer.write(canonical_json_bytes(payload))
    sys.stdout.buffer.flush()


def _fail(exc):
    _print(exc.to_dict() if isinstance(exc, PipelineError) else {"error": "E_UNKNOWN", "message": str(exc)})
    return 2


def cmd_generate(args):
    result = generate_workspace(args.workspace, seed=args.seed)
    _print(
        {
            "workspace": result["workspace"].root,
            "train_shards": result["train_paths"],
            "infer_shards": result["infer_paths"],
            "config": result["workspace"].config_path,
            "expected_model_version": result["expected_model_version"],
        }
    )
    return 0


def cmd_import(args):
    pipe = Pipeline(args.workspace)
    destination = pipe.import_file(args.path, args.kind)
    _print({"imported": destination, "kind": args.kind})
    return 0


def cmd_run(args):
    pipe = Pipeline(args.workspace)
    report = pipe.run(through_stage=args.through_stage)
    _print(report)
    return 0


def cmd_status(args):
    pipe = Pipeline(args.workspace)
    _print(pipe.status())
    return 0


def cmd_validate(args):
    pipe = Pipeline(args.workspace)
    _print(pipe.validate_incremental(scope=args.scope))
    return 0


def cmd_edit_sample(args):
    pipe = Pipeline(args.workspace)
    result = edit_training_sample(pipe.ws, args.sample_id, args.text, args.label)
    _print(result)
    return 0


def cmd_replace_shard(args):
    pipe = Pipeline(args.workspace)
    if args.kind == "train":
        records = read_jsonl(args.from_path)
        write_jsonl(_train_path(pipe, args.shard), records)
    else:
        requests = read_json_array(args.from_path)
        write_json_array(_infer_path(pipe, args.shard), requests)
    _print({"replaced": args.shard, "kind": args.kind})
    return 0


def _train_path(pipe, shard):
    import os

    return os.path.join(pipe.ws.train_dir, shard)


def _infer_path(pipe, shard):
    import os

    return os.path.join(pipe.ws.infer_dir, shard)


def cmd_serve(args):
    from pipeline.server import run_server

    run_server(args.workspace, host=args.host, port=args.port)
    return 0


def build_parser():
    parser = argparse.ArgumentParser(prog="pipeline", description="本地确定性训练/推理管线")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="生成确定性演示数据与配置")
    gen.add_argument("--workspace", required=True)
    gen.add_argument("--seed", type=int, default=20261122)
    gen.set_defaults(func=cmd_generate)

    imp = sub.add_parser("import", help="导入分片或配置")
    imp.add_argument("--workspace", required=True)
    imp.add_argument("--kind", choices=["train", "infer", "config"], required=True)
    imp.add_argument("--path", required=True)
    imp.set_defaults(func=cmd_import)

    run = sub.add_parser("run", help="分阶段执行（自动复用与失效重建）")
    run.add_argument("--workspace", required=True)
    run.add_argument("--through-stage", choices=STAGES, default=STAGES[-1])
    run.set_defaults(func=cmd_run)

    status = sub.add_parser("status", help="查询检查点、指纹与失效事件")
    status.add_argument("--workspace", required=True)
    status.set_defaults(func=cmd_status)

    validate = sub.add_parser("validate", help="增量 vs 全量逐字节一致性校验")
    validate.add_argument("--workspace", required=True)
    validate.add_argument("--scope", choices=STAGES, default=STAGES[-1])
    validate.set_defaults(func=cmd_validate)

    edit = sub.add_parser("edit-sample", help="单样本编辑")
    edit.add_argument("--workspace", required=True)
    edit.add_argument("--sample-id", required=True)
    edit.add_argument("--text")
    edit.add_argument("--label", type=int, choices=[0, 1])
    edit.set_defaults(func=cmd_edit_sample)

    replace = sub.add_parser("replace-shard", help="单分片替换")
    replace.add_argument("--workspace", required=True)
    replace.add_argument("--kind", choices=["train", "infer"], required=True)
    replace.add_argument("--shard", required=True)
    replace.add_argument("--from", dest="from_path", required=True)
    replace.set_defaults(func=cmd_replace_shard)

    serve = sub.add_parser("serve", help="启动本地 HTTP 接口")
    serve.add_argument("--workspace", required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.set_defaults(func=cmd_serve)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except PipelineError as exc:
        return _fail(exc)


if __name__ == "__main__":
    raise SystemExit(main())
