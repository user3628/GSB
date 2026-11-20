"""命令行接口：python -m segtag --train 训练样本 --inputs 文本文件..."""
from __future__ import annotations

import argparse
import sys

from .api import Segmenter
from .errors import SegTagError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="segtag",
        description="中文分词与词性联合标注（本地确定性，对数域 Viterbi）",
    )
    parser.add_argument("--train", required=True, help="本地标注训练样本路径（词/词性 空格分隔）")
    parser.add_argument("--inputs", nargs="+", required=True, help="待标注 UTF-8 文本文件，每行一句")
    parser.add_argument("--smoothing", type=float, default=0.1, help="拉普拉斯平滑系数，默认 0.1")
    parser.add_argument("--unknown-token", default="<UNK>", help="未知标记，默认 <UNK>")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        seg = Segmenter.train(args.train, smoothing=args.smoothing, unknown_token=args.unknown_token)
    except SegTagError as exc:
        print(f"建模失败: {exc}", file=sys.stderr)
        return 2

    print(f"模型指纹: {seg.fingerprint}")
    print(f"词性表: {' '.join(seg.pos_tags)}")
    report = seg.tag_files(args.inputs)
    for rec in report.records:
        if not rec.ok:
            print(f"[{rec.file}:{rec.line}] 错误 {rec.error.code}: {rec.error.message}")
            continue
        tokens = " ".join(f"[{w.start},{w.end}){w.surface}/{w.pos}" for w in rec.words)
        print(f"[{rec.file}:{rec.line}] logP={rec.logprob:.6f} {tokens}")
    print(
        "汇总: 句数={ok} 错误={err} 总词数={words} 总对数概率={total!r} "
        "交叉校验差={diff!r} 一致={consistent}".format(
            ok=report.ok_count,
            err=report.error_count,
            words=report.total_words,
            total=report.total_logprob,
            diff=report.crosscheck_diff,
            consistent=report.consistent,
        )
    )
    return 0 if report.error_count == 0 else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

