"""完整演示：批量标注 + 平滑版本/缓存 + 错误注入。

运行：.venv/bin/python demo.py
输出：终端打印 + 同目录生成固定摘要 demo_summary.txt（重复运行逐字节一致）。

错误注入入口（按任务要求预置）：
- 非法平滑系数 -0.5          -> E_SMOOTHING
- 发射概率注入 -inf / NaN    -> E_NUMERIC（携带句号与字符位置）
- 非法标签序列 / 未知词性    -> E_INVALID_TAG
- 空句                       -> E_EMPTY
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from segtag import Segmenter, SegTagError

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")
TRAIN = os.path.join(DATA, "train.txt")
INPUTS = [os.path.join(DATA, f"input{i}.txt") for i in (1, 2, 3)]
SUMMARY = os.path.join(ROOT, "demo_summary.txt")


def fmt_words(words) -> str:
    return " ".join(f"[{w.start},{w.end}){w.surface}/{w.pos}" for w in words)


def main() -> int:
    lines: list[str] = []

    def out(text: str = "") -> None:
        print(text)
        lines.append(text)

    out("== 1. 训练概率表（拉普拉斯平滑，对数域存储） ==")
    seg = Segmenter.train(TRAIN, smoothing=0.1)
    out(f"平滑系数: {seg.smoothing}")
    out(f"模型指纹: {seg.fingerprint}")
    out(f"词性表: {' '.join(seg.pos_tags)}")

    out("== 2. 批量标注（3 个本地文件，按文件与行序） ==")
    report = seg.tag_files(INPUTS)
    for i, rec in enumerate(report.records):
        where = f"[{os.path.basename(rec.file)}:{rec.line}]"
        if rec.ok:
            out(f"句{i} {where} logP={rec.logprob:.6f} 词数={len(rec.words)}")
            out(f"    {rec.text}")
            out(f"    {fmt_words(rec.words)}")
        else:
            out(f"句{i} {where} 错误 {rec.error.code}: {rec.error.message}")
    out(f"成功句数: {report.ok_count}  错误句数: {report.error_count}  总词数: {report.total_words}")
    out(f"总对数概率: {report.total_logprob!r}")
    out(
        f"逐句独立解码交叉校验: {report.crosscheck_logprob!r} "
        f"差值={report.crosscheck_diff!r} (容差 {report.tolerance}) 一致={report.consistent}"
    )

    out("== 3. 平滑系数调优与缓存失效 ==")
    probe = "他用Python写代码。"
    first = seg.tag_sentence(probe)
    seg.tag_sentence(probe)
    out(f"两次解码 logP={first.logprob!r}，缓存状态: {seg.cache_info()}")
    old_fp = seg.fingerprint
    seg.set_smoothing(0.5)
    out(f"平滑系数 0.1 -> 0.5，指纹变化: {old_fp != seg.fingerprint}")
    out(f"新指纹: {seg.fingerprint}")
    out(f"缓存状态: {seg.cache_info()}")
    rebuilt = seg.tag_sentence(probe)
    out(f"重建后 logP={rebuilt.logprob!r}，旧版本结果未被误用: {rebuilt.logprob != first.logprob}")
    seg.set_smoothing(0.1)
    out(f"还原平滑系数 0.1，指纹: {seg.fingerprint}")

    out("== 4. 错误注入（验证稳定业务错误码） ==")
    try:
        Segmenter.train(TRAIN, smoothing=-0.5)
        out("非法平滑系数 -> 未触发错误（异常！）")
    except SegTagError as exc:
        out(f"非法平滑系数 -0.5 -> {exc.code}: {exc.message}")

    seg_num = Segmenter.train(TRAIN, smoothing=0.1)
    seg_num.inject_emission_for_test("爱", "S-v", float("-inf"))
    bad_numeric = seg_num.tag_sentence("我爱北京。", sentence_index=7)
    out(
        f"发射注入 -inf -> ok={bad_numeric.ok} code={bad_numeric.error.code} "
        f"句号={bad_numeric.error.sentence_index} 字位={bad_numeric.error.char_index}"
    )
    out(f"    {bad_numeric.error.message}")
    seg_nan = Segmenter.train(TRAIN, smoothing=0.1)
    seg_nan.inject_emission_for_test("北", "B-ns", float("nan"))
    bad_nan = seg_nan.tag_sentence("我爱北京。", sentence_index=8)
    out(
        f"发射注入 NaN  -> ok={bad_nan.ok} code={bad_nan.error.code} "
        f"句号={bad_nan.error.sentence_index} 字位={bad_nan.error.char_index}"
    )

    bad_tag = seg.validate_tags("我爱北京", ("B-r", "E-v", "S-n", "S-w"))
    out(f"词内词性不一致 -> ok={bad_tag.ok} code={bad_tag.error.code}: {bad_tag.error.message}")
    bad_pos = seg.validate_tags("好", ("S-zzz",))
    out(f"未知词性       -> ok={bad_pos.ok} code={bad_pos.error.code}: {bad_pos.error.message}")
    bad_empty = seg.tag_sentence("")
    out(f"空句           -> ok={bad_empty.ok} code={bad_empty.error.code}: {bad_empty.error.message}")

    with open(SUMMARY, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"\n摘要已写入 {SUMMARY}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

