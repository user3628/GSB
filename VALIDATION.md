# 实际验证记录

验证日期：2026-11-21（Asia/Shanghai）

## 环境

- Python：3.14.4（代码声明并保持 Python 3.11+ 兼容）
- NumPy：2.5.5
- 平台：本地 Linux 文件系统，无 GPU、无容器、无网络服务依赖

## 自动测试

命令：

```bash
python -m unittest discover -s tests -v
```

结果：

```text
Ran 20 tests
OK
```

覆盖范围包括核心前向、参考容差、确定性与字节一致、前缀增量复用、配置失效、断点恢复、截断检查点、重复序号隔离、跨行块发布拒绝、非法续算、数值异常、内存预算和 CLI 查询。

## 完整演示

命令：

```bash
python scripts/run_demo.py --root .
```

关键输出：

```text
FlashBlock deterministic demo
  aggregate_sha256: c46130f941b857e01d17e83aee7f9da7c75949462607c84b5e6c2d611c1f108c
  short_causal: shape=[2, 2, 128, 32] reference_ok=True fresh_bytes=True
  long_causal: shape=[1, 2, 1024, 64] reference_ok=True fresh_bytes=True
  odd_dim_causal: shape=[2, 1, 257, 40] reference_ok=True fresh_bytes=True
  resume: paused=1 rowblocks, reused=[0], bytes_ok=True
  prefix update: reused_rows=[0], recomputed_rows=[1, 2, 3, 4], bytes_ok=True
  summary: artifacts/demo_summary.json
```

验证结论：

- 3 个分片的输出均与逐行全量参考实现在 `atol=1e-10`、`rtol=1e-9` 内一致。
- 3 个分片的输出、行最大值、归一化因子均与从零分块计算逐字节一致。
- 从磁盘缓存重新加载的结果与首次完整运行逐字节一致。
- 第 1 个分片处理 1 个行块后暂停，新引擎对象恢复后的结果与从零计算逐字节一致。
- 第 3 个分片修正 key 区间 `[80, 112)`：行块 0 复用，行块 1-4 重建；受影响行块内未变化列块复用，变化列块重算；最终输出与从零计算逐字节一致。
- 增量结果写入新运行代际后，重新加载仍与增量结果逐字节一致。

机器可读详情见 `artifacts/demo_summary.json`。

