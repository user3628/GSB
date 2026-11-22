# 实际验证记录

验证日期：2026-11-23（Asia/Shanghai）

## 环境

| 项目 | 值 |
| --- | --- |
| Python | 3.11.18 |
| NumPy | 2.4.6 |
| pytest | 9.2.0 |
| 引擎版本 | 1.0.0 |
| 样本种子 | 20261123 |
| 容器/外部服务 | 未使用 |

Python 3.11 通过 `uv python install 3.11` 安装并在 `.venv` 中执行；同一套测试也已在系统 Python 3.14.4 / NumPy 2.2.1 上通过。

## 测试命令

```bash
.venv/bin/pytest
```

结果：

```text
......................                                                   [100%]
24 passed in 8.26s
```

覆盖范围：

- WAV/清单生成、16 kHz / 单声道 / 16-bit 头检查、三份 demo 清单；
- 参考活动区间、重叠标记、唯一归属、待复核双候选；
- 非法滞回阈值、零帧移、音区偏移越界、词表引用缺失、NaN 强度；
- 区间切分/合并、编辑版本轨迹、同序列逐字节确定性；
- 优先级变更、帧参数变更、样本替换的最早失效阶段与上游缓存复用；
- 增量结果与干净全量构建逐字节比对、首个差异字节报告；
- 优雅停止、恢复、损坏 JSON、重复序号、强制终止等价恢复；
- 两个真实进程并发发布检查点，仅一个连续序号成功，另一个 `E_STALE_CHECKPOINT`；
- 标准库 HTTP 导入、构建、状态、指纹、配置变更、样本替换、错误映射。

## 端到端验收命令

```bash
.venv/bin/python -m cabin_voice validate --output validation_run
```

结果：`all_checks_passed: true`。

| 检查 | 结果 | 实测指标 |
| --- | --- | --- |
| 参考活动与重叠仲裁 | 通过 | 10 个区间比较，数量差异 0；最大起点偏差 10 ms，最大终点偏差 5 ms；8 条唯一归属；6 个重叠标记 |
| 重叠指令待复核 | 通过 | `chunk_002:open_window` 保留主驾/副驾两个候选，`responsible_zone=null` |
| 优雅停止与恢复 | 通过 | 第 5 个检查点停止；恢复后序号 1..12 连续；最终 JSON 与全量构建逐字节一致 |
| 优先级增量 | 通过 | 最早失效 `attribution`；detection/delay 来源均为 `cache`；增量/全量 SHA-256 相同 |
| 样本替换增量 | 通过 | 最早失效 `detection`；原因 `SAMPLE_CONTENT_CHANGED`；下游全部重建并与全量一致 |
| 损坏/重复检查点恢复 | 通过 | 损坏 JSON 与重复 seq 1 被隔离；从空前缀重建至 seq 12；最终字节一致 |
| 偏移越界注入 | 通过 | `E_ZONE_OFFSET_OUT_OF_BOUNDS`，`zone_id=driver`，`parameter=offset_samples` |

## 产物摘要

| 产物 | SHA-256 |
| --- | --- |
| 全量 final JSON | `70b19c8d7e62f306595ea08117e19ab3eeb66254fe2057b3e3a40b98a32139a8` |
| 增量 final JSON | `d5b5f4208766de227c6f3b72f739697d2004f6f3c9512903c35a69450bda3801` |
| 样本替换后 final JSON | `751b5d87cbe3340a2b350564e55de47375a4eb62bfedffa52a5218cfcc17fd59` |
| 会话 summary digest | `a5a6b3f938e38cfe0410b324fc24d4c173a2b3cd735c8ff8f04176dd5ede3cd6` |

注意：增量 final JSON 与全量参考在各自构建时逐字节相同；表中“全量”与“增量”SHA-256 不同是因为增量用例在构建前执行了优先级编辑，编辑版本和归属结果不同。每次 `verify=True` 的构建都会另建干净工作区，在同一输入/配置/编辑状态下做全量重建并逐字节比较，报告中的 `verification.equal` 为 `true`。

机器可读报告：`validation_report.json`。
