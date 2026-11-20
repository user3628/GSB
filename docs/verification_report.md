# 验证报告

验证环境：Python 3.14.4、NumPy 2.5.5、Linux（Asia/Shanghai）；无 GPU、无容器、
无外部网络服务。所有命令在仓库根目录执行。

## 1. 自动化测试

`python -m pytest tests/ -q` 结果：**30 passed**。

覆盖范围：
- 样本生成器确定性、4 类场景与参考序列完整性；
- 四阶段产物完整性、跨两次独立运行逐字节一致；
- 单/双栏判定、栏区间不重叠且覆盖有效版面宽度、通栏识别；
- 同行碎片框确定性合并（2 组合并、源框被吸收）；
- 页眉/页脚归类；阅读顺序列优先规则、无重复块；
- 顺序质量：4 个文档逆序数均为 0、`order_accuracy=1.0`；
- `E_BOX_INVALID`（越界、负坐标、零面积、重复框标识）；
- `E_COLUMN_UNASSIGNED`（正文框为空）；
- `E_ORDER_INVALID` 环检测路径与 `E_INCREMENTAL_MISMATCH` 比对；
- `E_INVALID_STAGE` / `E_STALE_CHECKPOINT`（非法跳转、重复发布、跨文档错位、
  未知阶段），且被拒绝发布不改变已有状态；
- 优雅停止后恢复，四阶段产物与从零全量工作区逐字节一致；
- 版本变化失效传播（合并阈值变化全链路重建；置信度权重变化仅 order/summary
  重建，normalize/column 指纹保持不变并复用）；
- 多进程（spawn）并发发布：4 个竞争者仅 1 个成功，其余全部
  `E_STALE_CHECKPOINT`；两个并发全量运行收敛；
- HTTP 全流程与 404 错误码；CLI 端到端（generate/analyze/stop/resume/verify/
  export/fingerprints）。

## 2. 一次完整运行的固定摘要

`workspace/exports/*.result.json`（默认配置 `cfg-2026-11-21-v1`）：

| doc_id | 栏数 | 原始框/块 | 合并组 | 通栏块 | 逆序数 | 准确率 | 置信度 | result_digest(前16) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| doc_double_en | 2 | 11 / 11 | 0 | 1 | 0 | 1.0 | 0.669840 | 067928865f4d283e |
| doc_double_mixed | 2 | 12 / 12 | 0 | 2 | 0 | 1.0 | 0.815086 | b6e07eaa78857b8b |
| doc_single_mixed_fragments | 1 | 10 / 8 | 2 | 6 | 0 | 1.0 | 0.700159 | 671563bc2833c6d5 |
| doc_single_zh | 1 | 7 / 7 | 0 | 5 | 0 | 1.0 | 0.658889 | aba671ff4df8b2cf |

导出包整体摘要（对四个导出文件摘要再做 SHA-256）：
`17afdcf00f521737a22ad06b08eb2ef9fe0792bb71d6131b09510d199895d13b`

完整 64 位 result_digest：

- doc_double_en: `067928865f4d283e1847f167aebd2b0d99387f1886b871964cd29199710214c1`
- doc_double_mixed: `b6e07eaa78857b8b60fcad412151d6ee6d631eac49c178be66106573805f4746`
- doc_single_mixed_fragments: `671563bc2833c6d54910dc34f370a459d7e65531ff932940883dcf37694232bb`
- doc_single_zh: `aba671ff4df8b2cefafef7e8310d2b90fff0afa11d3dc9debe661065738054968`

## 3. 停止-恢复逐字节一致性

`bash scripts/demo_resume.sh` 在 `doc_double_mixed` 的 column 阶段后停止，
恢复完成后与独立的从零全量工作区逐文件比对 4 文档 × 4 阶段共 16 个检查点：

```
RESULT: BYTE_IDENTICAL
```

## 4. 错误注入

`python scripts/demo_error_injection.py`：7 类注入全部命中预期错误码，
输出 `ERROR_INJECTION_RESULT: ALL_PASS`。

## 5. 失效传播实测

对 `doc_single_mixed_fragments` 先以默认配置构建，再仅修改顺序置信度权重
（0.6/0.4 → 0.8/0.2）重新运行：
- normalize、column 阶段指纹不变并被复用；
- 最早失效阶段为 `order`，被删阶段为 `["order","summary"]`；
- `invalidation.jsonl` 记录原因与期望/现存指纹，旧结果不会被冒充。

## 6. HTTP 应用

`bash scripts/demo_http.sh 8099` 验证 health/import/analyze/status/spanning/
result/stop/invalidations 全部返回预期 JSON，服务仅绑定 127.0.0.1。

## 7. 结论

四阶段分析、块序列与跨栏统计在确定性样本上结果稳定；阶段间停止恢复与全量
运行逐字节一致；版本失效传播、并发发布保护、稳定错误码与增量/全量一致性
校验均按要求工作。
