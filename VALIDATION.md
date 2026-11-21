# ShardSeek 实际验证记录

验证日期：2026-11-22  
工作目录：`/home/zsl/feedback/uid37fecb20-target-mwnkf3ge-77`

## 环境与依赖

```text
Python 3.11.18
numpy 2.4.6
Pillow 13.0.0
fcntl.flock-ok
```

`requirements.txt` 已固定版本；未使用 Docker、GPU、深度学习框架或外部服务。本地 HTTP 端口与 `ThreadingHTTPServer` 已验证。

## 自动测试

命令：

```bash
.venv/bin/python -m unittest discover -s tests -v
```

实际结果：

```text
Ran 29 tests in 15.486s

OK
```

覆盖范围包括：

- 固定种子生成器、64×64 RGB、特征/分块摘要逐字节稳定；
- Unicode NFKC/空白折叠/小写化、空 caption、空图像、网格越界；
- 投影维度不匹配、NaN/Inf、负/零缩放；
- 8 位/4 位、逐维/逐向量量化与声明误差界；
- 双向检索、并列 item_id 仲裁、top-k/空查询/未建索引错误；
- 4 位误差界大于 8 位；
- 增量最早阶段识别与全量逐字节对比；
- 真实多进程 `flock` 并发发布竞争；
- 检查点前置阶段、重复/跨分片错序、连续序号；
- 快照回退、混合索引拒绝；
- 优雅停止/恢复与一次全量构建一致性；
- HTTP reset/build/search/diagnostics/error/image 路由；
- 完整 demo 证据生成。

## 固定 demo

命令：

```bash
./run.sh --root runtime demo
```

实际结果：`demo_status=passed`，`artifacts/DEMO_SUMMARY.sha256`：

```text
773e37a53f6377580f33159d907dedce4e5df9b818b8d54e2c94a654ffeaf23a
```

连续运行两次，stdout 与 `demo_sha256` 均逐字节一致。

## 样本与阶段指纹

样本：8 个 PNG 分片，7 个可索引，1 个空 caption 隔离异常；`item-06` 与 `item-01` 为跨分片明暗近似重复项。

| 项目 | 8 位基线 | 4 位变更 |
|---|---|---|
| config fingerprint | `3333448eeed1ea00d63c5ec77134bf3bd86dd99121ce7d7886e85c10b77df8b0` | `2e4fb9893dbbb0ffb28640f0b3973a08ca06b20bc4261f473d8670769af78538` |
| representation summary SHA-256 | `4bf48544d1f65bfc4f6d14af48e848fbf0823f7b1829279b9a3e33e04dd41c29` | `233a5c0de9603b0a6d0fc9431cf16ecc5f96e54b53b5ded22446037442b4b122` |
| index summary SHA-256 | `ae2468330a6de977652e873349707566f69ac59637bbffcb8c1aa35a245c562c` | `9495cd1e81caa417a6212bcb149c5e828212de72535b28007ee0523bdb367739` |

## 双向检索

查询：`红色方块 blue gradient`，8 位基线 top-3：

| rank | item_id | source | combined | dense cosine | verified bound | digits |
|---:|---|---|---:|---:|---:|---:|
| 1 | item-06 | shard-05 | 0.923689993729286 | 0.894013880179563 | 0.0240385886589323 | 1 |
| 2 | item-01 | shard-00 | 0.921110782495124 | 0.890431642354338 | 0.0221695272782663 | 1 |
| 3 | item-05 | shard-04 | 0.207275910662415 | 0.189231247954267 | 0.0711344792673841 | 0 |

图像 `item-01` → 文本，8 位基线 top-3：

| rank | item_id | source | combined |
|---:|---|---|---:|
| 1 | item-01 | shard-00 | 0.731047277689148 |
| 2 | item-06 | shard-05 | 0.340679635471324 |
| 3 | item-05 | shard-04 | 0.0450713496233249 |

跨分片近重复 `item-01/item-06` 在两种方向均位于前二。检索结果含视觉/文本对数贡献、稠密/倒排分量、参考余弦、紧/解析误差界、有效位数和来源分片。重复调用相同查询，报告 JSON 字节一致。

## 量化位宽与误差界

全语料 49 个跨模态对的最后规模（7 分片）实际值：

| 指标 | 8 位 | 4 位 |
|---|---:|---:|
| max verified similarity error bound | 0.00505148096664531 | 0.0702108216223369 |
| max analytic scale-only bound | 0.0266650852147318 | 0.649577964721297 |
| 4/8 verified bound ratio | — | 13.899056947048921 |

所有检索/语料对均满足 `actual_error <= analytic_bound`；报告中的紧界再加 `1e-12` 验证余量。4 位界显著大于 8 位，符合位宽变化预期。

## 增量 vs 全量

命令：

```bash
./run.sh --root runtime verify-incremental
```

实际结果：

```text
all_equal: true
earliest_stage: project_quantize
reused_stages: [feature_extract]
representation_summary_bytes_equal: true
index_summary_bytes_equal: true
search_reports_bytes_equal: true
corpus_diagnostics_bytes_equal: true
verification_sha256: 0acbd67138652ddbdafb1656a04b0ec14ca7284383ab3494851e64a2ba2d0116
```

额外测试覆盖：caption 变化从 feature 阶段开始、投影变化从 project/quantize 阶段开始、排序权重变化从 index 阶段开始，且变更后与独立从零全量构建的摘要/排序逐字节一致。

## 停止与恢复

实际演示在第 2 个分片后停止：

```text
completed_shards = 2 / 7
checkpoint_last_seq = 8
next_expected = item-03, feature_extract, stage_index=1
```

恢复后：

```text
representation_summary_bytes_equal = true
index_summary_bytes_equal = true
search_reports_bytes_equal = true
corpus_diagnostics_bytes_equal = true
checkpoint_sequence_continuous = true
checkpoint_count_expected = true
verification_sha256 = 72168d535ac3135d3eda340131945562c42ade3a041ba09cdd162fd8b809edb0
```

当前会话共 28 条检查点，序号严格为 `1..28`，无跳跃或重复。

## 并发与快照安全

- 两个独立进程同时发布同一期望序号：恰好 1 个成功，另一个返回 `E_STALE_CHECKPOINT`；
- 阶段 2 先于阶段 1：返回 `E_INVALID_STAGE`；
- 跨分片先发布后一分片：返回 `E_STALE_CHECKPOINT`；
- 将旧表征文件复制回新快照：检索返回 `E_SNAPSHOT_REGRESSION`；
- 同序号但摘要被篡改：检索返回 `E_SNAPSHOT_MISMATCH`；
- 将旧索引复制回新表征：拒绝使用，返回 `E_SNAPSHOT_REGRESSION`；
- 保持顶层摘要不变但篡改索引中的定点向量：重新核对条目/向量摘要后返回 `E_SNAPSHOT_MISMATCH`。

## HTTP 实际检查

启动：

```bash
./run.sh --root runtime serve --port 8767
```

实测：

```text
GET /api/health                      -> 200 {"ok":true,"service":"shardseek","version":"1.0.0"}
GET /                                 -> 200 HTML UI
GET /assets/app.js                    -> 200 JavaScript
GET /api/image/item-01                -> 200 PNG 64×64 RGB
POST /api/search (text→image, k=3)    -> 200，top-1 item-01
POST /api/search (k=999)              -> 400 E_INVALID_K
```

UI 还实际支持配置切换、构建、停止/恢复、报告导出、检查点与误差曲线展示。

## 错误码演示

demo 中实际触发并记录：

```text
dimension_mismatch   E_DIM_MISMATCH
empty_caption        E_EMPTY_CAPTION
empty_query          E_QUERY_EMPTY
index_not_ready      E_INDEX_NOT_READY
invalid_k            E_INVALID_K
stale_checkpoint     E_STALE_CHECKPOINT
```

## 机器可读产物

- `artifacts/demo_report.json`：完整固定摘要与结果；
- `artifacts/DEMO_SUMMARY.sha256`：demo SHA-256；
- `artifacts/incremental_verification.json`：增量/全量比较；
- `runtime/`：实际运行后的库、索引、参考向量、检查点与验证子工作区（已在 `.gitignore` 中）。

