# 实际验证结果

验证时间：2026-11-26（Asia/Shanghai）。

## 一键验收

执行：

```bash
make verify
# 等价于 ./scripts/verify.sh
```

完整日志保存在 `artifacts/validation/verify-run.log`，机器可读摘要保存在 `artifacts/validation/summary.json`。

脚本实际完成：

1. `go test ./...`：`internal/api`、`internal/app`、`internal/engine`、`internal/nlp`、`internal/store` 均通过。
2. 构建 `bin/csrouter`。
3. 在临时 root 生成并导入两档样本。
4. 校验 6 个分片的声明行数/列数与实际一致。
5. 运行 small 与 medium，并比对本地高精度参考器。
6. 再次运行 small，比较报告指纹。
7. 分别建立连续运行 root 与 `--stop-after 41` 后恢复 root，逐字节 `cmp` 两份 `report-small.json`。

最终输出：

```text
ok  csrouter/internal/app
ok  csrouter/internal/engine
ok  csrouter/internal/nlp
small rerun fingerprint: b14c790d951b8ae6ffabe13c1135d7e4c29c674457115fef21f957131f9ed7ae
continuous and resumed reports are byte-identical
== verification passed ==
```

## 样本行列校验

固定种子 `20261126`：

| 档位 | 工单数 | 意图数 | 分片 | 每片行数 | 列数 |
|---|---:|---:|---:|---:|---:|
| small | 400 | 8 | 2 | 200 | 7 |
| medium | 4000 | 24 | 4 | 1000 | 7 |

`/api/validate` 与 CLI `validate` 均返回 `ok=true`；每个 JSON 对象的实际 key 数为 7。

## 两档核心指标

最终报告指纹：

| 档位 | 策略 | report SHA256 | 参考结论 |
|---|---|---|---|
| small | `exact_full` | `b14c790d951b8ae6ffabe13c1135d7e4c29c674457115fef21f957131f9ed7ae` | PASS |
| medium | `hash_bucket_budget` | `0dff36931a1a7151fe753e331770a97cab2944f869bae4b5d37329e37a844a5c` | PASS |

### Small

规模观测：400 tickets、8 intents、311 unique terms、15,168 term occurrences、hash dim 1024、budget 180,000。

候选成本：

| 策略 | estimated ops | fits budget |
|---|---:|---|
| `exact_full` | 121,344 | true |
| `vocab_priority_truncated` | 16,896 | true |
| `hash_bucket_budget` | 113,424 | true |

小档仍明确选择 `exact_full`，因为规模小且精确策略无近似；淘汰 hash 的理由是不需要为大规模词表/预算约束引入碰撞机制。

参考比对：400/400 Top1 相同，400/400 候选分数逐位相同，最大分差 `0`，后验差 `0`。

标签/优先级统计：

```text
intent_label_accuracy = 0.9825
priority_accuracy     = 0.828715
HIT=329 MISS=22 FALSE_POSITIVE=46
UNCOMPARABLE_PRIORITY=2 INVALID_LABEL=1
empty_text=3 degraded=5
```

### Medium

规模观测：4000 tickets、24 intents、781 unique terms、151,058 term occurrences、hash dim 4096、budget 3,500,000。

候选成本：

| 策略 | estimated ops | fits budget |
|---|---:|---|
| `exact_full` | 3,625,392 | false |
| `vocab_priority_truncated` | 457,560 | true |
| `hash_bucket_budget` | 3,469,920 | true |

最终选择 `hash_bucket_budget`：精确全量超过预算；hash 成本符合预算，且 4096 桶相对 781 词项足够稀疏。词表截断也符合预算，但会丢弃长尾信号；当前数据上低碰撞 hash 对参考结果保持了更好的后验一致性。

参考比对：

```text
intent_top1_matches = 4000 / 4000
priority_matches    = 3999
max_posterior_diff  = 0.01227201915
mean_centered_score_diff = 0.990517600443
pass = true
```

原始 log score 的最大差为 19.452、均值差为 10.369，这是 hash 分桶聚合和未归一化 log 质量项导致的绝对分平移；因此验收同时比较中心化分数边际与 posterior。Top1 和优先级裁决与高精度 exact reference 全量一致。

标签/优先级统计：

```text
intent_label_accuracy = 0.99925
priority_accuracy     = 0.832124
HIT=3326 MISS=170 FALSE_POSITIVE=501
UNCOMPARABLE_PRIORITY=2 INVALID_LABEL=1
empty_text=3 degraded=5
```

## 确定性与恢复

- 同一 root 再跑一次 small：报告指纹保持 `b14c790d...ed7ae`。
- `--stop-after 41` 返回 `STOP_REQUESTED`、`completed=41`。
- `resume` 后完成 400 条。
- 连续运行与中断恢复的 `report-small.json` 通过 `cmp` 逐字节比较。
- Go 测试 `TestStopAndResumeReportByteIdenticalToContinuous` 固化该约束。

## 并发发布与错误信封

- `TestCheckpointStageAndSequenceErrors` 固化缺失前置阶段返回 `E_ILLEGAL_STAGE`，重复发布和错误 `expected_head` 返回 `E_STALE_CHECKPOINT`，错误均带 `ticket_id` 与 `config_version`。
- `TestConcurrentPublishersMaintainContiguousStageSequence` 使用 16 个独立 `store.Store` 句柄并发写同一个 SQLite 文件；`TestMultipleProcessPublishersMaintainContiguousSequence` 再自派生 8 个测试子进程跨 OS 进程发布。真实 `flock` 加事务条件更新后，`stage_cursors.last_seq` 等于发布数，artifacts 序号连续，ledger 行数一致。
- `TestPublishCheckpointErrorUsesCurrentPolicyVersion` 验证请求即使传入 `"config_version":999`，HTTP 409 错误仍报告库内当前生效版本 `1` 和当前 `version_fingerprint`。
- `go test -race -count=1 ./internal/store ./internal/api` 通过；仅出现 mattn/go-sqlite3 amalgamation 的第三方 C 编译 const 警告。

## 增量失效实测

在最新二进制的临时 root 中，small 全量完成后：

### 优先级阈值变更

```text
CONFIG_UPDATED v1 -> v2, earliest_stage=priority, invalidated_artifact_rows=800
stage counts: normalize=400 intent=400 priority=0 verify=0
```

800 行对应 400 张工单的 `priority` 与 `verify`；规范化和意图产物全部复用。

### 平滑强度置零

配置改为 `smoothing_alpha=0` 后执行返回退出码 1 和结构化错误：

```json
{
  "code": "E_INVALID_SMOOTHING",
  "message": "smoothing_alpha must be non-zero for log-domain scoring",
  "ticket_id": "S-0001",
  "intent": "account_access",
  "config_version": 3
}
```

### 单分片替换

`replace-shard --demo` 修改 `small/shard-000.jsonl` 中 `S-0021` 的文本：

```text
SHARD_REPLACED direct=['S-0021'] model_affected=400 reused_normalize=399
stage counts after replacement: normalize=399 intent=0 priority=0 verify=0
```

直接文本变化只使 `S-0021` 的 normalize 失效；由于 NB 模型从全量标注统计训练，模型指纹变化后全部 400 条的 intent 及下游失效，其余 399 条 normalize 继续复用。

## SQLite 与 HTTP 冒烟

最终 demo 数据库检查：

```text
PRAGMA integrity_check = ok
artifacts: normalize/intent/priority/verify 各 4400 行
stage_cursors: 每阶段 last_seq=4400
artifact_ledger: 17600 行
```

HTTP 实测 `127.0.0.1:18081`：

- `GET /api/health` 返回 `OK`。
- `GET /api/strategy?tier=medium` 返回 hash 决策和候选成本。
- 未发布前置 intent 而发布 priority：HTTP 409 + `E_ILLEGAL_STAGE`，含 `ticket_id` 和 `config_version`。
- 使用错误 `expected_head=999` 发布 normalize：HTTP 409 + `E_STALE_CHECKPOINT`。
- `POST /api/run?tier=small` 带 `{"stop_after_tickets":5}` 返回 `STOP_REQUESTED completed=5`。
- `POST /api/resume?tier=small` 返回 `RUN_COMPLETED`，报告指纹仍为 `b14c790d...`，参考检查通过。

## 已知边界

- 中档选择的是有界 hash 近似策略，因此原始未归一化 log score 不要求与 exact reference 逐位相同；验收要求 Top1、优先级和 posterior 误差通过阈值。
- 生成数据包含刻意注入的标注噪声，因此业务标签准确率不是 100%；这是为了让命中、漏判、误判和降级明细都有非空样本。
- 报告不包含墙钟时间、随机 map 遍历或主机路径，以保证相同配置下字节稳定。
