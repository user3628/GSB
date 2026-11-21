# 实际验证记录

验证日期：2026-11-22（Asia/Shanghai）

## 环境

```text
Python 3.11.18
numpy 2.4.6
fastapi 0.165.0
SQLite 3.53.1
Uvicorn 0.54.0
```

使用`.venv`独立虚拟环境，未使用 Docker、GPU、外部账号或外部数据集。`127.0.0.1:8767` 通过真实 socket bind 和 Uvicorn 进程验证可用。

## 自动化测试

命令：

```bash
.venv/bin/python -m pytest -q
```

结果：

```text
20 passed
```

覆盖内容：

- 策略重复/陈旧提交、并发同基础版本只有一个增量成功。
- 同版本同样本两次运行的阶段摘要、分片 manifest 和全部张量文件逐字节一致。
- 21 行合法固定样本：20 个唯一样本、1 条重复移除、过滤后 15 条、采样后 12 条。
- 畸形行整批拒绝；重复冲突保持既有提交不变。
- 五类异常注入返回五个不同业务码。
- 分片无重叠/无遗漏；偏斜告警不改变归属。
- 同批次并发提交只执行一次，等待者复用同一 `attempt_id` 和结果。
- 同一策略版本的不同批次串行执行。
- 写批次运行期间，多读者并发读取旧完整提交，并且新批次不可见。
- 验证器首个偏离阶段、两侧摘要和建议范围。
- 任意历史策略 v1/v2 成对回放核对。
- 子进程 `os._exit(71)` 强制终止后，重启扫描恢复到最后完整阶段 `filter`，批次中止且无已提交分片。
- HTTP 成功路径和统一 JSON 错误体。

## 一键验收

命令：

```bash
.venv/bin/python scripts/acceptance.py
```

结果：

```text
passed: true
checks: 18
report: artifacts/acceptance-report.json
```

18 项检查：

```text
PASS loopback_port_8767_available
PASS same_policy_stage_digests_equal
PASS same_policy_shard_manifests_equal
PASS same_policy_tensor_files_byte_equal
PASS duplicate_first_kept_and_counted
PASS verifier_self_recompute_matches
PASS skew_warning_without_reassignment
PASS injection_accept-inject-label
PASS injection_accept-inject-shard
PASS injection_accept-inject-ratio
PASS injection_accept-inject-conflict
PASS injection_accept-inject-interrupt
PASS five_injection_codes_distinct
PASS injection_failures_do_not_change_committed_batch
PASS forced_termination_exit_71
PASS recovery_uses_last_complete_stage
PASS recovery_batch_aborted
PASS verifier_reports_first_divergence
```

关键实际值：

- 同版本两次运行五阶段 output digest 完全相等。
- 3 个分片共 21 个 `.npy`/manifest 文件逐字节相等。
- 固定策略 v1 的 stage digest：

```text
parse_clean sha256:4261af5efcb7d77c69f5b30e9d6d7cf11a5bbfdac71660f663d8613bd81d90d3
filter      sha256:2b0233c825a0239b7c068e55abef20db8245a1907a45fcb3bfc9918d08edd07d
shard       sha256:883b30d706c57d34a938b36bb6f90dc476b101528d7de1833e87b6c380139e2c
sample      sha256:6aa5b1c5587f78fbd88efe1c0d4e5e460868948619f18fe4b614b39e37656dfc
tensorize   sha256:b8e4cba914dc2ee1dc1eb1933a397a942794b8f6b19f7e1ccc7486ebfd87f9d2
```

- 五类注入实际返回：

```text
invalid_label       E_LABEL_INVALID
missing_shard_key   E_SHARD_KEY_MISSING
sample_ratio=1.25   E_SAMPLE_RATIO_INVALID
duplicate_conflict  E_CONFLICTING_RECORD
interrupt@shard     E_INJECTED_INTERRUPT
```

- 强制终止子进程退出码：`71`。
- 恢复前持久化事实：`parse_clean`、`filter` 两个阶段为 `completed`。
- 恢复结论：批次 `aborted`，最后完整阶段 `filter`，`committed_shards=[]`，staging 已移除，`exit_report_available=false`，`uncommitted_artifacts_recoverable=false`。
- 人为篡改 `sample` 阶段持久化 output digest 后，验证器返回 `E_REPRO_MISMATCH`，首个偏离阶段为 `sample`，并返回存储侧与重算侧摘要。

## 真实 HTTP 冒烟

命令：

```bash
.venv/bin/python -m prep_pipeline.cli serve --host 127.0.0.1 --port 8767
curl http://127.0.0.1:8767/health
curl http://127.0.0.1:8767/v1/batches/smoke_v1b/stages
curl -X POST http://127.0.0.1:8767/v1/verify/smoke_v1b -H 'content-type: application/json' -d '{}'
```

实际结果：

```text
GET  /health                 200
GET  /v1/batches/.../stages  200
POST /v1/verify/...          200, status=match
```

## 机器报告

完整验收结果保存在 `artifacts/acceptance-report.json`，其中包含执行环境、每项检查的 `passed` 布尔值和实际上下文。验收脚本每次会重建 `artifacts/acceptance/` 数据库和产物，避免旧结果污染。
