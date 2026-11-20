# 实际验证记录

## 环境

```text
OS              WSL2 Ubuntu (task-provided environment)
Python          3.14.4
NumPy           2.5.5
PyYAML          6.0.3
pytest          9.2.0
```

Windows 入口脚本与 `msvcrt` 文件锁分支已提供，但本次物理命令执行发生在上述 WSL2 环境。

## 自动化测试

命令：

```bash
.venv/bin/pytest -q
```

结果：

```text
........................                                                 [100%]
25 passed
```

覆盖内容：

- 对称/非对称量化、零点、量化域约束、钳制计数/位置。
- 零缩放与 NaN/Inf 的 `E_QUANT_INVALID` 层名详情。
- 全量误差序列确定性和显式绝对上界。
- 归因闭合、排序复核、预算诊断、`E_ATTRIBUTION_GAP`。
- 增量前缀复用、与全量结果逐字节一致、缓存不一致业务码。
- 连续版本、过期发布、缺失阶段、预算回滚与历史状态一致。
- 并发双进程发布只允许一个连续版本。
- 记录写入中断和 HEAD 写入中断后的原子恢复。
- HEAD/整体历史回退、结果摘要与参数指纹篡改区分。
- CLI 成功/失败退出码及完整演示固定摘要。

## 演示报告

命令：

```bash
.venv/bin/python -m quant_audit --pretty demo \
  --workspace demo_run --data-dir demo_data \
  --report artifacts/demo_report.json --force --faults
```

关键结果：

```text
dataset_digest        eb02e26f37694048d155949e8eca0aa5fc9cd0a946499bb33bf9ffc7dff8d23d
deterministic_digest  08d8aae842ddc42b6e8607ed5738f7aded94fe2573ed0c24724bef948b2e829a
layer names           linear_00 .. linear_05
dimensions            6 -> 10 -> 8 -> 7 -> 6 -> 5 -> 4
```

版本结果：

| 版本 | 模式 | 配置/样本 | 端到端相对 L2 | 状态 |
|---|---|---|---|---|
| v1 | full | 8-bit 混合基线 + clean | 0.030430485319426704 | 合规 |
| v2 | incremental | `linear_03` 改为 7-bit + clean | 0.028486257231737113 | 合规，活动 |
| v3 | full | 2-bit + outlier | 1.762345847827907 | 全部越界，不可用 |

v2 增量结果：

```text
earliest_recomputed_layer = 3
reused_layers             = linear_00, linear_01, linear_02
recomputed_layers         = linear_03, linear_04, linear_05
verified_against_full     = true
```

回滚结果：

```text
published_over_budget_version = 3
active_version_after_rollback = 2
result_matches_historical     = true
budget_matches_historical     = true
over_budget_layers            = all six layers
```

## 故障码验证

| 注入 | 业务码 |
|---|---|
| 校准激活含 NaN | `E_QUANT_INVALID` |
| 偏置全零导致缩放因子为零 | `E_QUANT_INVALID` |
| 2-bit 离群样本且 closure tolerance = 0 | `E_ATTRIBUTION_GAP` |
| 篡改增量前缀缓存张量 | `E_INCREMENTAL_MISMATCH` |
| 以旧基线重复发布 | `E_STALE_VERSION` |
| 删除 v2 阶段归档 | `E_INVALID_STAGE` |
| 回滚到预算越界版本 | `E_INVALID_ROLLBACK` |
| HEAD 版本回退 | `E_VERSION_ROLLBACK` |
| 用 v2 整体替换 v3 历史且保留 v3 锚点 | `E_VERSION_ROLLBACK` |
| 修改结果摘要 | `E_AUDIT_TAMPERED` |
| 修改层参数指纹 | `E_AUDIT_TAMPERED` |

完整机器可读记录在 `artifacts/demo_report.json`。

