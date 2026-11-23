# 架构、事务与恢复

## 1. 包边界

```text
httpapi -> service -> store -> SQLite
                  \-> domain -> numeric
```

- `numeric` 只处理确定性整数/有理数/分位/CI。
- `domain` 不依赖数据库，负责校验、门槛规则和状态迁移矩阵。
- `store` 独占 SQL schema 和恢复扫描。
- `service` 负责每发布批次单写者、版本生成和事务边界。
- `httpapi` 只做传输层、查询参数和统一错误映射。

## 2. SQLite 表

| 表 | 作用 |
|---|---|
| `releases` | 发布头：状态、当前阶段、流量、当前统计版本 |
| `stats_versions` | 不可变完整快照：阶段/累计统计、估计、误差契约 |
| `observation_batches` | 批次状态：`STAGING`、`COMMITTED`、`ABORTED` |
| `observations` | 原始事件，仅 `COMMITTED` 批次参与统计 |
| `decisions` | 门禁裁决、门槛、实测值、迁移前后状态 |
| `intents` | 所有领域写操作的意图与恢复状态 |
| `recovery_reports` | 重启后恢复扫描事实 |

SQLite 使用 WAL、`synchronous=FULL`、`foreign_keys=ON` 和 busy timeout。正式查询总是读取单行 `stats_versions.snapshot_json`，因此不会出现分位已更新而差异估计仍旧的混合版本。

## 3. 写事务协议

### 创建、启动、门禁和迁移

```text
1. 在发布单写者锁内完成只读校验
2. 独立事务写入 PENDING intent 并提交
3. 开启领域事务，重新读取发布头，验证前置条件未变化
4. 写入裁决/发布头/统计版本
5. 在同一领域事务内把 intent 标记 COMPLETED
6. commit
```

若第 3-6 步失败，领域事务回滚，intent 标记为 `FAILED`。若进程在第 2 步后、第 6 步前终止，重启扫描会看到 `PENDING`，但不会把意图误认为已提交事实。

### 观测摄入

```text
1. 校验并归一化整批观测，计算内容哈希
2. 单写者锁内检查 batch_id 幂等/冲突
3. 写入 PENDING intent
4. 独立事务写入 STAGING 批次与原始事件（对外不可见）
5. 领域事务：STAGING -> COMMITTED
6. 读取所有 COMMITTED 事件，生成下一版本完整快照
7. 插入 stats_versions，淘汰过旧版本，更新 releases.current_stats_version
8. intent COMPLETED，commit
```

崩溃点与恢复：

- 第 3 步后：无业务变化，恢复报告记录未提交意图。
- 第 4 步后：批次仍为 `STAGING`，恢复扫描标记 `ABORTED`，不会被统计读取。
- 第 5-7 步中：领域事务回滚，批次回到 `STAGING`，随后被恢复扫描中止。
- 第 8 步后：全部事实原子可见。

## 4. 门禁原子性

门禁裁决事务同时写入：

- `decisions` 行；
- `releases.state`；
- `releases.current_stage`；
- `releases.traffic_per_mille`；
- intent 完成状态。

成功门禁从 `CANARY` 或 `OBSERVING` 进入 `PROMOTING`，并把阶段和流量推进到下一阶段目标。失败或证据不足门禁进入 `OBSERVING`，保留当前阶段和流量。单事务提交避免“阶段已推进但流量仍旧”或“裁决已记录但状态未迁移”。

## 5. 状态迁移矩阵

| 当前状态 | 合法目标 |
|---|---|
| `PREPARED` | `CANARY`, `ABORTED` |
| `CANARY` | `OBSERVING`, `PROMOTING`, `ROLLED_BACK`, `ABORTED` |
| `OBSERVING` | `PROMOTING`, `ROLLED_BACK`, `ABORTED` |
| `PROMOTING` | `OBSERVING`, `ROLLED_BACK`, `ABORTED` |
| `ROLLED_BACK` | 无（终态） |
| `ABORTED` | 无（终态） |

`ROLLED_BACK` 和 `ABORTED` 会把流量设为 `0`。非法迁移在校验阶段返回 `E_INVALID_TRANSITION`，不写意图以外的业务数据，也不修改发布头。

## 6. 版本与并发

- 每次成功摄入递增一个版本，版本号从 1 开始。
- 快照包含阶段统计、累计统计、两份差异估计和两份误差契约。
- 保留最近 8 个版本；更早版本标记 `RETIRED`，查询返回 `E_VERSION_RETIRED`。
- `GET /stats?version=N` 在单个只读事务中读取发布头与快照。
- 门禁如带 `stats_version`，必须等于当前版本；否则返回 `E_VERSION_RETIRED`。
- 同一发布的所有摄入、重算、门禁和迁移由进程内单写者互斥量串行化；不同发布可并行。
- 同一 `batch_id` 并发提交时，后到者等待先到者提交，随后按内容哈希复用；内容不同返回 `E_CONFLICTING_BATCH`。

## 7. 恢复报告

启动时自动执行一次恢复扫描。扫描：

1. 读取所有 `PENDING intents`；
2. 把无对应完整提交的 `STAGING` 批次标记 `ABORTED`；
3. 检查 `current_stats_version` 是否指向完整版本，必要时回退到最后完整版本；
4. 将已检查的 pending intent 标记 `RECOVERED`；
5. 写入新的 `recovery_reports` 行。

报告字段 `claim_boundary` 明确说明：强制终止瞬间不存在退出报告；重启后报告只包含持久化数据库中可以验证的事实。

故障注入接口先提交意图，然后直接 `os.Exit(70)`，不写 HTTP 响应。示例中 curl 因空回复返回 52，进程退出码为 70；重启后报告 `pending_intents_found=1`、`intents_recovered=1`。

## 8. 适用范围

该组件是本地单进程参考实现，强调数值一致性与事务边界，不提供多节点选主、远程复制、认证授权或生产级访问控制。SQLite 单写者和进程内锁适用于本地评估；跨进程写入需外部分布式协调或改换存储后端。
