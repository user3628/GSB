# 架构与一致性设计

## 模块

| 模块 | 职责 |
| --- | --- |
| `slowquery.validation` | 样本/快照归一化、范围与摘要校验 |
| `slowquery.sql_analysis` | 单语句只读校验、轻量 SELECT 解析、选择率估算 |
| `slowquery.planner` | 慢查询判定、慢因与规则改写 |
| `slowquery.verification` | 临时副本、SQLite 执行、计划代价、行集合摘要、资源释放 |
| `slowquery.database` | WAL 连接、显式事务、表与数据库触发器 |
| `slowquery.repository` | 意图、样本、快照、会话、事件、建议、验证与恢复事实 |
| `slowquery.service` | 状态编排、幂等、单写者并发、快照绑定与启动恢复 |
| `slowquery.api` | Flask 路由、统一业务错误 JSON |

## 持久化事实

- `samples`：归一化样本与 payload SHA-256。
- `snapshots`：历史快照负载、摘要与生成时间；`snapshot_pointer` 仅一个活动指针。
- `sessions`：当前状态、绑定快照、分析结果、错误和修订号。
- `session_events`：每次状态变化及触发原因，与状态更新同事务。
- `suggestions`：规则、SQL、DDL、收益、快照版本、原因集合和验证状态。
- `verification_records`：原/改写代价、结果摘要、行数、结果和资源释放证据。
- `operation_intents`：在业务写之前提交的持久化意图。
- `recovery_reports`：只基于已提交事实生成的启动恢复报告。

## 诊断事务序列

1. 在单写者锁内读取 `CREATED` 会话，选择活动快照；旧版本存在但非活动时返回 `E_SNAPSHOT_RETIRED`。
2. 提交 `ANALYZE` 意图（独立事务）。
3. `CREATED -> ANALYZING`，同时写入绑定快照和意图检查点。
4. 在内存计算慢因与建议，不写数据库。
5. 单个事务中依次：插入建议、`ANALYZING -> SUGGESTED`、写分析 JSON、把意图标为完成。
6. 任一失败则回滚；已进入 `ANALYZING` 时会话转 `FAILED`，否则保持 `CREATED`。

数据库触发器在 `SUGGESTED` 更新时验证至少存在一条建议，因此读者不可能看到“已 SUGGESTED 但建议未提交”的持久化状态。

## 验证事务序列

1. 仅允许 `SUGGESTED`；选择持久化建议并确认其快照版本等于会话绑定版本。
2. 提交 `VERIFY` 意图，`SUGGESTED -> ANALYZING`。
3. 在会话锁和单写者锁内，复制样本库并执行原查询、DDL 与改写。
4. 成功事务：插入 `VERIFIED` 验证记录、`ANALYZING -> VERIFIED`、建议状态 `VERIFIED`、意图完成。
5. 不一致或执行错误事务：插入 `FAILED` 记录、建议 `FAILED`、`ANALYZING -> FAILED`、意图完成。
6. `ANALYZING -> VERIFIED` 的数据库触发器要求同事务已存在成功验证记录。

## 强杀窗口

- `ANALYZE` 意图已提交、业务迁移未提交：会话仍为 `CREATED`；恢复只回收意图。
- 已提交 `ANALYZING`、建议事务未提交：会话为 `ANALYZING`；恢复转 `CREATED`，清空快照绑定和分析。
- `VERIFY` 意图已提交、验证迁移未提交或验证执行中：恢复转 `FAILED`，不生成成功记录。
- 验证结果事务提交前强杀：结果只存在内存中，恢复不可见。
- 验证成功事务提交后强杀：验证记录、会话 `VERIFIED`、建议状态与意图完成已原子落盘。

## 恢复报告

启动时对每个 `PENDING` 意图执行一次恢复事务，然后重新查询事实生成报告。报告包含：

- 扫描到的意图数与每个意图的上一状态、最终状态、动作、证据。
- 当前会话、状态事件、意图、验证记录。
- `suggested_without_suggestions`、`verified_without_verification_record`、`pending_intents_remaining`。
- `interrupted_verification_marked_verified` 必须为 `false`。

报告不使用内存中的计划、代价或验证结果。

## 并发模型

- 每个会话有独立锁：同一会话诊断/验证按提交顺序串行，重复诊断等待后复用结果。
- 进程级写锁串行化所有写事务；SQLite `BEGIN IMMEDIATE` 作为第二层保护。
- 只读 API 使用独立只读 URI 连接和 `BEGIN` 事务；WAL 允许它们在写事务未提交时读取旧的一致快照。
- 会话、建议、验证记录在单个读事务中组合，避免跨提交读取造成组合撕裂。

## 快照版本

- 版本号与生成时间均严格递增。
- 新快照在一个事务中插入历史表并切换活动指针。
- 诊断新会话只读取活动版本；历史版本只供已绑定会话的审计和验证使用。
- 不删除历史版本，避免重新解释旧会话；退休只表示“不再是活动版本”。
