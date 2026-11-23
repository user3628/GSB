# 设计说明

## 1. SQLite 数据模型

| 表 | 作用 |
| --- | --- |
| `key_state` | 每个对象键当前最大上传代际与活跃会话槽位 |
| `sessions` | 会话键、代际、下界、TTL、状态和完成结果 |
| `parts` | 会话内分片元数据与原始内容，主键 `(session_id, part_number)` |
| `objects` | 当前对象键的成功合并元数据 |
| `intents` | 先写意图日志，`seq` 为自增日志序号 |
| `requests` | 跨重启的请求规范内容指纹与完整响应缓存 |
| `recovery_reports` | 每次恢复生成的不可变快照 JSON |

关键约束：

- `ux_sessions_one_active_per_key` 是只覆盖 `status='active'` 的部分唯一索引，数据库层保证同键单活跃会话。
- `parts` 以会话和序号为主键，重复提交不能产生第二行。
- `sessions(object_key, generation)` 唯一，代际由键级写事务递增。
- 所有写事务使用 `BEGIN IMMEDIATE`，配合 WAL 与 `busy_timeout`，将 SQLite 的单写者约束显式化。

## 2. 写路径与日志顺序

创建、提交分片、取消、合并都执行两段式写：

```text
T1: INSERT intents(... status='pending') COMMIT
T2: BEGIN IMMEDIATE
    验证并修改 sessions/parts/objects/key_state
    UPDATE intents SET status='committed'
    COMMIT
```

业务校验失败时，引擎把对应意图标记为 `rejected`。崩溃发生在 T1 与 T2 之间时只留下 `pending` 意图，不会修改状态表；恢复将其标记为 `abandoned`。数据事务自身原子，因此任何查询和恢复只能看到完整版本。

## 3. 会话代际与同键裁决

创建会话在对象键锁和 `BEGIN IMMEDIATE` 事务中完成：

1. 若存在未过期活跃会话，返回 `E_SESSION_CONFLICT` 和现有会话标识；
2. 若活跃会话已过期，先写过期意图、作废分片并释放槽位；
3. 读取并递增 `key_state.generation`，插入新会话；
4. 更新对象键活跃会话槽位。

取消与 TTL 回收都会在同一事务内把该会话 `active` 分片改为 `void`，并仅在槽位仍指向该会话时清空槽位，避免误释放更新代际会话。

## 4. 分片幂等与摘要

HTTP 层先把 `content_base64` 解码为原始字节，引擎计算实际 SHA-256：

- 声明大小必须等于内容长度；
- 声明摘要必须等于内容实际摘要；
- 存储按 `(session_id, part_number)` 查找首次记录；
- 摘要相同视为幂等，增加 `duplicate_count`；
- 摘要不同返回 `E_PART_MISMATCH`，不更新首次记录。

合并时所有有效分片按序号升序读取，对象 SHA-256 是对拼接后的原始字节做摘要。

## 5. 合并复核调度

复核器维护进程级：

- `concurrency` 个运行槽位；
- `queueCapacity` 个等待槽位；
- 每个任务带本轮截止时刻。

提交顺序按分片序号升序。有空槽时立即启动，否则进入 FIFO 等待队列；队列满返回 `E_VERIFY_QUEUE_FULL`。任务真正启动时若本轮预算已过期，则该任务直接标记为 skipped。预算在提交阶段耗尽时，剩余连续后缀也标记 skipped。报告中的 verified/skipped 列表排序输出。

任何实际启动并失败的复核都会阻止合并；跳过只发生在预算约束下，并在响应中可见。

## 6. 合并事务

摘要复核完成后，合并在一个 `BEGIN IMMEDIATE` 事务内再次落盘校验：

1. 会话必须仍为 `active` 且未过期；
2. 分片从 `lower_bound` 开始连续无缺口；
3. 请求的可选 `expected_part_count` 与实际数量一致；
4. 每个记录大小等于存储内容长度；
5. 总大小等于各分片大小之和并满足可选 `expected_total_size`；
6. 当前对象代际不得高于会话代际；
7. UPSERT `objects`；
8. 同一事务将会话更新为 `completed`、写入对象摘要/大小/分片数并释放活跃槽位。

已完成会话再次合并直接返回已持久化结果，不再写对象表。会话完成与对象元数据写入同事务，避免查询看到半完成状态。

## 7. 请求幂等

HTTP 层将解码后的逻辑请求（含路径中的会话与分片序号）规范化为 JSON，计算包含操作名的 SHA-256 指纹：

- 进程内同 `request_id` 的并发请求只有一个执行，其余等待；
- 不同规范内容的并发请求立即返回 `E_CONFLICTING_REQUEST`；
- 执行前在 `requests` 表登记 `processing`；
- 完成后持久化状态码、响应正文和业务码；
- 重启后相同标识/指纹直接复用已持久化响应；
- 恢复把遗留 `processing` 标记为 `interrupted`，后续可安全重试。

底层操作本身也具备重试安全性：创建使用 `creation_request_id` 找回已建会话，分片按序号和摘要幂等，合并对已完成会话复用结果，取消对已取消/过期会话幂等。

## 8. 恢复与一致性

恢复在一个写事务中完成：

1. 收集并标记 `pending` 意图为 `abandoned`；
2. 将遗留 `processing` 请求标记为 `interrupted`；
3. 写过期意图并回收所有 TTL 到期活跃会话；
4. 全量读取会话、分片、对象与意图状态；
5. 计算一致性检查；
6. 写入报告 JSON。

报告中的会话、分片和对象都是从已提交表读取的事实，`interrupted_intents` 明确标注未应用。恢复不会把未提交意图推断成新对象，因此结果与最后一次完整提交状态一致。

## 9. 并发模型

- 存储层用互斥写锁和 `BEGIN IMMEDIATE` 串行化所有 SQLite 写事务。
- 引擎对会话 ID 使用细粒度单写者锁，对对象键创建使用键级锁。
- 恢复入口持有服务级排他闸门，普通变更操作持有共享闸门，避免恢复把正在两段式提交中的意图误判为中断。
- 查询使用独立连接和 SQLite WAL 快照，可并发执行；单条查询或多查询读事务只能看到已提交版本。
- 复核调度器的运行槽位和等待队列是进程级全局状态。

目标部署模型是每个 SQLite 文件对应一个本地服务进程。数据库唯一约束仍能防止跨进程创建同键双活跃会话；请求幂等的进程内等待结合持久响应缓存覆盖常规重启和并发重试场景。

