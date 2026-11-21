# 技术设计

## 1. 模块划分

- `gateway/domain.py`：路由/请求 Pydantic 模型、摘要、状态机、字段规范化。
- `gateway/store.py`：SQLite schema、事务、意图日志、状态迁移、轮次、恢复、验证快照。
- `gateway/scheduler.py`：确定性排序、预算/容量裁决、租户轮转、派发与工作线程。
- `gateway/backend_client.py`：基于 `http.client` 的独立短连接、读超时、取消和连接注册表。
- `gateway/service.py`：启动恢复、关闭与存储生命周期。
- `gateway/api.py`：FastAPI 路由、统一业务错误、生命周期。
- `gateway/validation.py`：持久化事实闭环检查与 CLI。
- `scripts/mock_backend.py`：确定性延迟、显存语义、success/fail/hang 注入。
- `e2e/run_e2e.py`：真实进程、两轮轨迹、强杀恢复、结果落盘。

## 2. SQLite 表

| 表 | 关键事实 |
|---|---|
| `routes` | 版本、原始摘要、规范化载荷、原始载荷、提交时间 |
| `backend_runtime` | 每个 `(route_version, backend_id)` 的并发/显存上限、在途、预留、熔断统计 |
| `requests` | 请求标识、幂等摘要、绑定规格、状态、终态、恢复次数 |
| `request_transitions` | 只追加的状态迁移链 |
| `tenant_usage` | 租户历史派发、SHED、失败和估算预算消耗 |
| `rounds` | 每轮预算、决策耗时、汇总和完整轨迹 |
| `intents` | 派发/释放两阶段操作意图 |
| `admission_rejections` | 队满/严格容量拒绝事实，可附着到下一轮轨迹 |
| `recovery_reports` | 每次启动的持久化恢复事实 |

数据库连接启用 WAL、`synchronous=FULL`、外键和 10 秒 busy timeout。进程内使用 `RLock` 串行化单连接访问，读取也使用显式事务，避免多个 SELECT 看到不同提交点。

## 3. 派发/释放事务协议

派发：

1. 在决策锁内读取请求，预检状态、熔断、时间预算、全局并发、后端并发和显存。
2. 事务 A 写入 `DISPATCH` OPEN 意图并提交。
3. 事务 B 重新检查同一事实，然后原子更新：
   - `requests.status QUEUED -> DISPATCHED`
   - `vram_reserved_mb`
   - `backend_runtime.in_flight + 1`
   - `backend_runtime.vram_reserved_mb + demand`
   - 迁移记录
   - 租户使用量
   - 意图关闭
4. 提交后启动 HTTP 工作线程。若在第 3 步后、第 4 步前崩溃，重启将观察到无主 `DISPATCHED` 并重排。

释放：

1. 事务 A 写入 `RELEASE` OPEN 意图。
2. 事务 B 仅在请求仍为 `DISPATCHED` 时原子更新终态、减在途、减预留、失败/超时统计、迁移记录与意图关闭。
3. 条件更新保证重复完成不会二次扣减。

轮次决策锁覆盖整轮选择过程，避免工作线程在轮中完成导致同一输入产生不同派发结果。HTTP 等待在锁外进行。

## 4. 确定性调度

候选集合按以下层级排序：

1. `priority DESC`。
2. 同优先级内，将各租户队列按“历史派发次数 + 本轮已派发次数、历史与本轮已消耗估算预算、上次派发序号、队首提交序号、租户标识”排序。
3. 每次从每个租户取一个队首请求形成轮转，再进入下一轮。

预算以路由/模型 `estimated_latency_ms` 累加。实际后端延迟与超时由独立工作线程处理，不阻塞其它派发。容量判定也在事务 B 中重复执行，确保轮次决策锁之外的取消/查询不会破坏上限。

## 5. 超时与取消

`http.client.HTTPConnection` 使用 `latency_cap_ms` 作为 socket 超时，并额外启动独立定时器作为请求总时限；定时器到期直接 shutdown/close 连接，避免慢速滴流响应绕过单次读超时。连接在调用开始时注册到 `ActiveCallRegistry`。取消接口：

1. 若请求为 PENDING/QUEUED，直接事务迁移为 FAILED，不占资源。
2. 若为 DISPATCHED，设置 `cancel_requested`，对底层 socket 执行 `shutdown` 与 `close`。
3. 工作线程观察到连接异常或取消标记后，通过正常释放事务回收并发与显存。

这种方式确实关闭网关到后端的 HTTP 连接，但不能、也不声称能杀死后端进程中已启动的执行逻辑。挂起后端不会占用网关工作队列中的并发名额，也不会阻塞后续轮次。

## 6. 熔断

每次释放更新后端 `consecutive_failures`、`total_failures`、`timeout_count`。连续失败达到 `failure_threshold` 时状态变为 `DEGRADED`。准入直接返回 `E_BACKEND_DEGRADED`；已排队请求在轮次内迁移 FAILED。恢复健康需显式管理接口且要求 `in_flight=0`，避免静默恢复。

## 7. 恢复算法

启动时在单事务快照中读取：

- 所有 OPEN 意图；
- 所有 `DISPATCHED` 请求；
- 所有 `RUNNING` 轮次；
- 所有后端运行记录。

对每个 `DISPATCHED` 请求：

- 路由版本与后端记录尚存在；
- 非取消状态；
- `recovery_count < max_recovery_attempts`；
- 配置允许重新入队；

则释放持久化预留、`DISPATCHED -> QUEUED`、清空轮次派发字段并增加恢复次数。否则永久 `ABORTED`。OPEN 意图标记 ABORTED，运行中轮次标记 INTERRUPTED。所有后端计数按仍为 `DISPATCHED` 的持久化请求重新聚合。

恢复报告只包含上述事实，不包含被杀死进程的退出状态、未提交内存数据或未落盘结果。

## 8. 验证器

验证器只读 SQLite，不调用调度器内存状态。核心检查是跨表不变量：

- 摘要：原始 JSON 可重算摘要。
- 状态链：首迁移 `NULL -> PENDING`，中间 `from == 前一 to`，最后状态等于请求状态。
- 资源闭合：后端计数等于在途请求聚合，非在途请求预留为零。
- 上限：全局/后端并发、显存预算。
- 绑定：请求端点等于其路由版本中的后端端点，显存需求可由样本数重算。
- 轮次：估算预算、轨迹唯一性、轨迹与持久化派发迁移的一一对应。
- 意图/恢复：稳定快照无 OPEN 意图，无无主 DISPATCHED。

该工具能抓出人为修改后端计数、断开迁移链、预算越界、轨迹与实际状态不一致等问题。

## 9. Windows 注意事项

- 所有路径使用 `pathlib`；SQLite 使用标准库。
- 端口只绑定 `127.0.0.1`。
- E2E 在 Windows 使用 `subprocess.CREATE_NEW_PROCESS_GROUP`，强杀调用 `Popen.kill()`；POSIX 使用 `SIGKILL`。
- 后端仍执行挂起任务时，通过关闭 TCP 连接和网关资源释放保证验收不依赖后端进程自行取消。
- 测试中的 `uvicorn` 服务运行在后台线程，结束后设置 `should_exit` 并带短优雅超时。
- 网关只支持单进程所有权；不要对同一 SQLite 文件启动多个 uvicorn worker，否则启动恢复会相互竞争。

