# GPU 显存配额调度与批推理裁决组件 — 技术说明

## 1. 目标与范围

单机 Linux 上的 GPU 显存/算力配额调度与批推理裁决服务。运维通过本地 HTTP 接口编辑逻辑
GPU 节点、推理请求与策略快照，触发调度轮次，查询配额占用和批组织结论。核心目标是在高
频只读查询与串行写裁决之间保证**跨表、同事务的一致性**，并在进程被强制终止后可恢复到最
后一个完整提交版本。

不使用容器；SQLite 采用纯 Go 驱动 `modernc.org/sqlite`（无 CGO 依赖），数据通过固定种子
在本地确定性生成，不访问任何外部服务。

## 2. 模块结构

```
cmd/gpusched          CLI: generate / serve / recover
internal/domain       量纲解析、类型、状态机、校验归一化、摘要
internal/gen          固定种子(xorshift64*)确定性样本生成器
internal/store        SQLite schema、事务、意图/轮次/批次/请求访问器
internal/scheduler    确定性延迟估算、批组织算法
internal/service      单写者裁决、准入、抢占、轮次、状态迁移、恢复、查询
internal/httpapi      HTTP 路由、统一响应信封与错误码映射
scripts               本地客户端与强制终止恢复脚本
samples/sample.json   固定种子产出的确定性样本
```

## 3. 关键设计

### 3.1 量纲解析与整批校验（需求 1）

- 显存：`Ki/Mi/Gi/Ti` 按 1024 进制转为字节整数，支持裸字节；拒绝负数与 int64 溢出。
- 算力：`250m` 即 250 千分比；`1` 视为 1000m；`0.25` 转为 250m，小数精度超过 0.001 拒绝。
- `domain.Normalize` 对整份规格做一次性校验：重复请求/节点/组 ID、未知配额组、非法量纲、
  非单调快照版本、摘要断链、组容量超过成员节点容量等，统一返回 `E_SPEC_INVALID`，调用方
  整批不入库（Load 在单事务内写入，任一错误整体回滚）。
- 策略快照携带单调 `version`、链式 `prev_digest` 与 sha256 摘要；增量片段只含受影响字段，
  摘要基于 `base_digest` 与规范化（键排序）后的字段计算，保证跨进程可重复。

### 3.2 配额准入与同事务记账（需求 2）

组占用 = 组内成员节点已占用（显存/算力）+ 本组已准入请求预留，二者在数据模型上保持恒等：

```
quota_groups.memory_reserved  ==  Σ nodes.memory_used      (按组)
quota_groups.compute_reserved ==  Σ nodes.compute_used_m   (按组)
```

`AdmitOne` 在单个事务内顺序执行：写 `ADMIT` 意图 -> 读组/节点 -> 组容量门限 -> 选健康且
有余量的成员节点 -> `PENDING->ADMITTED` 守卫更新 -> 节点占用递增 -> 组预留递增 -> 提交前
不变量断言 -> 标记意图 COMMITTED。超额返回 `E_QUOTA_EXCEEDED`，details 内含 current/limit/
requested/shortfall；事务回滚保证不会出现“状态改了但预留没变”。

### 3.3 请求状态机（需求 3）

合法迁移（`internal/domain/statemachine.go`）：

```
PENDING   -> ADMITTED | REJECTED | PREEMPTED
ADMITTED  -> BATCHED  | REJECTED | PREEMPTED
BATCHED   -> RUNNING  | ADMITTED(降级/拆批) | ABORTED(恢复)
RUNNING   -> COMPLETED | PREEMPTED
PREEMPTED -> PENDING(重新入队)
REJECTED / COMPLETED 为终态
```

持久化更新使用 `UPDATE ... WHERE id=? AND state=?` 守卫，非法迁移影响 0 行并返回
`E_INVALID_TRANSITION`，已落盘状态不变。重复同一迁移是幂等 no-op（`applied=false`），
同一请求同版本下只生效一次。

### 3.4 批组织（需求 4）

`scheduler.Organize` 按提交顺序（seq）扫描已准入请求，仅把**同模型、同节点、相邻且同优先级**
的请求装入同一批；优先级是硬分隔，绝不混批。每个候选批检查：

- 合计显存 ≤ 节点剩余显存；
- 合计样本数 ≤ 模型单批样本上限；
- 估计延迟 `base + per_sample*samples` 不超过成员延迟上限，否则在该请求处拆分重排。

拆分不重排元素，因此保留原始提交次序；单个请求自身即超延迟上限时无法再拆，记录到
`latency_violations` 而非静默丢弃。批次输出成员、样本数、合计显存与估计延迟。

### 3.5 并行调度器：并发上限 + 时间预算（需求 5）

`RunRound` 在一个写事务内完成整轮：批按 `优先级 -> 配额组轮转 -> 估计延迟 -> 稳定 ID`
排序后依次裁决。受策略 `global_max_concurrent_batches` 限制，超额批次进入容量为
`wait_queue_capacity` 的有界等待队列；队列满时溢出批次落为 DOWNGRADED 并在提交后返回
`E_EXEC_QUEUE_FULL`（已运行/排队的批次仍正常提交，不回滚、不丢请求）。时间预算耗尽时按
稳定次序产出确定性降级清单，被降级批内请求保持 ADMITTED，下一轮可重试。支持虚拟时钟
（`virtual_budget_consumed_ms`）使预算行为完全可复现。

### 3.6 抢占与真实配额回收（需求 6）

`Preempt` 在单事务内：必要时先解绑批次成员 -> 守卫迁移到 PREEMPTED -> 节点占用与组预留
**同时**按请求量负向归还 -> 提交前不变量断言。若状态变更无法与配额释放配对，事务以
`E_QUOTA_LEAK` 回滚。`ChoosePreemptionVictims` 以“低优先级优先、同优先级按提交序”稳定选择
受害者。回收后的组占用可被下一次准入立即使用（已在端到端验证）。

### 3.7 单写者/多读者与幂等（需求 7）

- 所有变更方法持 `writeMu` 串行提交；只读查询不加写锁，依赖 SQLite WAL 的快照隔离，读者总
  是看到某个完整提交版本，不会观察到“已 ADMITTED 但预留未计入”或“已 BATCHED 但节点余量
  未扣减”的中间态。
- 准入通过 `doOnce(key, payload)` 折叠并发同 ID 裁决：首个执行，其余等待复用结果；payload
  不一致返回 `E_CONFLICTING_REQUEST`。

### 3.8 意图日志与崩溃恢复（需求 8）

每个写事务先 INSERT 一条 PENDING 意图，再做数据变更，提交前将意图置为 COMMITTED。轮次另有
`rounds` 行（RUNNING/COMMITTED/RECOVERED）。启动时 `Recover`：

1. 扫描所有未结束轮次：未到终态的批次置 ABORTED，其 BATCHED/RUNNING 请求回退到最后完整
   状态 ADMITTED（保留准入预留），轮次标记 RECOVERED；
2. 扫描所有残留 PENDING 意图并置 ABORTED（其数据变更已由 SQLite 回滚，仅记录落盘事实）；
3. 恢复报告只包含数据库中实际存在的行，不做推测；恢复本身幂等。

SQLite 配置 `journal_mode=WAL`、`synchronous=FULL`、`foreign_keys=ON`、`_txlock=immediate`，
保证已提交事务耐久、未提交事务原子回滚。

## 4. HTTP 接口（节选）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/spec/load` | 上传整份规格（校验失败整批不入库） |
| POST | `/v1/spec/load-sample` | 装载固定种子样本 |
| POST | `/v1/requests` | 提交单个请求 |
| POST | `/v1/requests/{id}/admit` | 单请求准入 |
| POST | `/v1/requests/{id}/transition` | 状态机迁移 `{to:...}` |
| POST | `/v1/requests/{id}/preempt` | 抢占并真实回收配额 |
| POST | `/v1/scheduler/round` | 触发调度轮次 |
| GET  | `/v1/quota` | 组/节点配额占用只读视图 |
| GET  | `/v1/batches` | 批组织结论 |
| GET  | `/v1/requests` `/v1/nodes` `/v1/groups` | 只读列表 |
| GET  | `/v1/policy/latest` | 最新策略快照与摘要 |
| GET  | `/v1/recovery` | 手动触发恢复并返回事实报告 |
| POST | `/v1/debug/crash` | 在指定持久化点强制退出（恢复演练） |

错误响应统一为 `{ok:false,code,message,details}`，业务码包括 `E_SPEC_INVALID`、
`E_QUOTA_EXCEEDED`、`E_INVALID_TRANSITION`、`E_EXEC_QUEUE_FULL`、`E_QUOTA_LEAK`、
`E_CONFLICTING_REQUEST`、`E_NOT_FOUND`。

## 5. 验证方式

```
go test -race ./...                  # 全部单元/集成测试（含子进程强制终止恢复）
go vet ./...
scripts/crash_recovery.sh            # 真实进程 kill 后重启的端到端恢复核对
scripts/client.sh <action>           # 本地 HTTP 客户端
sqlite3 <db> <sql>                   # 跨表一致性人工核对
```

实际验证结果见 `docs/VERIFICATION.md`。
