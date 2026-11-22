# 本地图遍历与索引组件 — 技术说明

## 1. 模块结构

```
graphstore/
  errors.py      统一业务错误码（5 个必需码 + 基类）
  models.py      Node / Edge 不可变数据模型与 JSON 解析
  snapshot.py    快照解析、SHA-256 摘要、版本单调校验
  indexes.py     标签倒排索引、(边类型,源) 邻接索引、增量应用
  storage.py     SQLite 表结构、意图日志、版本物化、缓存与去重持久化
  recovery.py    启动恢复：扫描 PENDING 意图并回滚/确认
  view.py        不可变 SnapshotView、版本注册表与引用计数
  dedup.py       同 request_id 的单执行/等待复用/冲突检测
  query.py       多跳 BFS、Dijkstra 最短路径、模式匹配、快速/精确双模式
  diff.py        双模式结果逐项差异核对
  engine.py      单写者更新、多读者查询、事务编排与崩溃钩子
  analytics.py   NumPy 只读统计
  api.py         127.0.0.1 线程化 HTTP API
samples/         确定性样本（v1、v2、坏摘要、坏权重、查询请求）
tests/           39 个 pytest 用例
scripts/         样本生成、端到端演示（含强制终止恢复）
```

## 2. 快照与摘要

* 摘要对“按 id 排序的节点 + 按 (source,type,target) 排序的边”做规范化
  JSON 后取 SHA-256，与时间戳无关，环境间可复现。
* 加载顺序：纯函数解析校验 → 构造索引 → 写意图 → 单事务物化 → 原子发布视图。
  校验失败在写库之前抛出，旧视图与旧数据完全不受影响，不存在部分加载。
* 版本号必须为正整数且严格大于当前版本，否则 `E_SNAPSHOT_INVALID`。

## 3. 索引模型与增量更新

* 标签倒排：`label -> set(node_id)`；邻接：`(edge_type, source) -> {target: weight}`。
* `IndexBundle.apply_delta` 只复制并修改受影响的标签/邻接键，删除后立即清理
  空条目，并返回受影响依赖键集合；随后用全量重建结果做一次内部断言，防止
  增量逻辑与全量结果漂移（漂移即 `E_GRAPH_INCONSISTENT`）。
* 删除节点会自动级联删除其全部入边/出边并清理对应邻接键。

## 4. 查询与双模式

* 多跳：分层 BFS，每层结果按节点 id 排序、全局去重。
* 最短路径：确定性 Dijkstra（堆项按 `(距离, id)` 排序，保证同距下路径稳定）；
  查询开始先全量校验权重，遍历中再做防御性校验，零/负权重返回
  `E_WEIGHT_INVALID` 并附带问题边。
* 模式匹配：按有序边类型规格做 DFS，可约束目标标签，路径按节点序列排序。
* 精确模式不设截断；快速模式受 `node_budget`（访问节点上限）与
  `prefetch_budget`（索引预取上限）约束，返回 `truncated_branches` 与实际
  预算消耗。两种模式共享同一展开顺序，快速结果是精确结果的严格子集，
  `/query/audit` 在同一快照上分别执行并输出 `missing_in_fast`、
  `unexpected_in_fast`（恒为空）与可界定偏差，实现逐项可核对。

## 5. 版本绑定、缓存与级联失效

* 查询通过 `ViewRegistry.acquire(version)` 拿到不可变视图并持有到查询结束；
* 新版本只替换注册表当前视图，在途查询继续引用旧视图完成，绝不混用新旧索引。
* 缓存键为 `(版本, 查询签名)`，签名由查询类型与参数的规范化 JSON 哈希得到；
  每个缓存条目记录其实际读取的依赖键（`label:*`、`adj:*`）。
* 增量更新在同一 SQLite 事务内：删除命中受影响依赖键的旧缓存 → 继承未受影响
  缓存 → 写入新图数据/索引 → 写提交标记 → 意图置 COMPLETE。读者只能看到
  事务提交后的完整状态，不存在“图已更新而缓存仍旧”的中间可见状态。

## 6. 并发模型

* 写路径持全局写锁（单写者串行，版本号连续）；读路径无写锁，多读者并发。
* 同 `request_id`：内存中并发请求由 `RequestDeduplicator` 保证只执行一次，
  其余等待并复用；结果同时落盘 `request_runs`，进程重启后的重放直接复用；
  请求体哈希不一致返回 `E_CONFLICTING_REQUEST`。

## 7. 意图日志与强制终止恢复

每个写操作先在独立事务写入 `PENDING` 意图并提交，再在单事务内完成数据/索引/
缓存写入并把意图置 `COMPLETE`。重启时 `recovery.recover` 仅依据持久化事实：

| 意图状态 | 提交标记是否存在 | 判定 |
 --- | --- | ---
| PENDING | 存在 | 标记 COMPLETE（数据已完整落盘） |
| PENDING | 不存在 | 删除该版本残留行并标记 ABORTED，回到最后完整版本 |

恢复报告包含意图 id、类型、版本、判定依据与删除行数，全部可在库中复核。

## 8. 错误码

| 码 | 触发场景 |
| --- | --- |
| `E_SNAPSHOT_INVALID` | 摘要不匹配、版本非单调、结构非法、引用悬空 |
| `E_WEIGHT_INVALID` | 最短路径遇到零/负权重 |
| `E_GRAPH_INCONSISTENT` | 节点/边缺失、索引引用悬空、增量与全量重建不一致 |
| `E_VERSION_RETIRED` | 请求绑定版本不存在/已退役或无任何快照 |
| `E_CONFLICTING_REQUEST` | 同 request_id 提交内容不一致 |

## 9. 验证结果

* `python -m pytest -q`：39 用例全部通过，覆盖摘要校验、增量索引、双模式
  子集性、最短路径权重校验、模式匹配、版本绑定、级联失效、单写多读、请求
  去重与强制终止恢复（子进程 `os._exit(137)` 后重启）。
* `python -m scripts.demo_e2e`：HTTP 端到端 11 个场景全部成功，恢复报告显示
  未完成意图被 ABORTED、最后完整版本保持可读且完整性校验通过。
