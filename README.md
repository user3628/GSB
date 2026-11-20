# Shardroute Local

面向本地多逻辑库、多分表的确定性路由组件。规则来自带版本号和 SHA-256 摘要的 JSON 快照；查询经本地 HTTP 接口提交，组件解出有序物理表计划后在本地 SQLite 文件上执行，并按分片键、主键稳定归并。所有样本、数据和服务都在本机生成，不依赖真实数据库集群、Docker 或外部网络。

## 快速开始

环境：Python 3.11+，`pytest` 可选。SQLite 使用 Python 标准库，无需单独安装客户端。

```bash
python3 -m pip install -e '.[test]'
python3 -m pytest -q
python3 scripts/demo.py
```

`scripts/demo.py` 会完成一次全量快照加载、一次只修改 `orders` 分片算法的增量提交、五类异常注入、真实强杀和重启恢复，并写出：

- `artifacts/demo-report.json`：完整路由轨迹、命中物理表、补丁缓存变化、注入结果与恢复差异。
- `artifacts/recovery-report.json`：仅由已持久化元数据和意图事实重建的恢复报告。

## 目录

- `shardroute/snapshot.py`：快照摘要、版本、映射、算法引用和规则校验。
- `shardroute/routing.py`：取模、固定种子哈希、范围、IN 与全分片路由计划。
- `shardroute/cache.py`：逻辑表派生缓存及原子版本切换。
- `shardroute/engine.py`：单写者提交、多读者绑定、幂等查询、执行与恢复。
- `shardroute/persistence.py`：SQLite 元数据、意图、快照、缓存清单与恢复报告。
- `shardroute/datastore.py`：本地 SQLite 种子初始化与只读物理表查询。
- `shardroute/merge.py`：跨表冲突裁决、稳定排序、去重和窗口化。
- `shardroute/server.py`：回环 HTTP JSON 服务。
- `samples/`：固定快照、增量补丁和确定性种子数据。
- `scripts/`：完整演示、前台启动和强制中断脚本。
- `tests/`：路由、归并、缓存、并发、HTTP 与真实强杀恢复测试。
- `docs/DESIGN.md`：一致性、并发与恢复设计。
- `docs/VERIFICATION.md`：本次实际验证结果。

## 快照模型

快照摘要使用去掉顶层 `checksum` 后的规范化 JSON（键排序、无多余空格、UTF-8）计算 SHA-256。加载时先检查摘要和结构，再检查版本单调性；完整快照版本必须严格大于当前版本。

核心结构：

```json
{
  "schema_version": 1,
  "snapshot_version": 1,
  "generated_at": "2026-11-21T00:00:00Z",
  "shard_key": "user_id",
  "shard_key_type": "integer",
  "algorithm": "orders_mod4",
  "conflict_rule": "physical_order_first",
  "algorithms": {
    "orders_mod4": {"type": "mod", "modulus": 4},
    "events_hash4": {"type": "hash", "modulus": 4, "seed": 20261121}
  },
  "tables": {
    "orders": {
      "logical_db": "commerce_db",
      "algorithm": "orders_mod4",
      "physical_tables": [
        {"shard": 0, "table": "orders_0000", "primary_key": "id"},
        {"shard": 1, "table": "orders_0001", "primary_key": "id"}
      ]
    },
    "orders_view": {"route_ref": "orders"}
  },
  "checksum": "<sha256>"
}
```

算法：

- `mod`：整数键 `value % modulus`，负数也得到非负余数。
- `hash`：`sha256(seed + NUL + key_type + NUL + normalized_value)` 的前 8 字节取大端整数后 `% modulus`；`seed` 写入快照，跨进程固定。
- `range`：区间默认 `[lower, upper)`，可用 `lower_inclusive` / `upper_inclusive` 改写；`null` 表示无界。等值查询要求键落在某个区间，范围查询选择所有与查询区间相交的物理表，空查询区间返回空计划。
- 算法名可引用其他命名算法；逻辑表可用 `route_ref` 继承另一逻辑表映射。算法引用环和表映射引用环均返回 `E_ROUTE_CYCLE`。

冲突与映射规则：

- 同一逻辑表重复映射同一物理表、重复分片号、物理分片集合与算法不一致、范围区间重叠，均返回 `E_ROUTE_CONFLICT` 且不提交候选快照。
- 空物理表列表返回 `E_EMPTY_PHYSICAL_MAPPING`。
- 结果冲突规则支持 `physical_order_first`、`physical_order_last`、`max_updated_at_physical_order`；同一主键最终只输出一条记录。

## HTTP 接口

服务默认只监听 `127.0.0.1`。所有业务错误统一返回：

```json
{
  "ok": false,
  "request_id": "req-1",
  "error": {
    "code": "E_SHARD_KEY_INVALID",
    "message": "shard key must be an integer",
    "context": {"shard_key_type": "integer", "value": "bad"}
  }
}
```

主要端点：

- `GET /health`、`GET /state`、`GET /cache`、`GET /snapshots`、`GET /snapshots/{version}`。
- `GET /recovery/report`：最近一次启动恢复的持久化事实报告。
- `POST /snapshot`：提交完整快照；请求体可直接是快照，也可为 `{"snapshot": ...}`。
- `POST /snapshot/patch`：提交只包含受影响逻辑表的补丁。
- `POST /query`：执行路由查询。
- `POST /injections`：五类确定性异常注入的便捷入口。
- `POST /shutdown`：优雅停止并写入干净退出事实。

查询示例：

```json
{
  "request_id": "req-1",
  "logical_table": "orders",
  "query_type": "eq",
  "shard_key_value": 7,
  "routing_version": 2,
  "limit": 20,
  "offset": 0
}
```

`query_type` 为 `eq`、`in`、`range`、`scan`。IN 使用 `shard_key_values`，范围使用：

```json
{"lower": 10, "upper": 100, "lower_inclusive": true, "upper_inclusive": false}
```

响应包含 `physical_tables`、`route_trace`、`plan_checksum`、按稳定顺序归并后的 `rows`、逐行来源 `row_sources`、冲突明细和 `page`。当查询开始后快照被替换时，响应中的 `routing_version` 仍是请求绑定的旧版本，并标记 `version_replaced_during_request=true`；旧版本不会混入新映射。

同一 `request_id` 与相同载荷只执行一次；并发重复请求等待并复用结果，进程重启后也可从 `query_batches` 重放成功结果或同一错误。相同 ID 搭配不同载荷返回 `E_REQUEST_CONFLICT`。

## 增量与缓存

补丁体只包含 `tables` 中受影响的逻辑表，并携带 `base_version` 和目标版本。`base_version` 不等于当前版本立即返回 `E_STALE_BASE`，不写意图、不改缓存、不换快照。通过继承关系受影响的逻辑表会被语义比较发现并级联重建。

每个逻辑表缓存包含物理表清单、范围边界、算法签名、`source_version` 与生效版本。提交时先持久化快照意图和缓存重建意图，再构建新状态，并在同一元数据事务中写入快照、活动版本、缓存清单并完成意图，最后一次性替换内存中的不可变 `RouteState`。读者只会看到旧状态或完整新状态。

## 恢复模型

SQLite 元数据库持久化：

- `snapshots` 与单行 `active_state`：完整提交边界。
- `intents`：快照提交、补丁、缓存重建和查询批次的 `PREPARED/RUNNING/COMMITTED/FAILED/ABORTED` 事实。
- `query_batches`：幂等结果或错误、请求载荷摘要、路由版本与状态。
- `service_runs`：进程启动与优雅退出事实。
- `recovery_reports`：启动时根据上述事实生成的报告。

强杀后的下一次启动会扫描未完成意图：查询批次标记为 `ABORTED`，快照/补丁意图标记为中止；活动路由选择最后一个完整且摘要有效的快照，重建派生缓存并写入恢复报告。报告明确包含 `persisted_facts_only=true`，强杀场景下 `complete_exit_report_available=false`，不会虚构退出报告。

## 业务码

| 业务码 | 含义 |
| --- | --- |
| `E_SNAPSHOT_INVALID` | 摘要、结构、算法或快照内容非法 |
| `E_VERSION_ROLLBACK` | 完整快照或补丁目标版本回退 |
| `E_STALE_BASE` | 补丁基础版本已过期 |
| `E_VERSION_RETIRED` | 查询开始时请求版本已不是当前版本 |
| `E_ROUTE_CYCLE` | 逻辑表或算法引用成环 |
| `E_ROUTE_CONFLICT` | 重复映射、分片映射不一致或范围重叠 |
| `E_EMPTY_PHYSICAL_MAPPING` | 逻辑表没有物理表 |
| `E_SHARD_KEY_INVALID` | 分片键缺失、类型错误或不在范围 |
| `E_QUERY_NOT_SUPPORTED` | 查询类型或算法组合不支持 |
| `E_EXECUTION_ABORTED` | 注入中断或重启时发现未完成查询批次 |
| `E_EXECUTION_FAILED` | 本地 SQLite 执行失败 |
| `E_REQUEST_CONFLICT` | 同一请求 ID 对应不同载荷 |

## 测试与演示

```bash
# 全量测试；其中一个测试会真实启动子进程并以退出码 86 强杀
python3 -m pytest -q

# 完整可重复演示
python3 scripts/demo.py

# 手动演示：前台启动（首次初始化数据并加载样本快照）
python3 scripts/start_demo_server.py --work-dir artifacts/manual-demo --init-data --load-sample-snapshot

# 另一个终端：真实中断服务进程
python3 scripts/crash_query.py --base-url http://127.0.0.1:<端口>

# 重新启动同一元数据库后查看恢复事实
python3 scripts/start_demo_server.py --work-dir artifacts/manual-demo
```

Windows 上使用同样的 `python` 命令即可；强制终止由进程内确定性故障点 `os._exit(86)` 模拟，脚本不依赖 Unix 信号或 Docker。

## 设计约束

- 该交付是单服务进程的本地路由组件；同一元数据库不应同时启动多个写服务进程。
- 物理表只读查询，不实现跨库写事务或在线数据重分布。快照改变分片算法后，目标物理表是否存在由测试数据或上层数据迁移负责。
- HTTP 服务面向本机集成，不包含鉴权、TLS 和公网暴露能力。

更完整的提交时序、并发不变量和故障点说明见 `docs/DESIGN.md`，实际验证结果见 `docs/VERIFICATION.md`。

