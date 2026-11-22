# 本地图遍历与索引组件（graphstore）

面向本地图数据的存储、索引与查询组件：带版本号的图快照、节点标签倒排索引、
边类型邻接索引、多跳遍历 / 最短路径 / 模式匹配，支持快速与精确双模式、版本
绑定、缓存级联失效、单写多读并发，以及基于 SQLite 意图日志的强制终止恢复。
纯本地运行，不依赖外部图数据库或网络服务。

## 环境准备

- Python 3.10+（开发验证使用 3.14）
- 依赖：`pip install -r requirements.txt`（NumPy、pytest）
- SQLite 使用标准库 `sqlite3`，无需额外服务

## 生成固定样本

```bash
python -m scripts.generate_samples
```

产出 `samples/`：`snapshot_v1.json`、`snapshot_v2.json`、
`snapshot_bad_checksum.json`、`snapshot_bad_weight.json`、`query_requests.json`。

## 启动 HTTP 服务

```bash
python -m graphstore --db data/graphstore.db --port 8088 \
  --snapshot samples/snapshot_v1.json
```

仅监听 `127.0.0.1`。主要接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` `/version` `/recovery` | 健康检查、版本信息、恢复报告 |
| POST | `/snapshots/load` | 加载带摘要的快照文件 `{"path": "..."}` |
| POST | `/updates` | 增量增删节点/边（单写者事务） |
| POST | `/query` | `multi_hop` / `shortest_path` / `pattern_match`，`mode=fast|exact` |
| POST | `/query/audit` | 同版本双模式结果逐项差异核对 |
| POST | `/verify` | 图数据与持久化索引交叉一致性校验 |

查询示例：

```bash
curl -s 127.0.0.1:8088/query -H 'Content-Type: application/json' -d '{
  "request_id": "q1",
  "query": "multi_hop",
  "version": 1,
  "mode": "fast",
  "fast_budget": {"node_budget": 5, "prefetch_budget": 3},
  "params": {"start": "p1", "max_hops": 2}
}'
```

## 端到端演示（含强制终止恢复）

```bash
python -m scripts.demo_e2e --db data/demo.db
```

演示 11 个场景：快照加载、坏摘要拒绝、双模式差异核对、最短路径、模式匹配、
增量更新、版本绑定、请求去重、NumPy 统计、完整性校验，以及子进程在意图写入
后被 `os._exit(137)` 强杀，重启后输出 ABORTED 恢复报告并回到最后完整版本。

## 测试

```bash
python -m pytest -q
```

39 个用例覆盖全部业务要求与五个业务错误码。设计细节见
[docs/architecture.md](docs/architecture.md)。

## 业务错误码

`E_SNAPSHOT_INVALID`、`E_WEIGHT_INVALID`、`E_GRAPH_INCONSISTENT`、
`E_VERSION_RETIRED`、`E_CONFLICTING_REQUEST`，统一响应结构：

```json
{"ok": false, "error": {"code": "...", "message": "...", "context": {}}}
```
