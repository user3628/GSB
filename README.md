# 本地列式扫描与压缩组件

这是一个可在 Windows/Linux 本地运行的列式快照扫描服务。它提供确定性快照生成、三种块编码、预算感知的并行调度、SQLite 两阶段分片提交、强制终止后的自动续扫，以及本地 HTTP 查询 API。

核心实现位于 `colscan/`，固定样本生成器为 `scripts/generate_sample.py`，端到端验收脚本为 `scripts/e2e_verify.py`。

## 快速开始

```bash
python -m pip install -r requirements.txt
python scripts/generate_sample.py --out data/snapshots --versions 1 3 --rows 96 --block-size 16
python -m colscan.server --host 127.0.0.1 --port 8765 --state-db data/state.db --reset-state
```

服务只接受回环地址。`--reset-state` 会删除项目内的 SQLite 主文件、`-wal` 和 `-shm` 文件；正常恢复流程不要使用该参数。

加载快照：

```bash
curl -X POST http://127.0.0.1:8765/v1/snapshots/load \
  -H "Content-Type: application/json" \
  -d '{"path":"data/snapshots/snapshot_v1.csnap"}'
```

提交查询：

```bash
curl -X POST http://127.0.0.1:8765/v1/queries \
  -H "Content-Type: application/json" \
  -d '{
    "query_id":"q-demo",
    "columns":["id","name","status","score","active"],
    "predicate":{"op":"and","predicates":[
      {"column":"score","op":"ge","value":35.0},
      {"column":"status","op":"ne","value":"archived"}
    ]},
    "block_start":0,
    "block_end":6,
    "offset":0,
    "limit":null,
    "budget":{
      "max_concurrent_tasks":3,
      "per_task_memory_bytes":1200,
      "round_time_budget_ms":8.0,
      "queue_capacity":256,
      "degrade_mode":"split"
    }
  }'
```

查询状态、分片、裁决轨迹与结果：

```bash
curl http://127.0.0.1:8765/v1/queries/q-demo
curl http://127.0.0.1:8765/v1/queries/q-demo/shards
curl http://127.0.0.1:8765/v1/queries/q-demo/decisions
curl "http://127.0.0.1:8765/v1/queries/q-demo/results?offset=0&limit=10"
curl "http://127.0.0.1:8765/v1/recovery/report?query_id=q-demo"
```

`GET /results` 不带参数时返回查询请求声明的 `offset/limit` 窗口；带分页参数时从完整稳定排序结果中切片，因此分页拼接不重不漏。

## 验证

```bash
python -m unittest discover -s tests -v
python scripts/e2e_verify.py --work-dir artifacts/e2e_work
```

端到端脚本会启动独立的本机 Uvicorn 进程，完成以下检查：

- 生成固定多列多块、混合 `fixed/rle/dict` 编码和空值位图的快照。
- 验证非法快照、旧版本、队列满和损坏块业务码。
- 全量并行扫描并与独立单线程参考扫描逐行比较。
- 低预算扫描产生确定性拆分裁决，结果仍与全量逐行一致。
- 验证分页、重复 `query_id` 幂等复用。
- 使用 `Popen.kill()` 强制终止进程，重启后根据 SQLite 已持久化事实复用已完成分片、恢复未完成分片并自动续扫。
- 恢复结果与 v3 快照的一次性参考扫描逐行比较，并检查恢复报告不声称存在正常退出报告。

最近一次实际运行结果保存在 `artifacts/e2e_output.txt` 和 `artifacts/e2e_work/e2e_report.json`。摘要：

```text
CHECK E_SNAPSHOT_INVALID http=422 reason=checksum mismatch
PHASE block stats columns=6 blocks=36 sample=id:b0000 raw=128 compressed=306 ratio=0.418301
CHECK E_VERSION_RETIRED active=1 | E_SCAN_QUEUE_FULL http=429 limit=2
PHASE full scan complete rows=26 generation=1 snapshot=v1
PHASE low budget complete rows=26 equal=true actions={"admit": 24, "queue": 132, "split": 18}
PHASE pagination pages=7+9 no_gap_no_overlap=true
CHECK E_BLOCK_CORRUPT http=422 block_id=name:b0000
PHASE forced termination committed_before_kill=2 exit_code=-9
PHASE recovery complete rows=24 equal=true reused=2 resumed=4 generation=3
PHASE recovery report source=sqlite_persisted_facts clean_exit_report=false persisted_facts=true
```

## API 与错误协议

成功响应统一为 `{"ok": true, "data": ...}`。失败响应统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_BLOCK_CORRUPT",
    "message": "compressed block is corrupt: ...",
    "details": {"block_id": "name:b0000", "reason": "..."}
  }
}
```

主要路径：

- `POST /v1/snapshots/load`：校验并原子安装快照。
- `GET /v1/snapshots/current`：当前活动快照信息。
- `GET /v1/snapshots/blocks`：逐块输出编码、原始大小、压缩大小与压缩率。
- `POST /v1/queries`：提交查询预算、列选择、谓词、块范围和窗口。
- `GET /v1/queries/{id}`：查询状态与进度。
- `GET /v1/queries/{id}/shards`：分片提交、复用和恢复状态。
- `GET /v1/queries/{id}/decisions`：预算裁决轨迹与动作汇总。
- `GET /v1/queries/{id}/results`：只返回已完整提交代次的结果，可分页。
- `GET /v1/recovery/report`：基于 SQLite 事实的启动恢复报告。

已覆盖的业务码包括 `E_SNAPSHOT_INVALID`、`E_BLOCK_CORRUPT`、`E_SCAN_QUEUE_FULL`、`E_VERSION_RETIRED`，另有 `E_QUERY_INVALID`、`E_QUERY_CONFLICT`、`E_QUERY_NOT_READY`、`E_QUERY_NOT_FOUND`、`E_TASK_MEMORY_EXCEEDED` 和 `E_ROUND_BUDGET_EXCEEDED`。

## 关键语义

- 快照版本必须严格大于当前活动版本；校验失败或旧版本安装失败不会改变当前快照。
- 查询固定在其提交时的快照版本，后续加载新快照不会造成新旧列块混合。
- 谓词采用 SQL 三值逻辑：与 NULL 的普通比较为 `UNKNOWN`，`WHERE` 只保留 `TRUE`；`is_null` / `is_not_null` 显式判断空值。
- 结果按 `block_seq`、块内 `row_offset` 稳定归并，输出列只包含投影列，类型沿用快照声明。
- 每个分片先持久化 `INTENT`，再在同一提交事务中写结果并进入 `COMMITTED`；结果接口只在查询完整归并代次提交后开放。
- 同一 `query_id` 的相同请求只执行一次；不同请求复用同一标识会返回 `E_QUERY_CONFLICT`。

更详细的文件格式、预算估算、调度和恢复状态机见 `docs/TECHNICAL.md`。

