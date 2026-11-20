# 实际验证记录

验证日期：2026-11-21（Asia/Shanghai）。执行环境为工作区本地 Python 3.14.4 虚拟环境；代码使用 Python 3.11 兼容语法与标准库 SQLite。Windows 离线 wheelhouse 已按 `cp311`、`cp312`、`cp313`、`cp314` 四个 ABI 下载，并通过 `pip --no-index` 解析检查。

## 环境安装

```text
$ python3 --version
Python 3.14.4

$ .venv/bin/python -m pip install fastapi==0.165.0 uvicorn==0.54.0 pytest==9.2.0 httpx==0.28.1
Successfully installed ... fastapi-0.165.0 uvicorn-0.54.0 pytest-9.2.0 httpx-0.28.1 ...
```

SQLite 来自 Python 标准库，未额外安装。

## 测试套件

```text
$ .venv/bin/pytest -q
........................                                                 [100%]
24 passed, 1 warning in 2.28s
```

覆盖项：

- 合法/非法块号、正确/错误摘要、版本与时间持久化。
- 相同请求标识复用、冲突拒绝、并发重复请求只执行一次。
- 同卷并发写入串行化及版本无重复。
- 全量冻结、增量 delta、父链完整解析、首次快照后的真实 COW。
- 父快照删除受引用保护、叶子删除、终态非法迁移。
- 相邻合并、源 `MERGED`、目标引用重写、子快照重挂、链深和引用数下降。
- 非相邻/重复合并拒绝、并发合并 `E_OPERATION_ACTIVE`。
- 回滚差异、执行回滚、版本单调后续写入、快照内容损坏拒绝。
- 合并强杀后 `MERGING -> COMMITTED` 恢复、回滚事务未提交恢复、请求 `ABORTED`、报告一致性检查。
- HTTP 路由的业务码 JSON、全量/增量/合并/删除/回滚/恢复报告端到端接口。

## 强制终止端到端演示

```text
$ .venv/bin/python scripts/e2e_demo.py
...
"chain_before_merge": {
  "chain_depth": 3,
  "reference_block_count": 11,
  "active_snapshot_count": 3
},
"merge_result": {
  "source_state_after": "MERGED",
  "chain_depth_before": 3,
  "chain_depth_after": 2,
  "reference_block_count_before": 11,
  "reference_block_count_after": 9,
  "refs_rewritten": 8,
  "children_reparented": ["snap-inc-2"]
},
"rollback_diff": {
  "write_back_blocks": 1,
  "unchanged_blocks": 7,
  ...
},
"forced_termination": {
  "http_client_error": "RemoteDisconnected",
  "server_exit_code": 86,
  "durable_facts_after_crash": {
    "snapshot_states": {
      "snap-full": "COMMITTED",
      "snap-inc-1": "MERGED",
      "snap-inc-2": "MERGING"
    },
    "pending_intents": [
      {"operation": "SNAPSHOT_MERGE", "state": "PENDING"}
    ],
    "in_progress_requests": ["crash-merge"]
  }
},
"recovery_report": {
  "status": "RECOVERED",
  "durable_only": true,
  "summary": {
    "consistency_checks_ok": true,
    "intents_aborted": 1,
    "pending_intents_scanned": 1,
    "requests_aborted": 1,
    "snapshots_repaired": 1
  },
  "snapshot_repairs": [
    {"snapshot_id": "snap-inc-2", "from_state": "MERGING", "to_state": "COMMITTED"}
  ],
  "consistency_ok": true
},
"post_recovery_merge": {
  "source_state_after": "MERGED",
  "chain_depth_before": 2,
  "chain_depth_after": 1,
  "reference_block_count_after": 8
},
"conclusion": "PASS: snapshot chain, incremental merge, rollback diff, and forced-termination recovery all verified from durable SQLite state."
```

完整结果保存在 `artifacts/e2e/e2e_demo_result.json`。演示使用 `samples/seed.txt` 的固定种子 `20261121` 和 `samples/blocks.json` 的确定性块内容。

## Windows 离线依赖检查

```text
$ .venv/bin/python -m pip install --dry-run --no-index --find-links wheelhouse \
    --only-binary=:all: --platform win_amd64 --python-version 3.11 \
    --implementation cp --target /tmp/wheelcheck -r requirements.txt
Would install ... fastapi-0.165.0 ... pydantic_core-2.50.5 ... uvicorn-0.54.0
```

`wheelhouse/` 包含 `pydantic_core` 的 `cp311`、`cp312`、`cp313`、`cp314` Windows x64 wheel，以及所有纯 Python 传递依赖。Windows 上的实际启动、pytest 与演示可由 `scripts/test_windows.ps1` 复验。

## 恢复语义核对

- 强杀发生后，SQLite 中可核对事实为 `snap-inc-2=MERGING`、一条 `PENDING` 合并意图和一条 `IN_PROGRESS` 请求。
- 重启扫描未完成合并意图，将源快照恢复为 `COMMITTED`；目标引用未被第二阶段事务改写，因此不存在“元数据已合并但部分引用仍指向旧快照”的可见状态。
- 请求被持久化标记为 `ABORTED`，不会在重启后静默重放。
- 恢复报告状态为 `RECOVERED`，`durable_only=true`，并明确说明报告只基于重启时已提交事实，不声称强杀前生成过退出报告。
- 恢复后再次执行合并成功，证明修复后的快照链可继续服务。

