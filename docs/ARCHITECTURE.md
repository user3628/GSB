# 事务与状态设计

## 写入协议

```text
client
  -> single writer lock
  -> full request validation (no database mutation)
  -> duplicate batch lookup
  -> write PENDING intent transaction
  -> BEGIN state transaction
       re-check duplicate
       apply group type evolution
       assign/merge all window aggregates
       queue or reject late events
       insert batch/events/transition records
       increment state_version
       mark intent COMPLETED
     COMMIT
```

`PENDING` 意图与业务状态不在同一事务中。这样进程在两者之间或状态事务中途死亡时，重启一定能看到可恢复的意图。业务状态与 `COMPLETED` 标记同事务提交，因此不会出现“批次已提交但意图仍待恢复”的正常状态。

## 窗口键与状态版本

- 滚动/滑动窗口键由 `spec_hash + kind + group + start + end` 决定。
- 会话窗口键由 `spec_hash + kind + group + session_start` 决定；结束时间随事件推进，但对同一会话保持稳定键。
- 每次批次或显式状态操作推进一个全局 `state_version`。窗口自身 `version` 记录被合并次数。
- 查询在单一读事务中读取 `state_version`、窗口、类型信息，保证返回完整提交版本。

## 快照格式

```json
{
  "format": "sqlite-stream-window-aggregator/v1",
  "version": 1,
  "created_at": "...",
  "state": {
    "state_version": 7,
    "windows": [],
    "late_events": [],
    "sessions": [],
    "type_evolution": [],
    "group_types": [],
    "batches": [],
    "batch_events": [],
    "window_transitions": []
  },
  "checksum": "sha256:..."
}
```

摘要覆盖除 `checksum` 外全部字段，使用键排序、紧凑分隔符和 UTF-8 JSON。加载前先验证格式、摘要、版本和结构，再进入替换事务。替换表包含全部状态表，但保留本库的 `snapshots` 与 `intents` 审计表。

## 类型切换

聚合状态保存“最宽 scale”：

- 整数 10、scale 2 小数 `1.25`：内部为 `1000 + 125 = 1125`，输出 `11.25`。
- 后续切回整数 2：内部加 `200`，仍输出 `13.25`；分组当前声明切回 integer，但历史聚合不会丢失小数。
- scale 从 2 提升到 4：已有 `sum_scaled/min_scaled/max_scaled` 乘 `100`，精确无舍入。
- 声明 scale 小于近期数据时，已有窗口维持较大 scale；只有后续输入校验受新声明约束。

## 崩溃恢复

启动时扫描所有 `PENDING` 意图：

| 意图类型 | 不一致时的动作 |
|---|---|
| `batch` | 批次不存在或未提交：写 `ABORTED` 批次记录 |
| `snapshot_create` | 快照行未提交：中止意图，保留上一个完整快照 |
| `snapshot_load` | 状态替换未提交：中止意图，原状态由事务回滚保留 |
| 窗口操作 | 中止意图，窗口状态由事务回滚保留 |

恢复报告不推断未落盘数据，只描述扫描到的意图、实际写入的终态、当前状态版本和完整快照版本。
