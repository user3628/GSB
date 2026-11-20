# 实际验证记录

## 环境

| 项目 | 实际值 |
| --- | --- |
| 执行环境 | 本地 WSL/Linux 工作区，回环地址 `127.0.0.1` |
| Python | 3.14.4 |
| SQLite | 3.46.1 |
| NumPy | 2.5.5 |
| FastAPI | 0.165.0 |
| Uvicorn | 0.54.0 |
| pytest | 9.2.0 |

未使用 Docker、容器或外部网络服务。`pip check` 返回 `No broken requirements found.`。

## 自动化测试

执行：

```bash
.venv/bin/python -m pytest -q
```

结果：

```text
..................                                                       [100%]
19 passed（约 5 秒）
```

覆盖内容：

- 滚动窗口纪元对齐与 `999/1000` 边界。
- 滑动窗口全部覆盖分配。
- 会话窗口精确间隔、切分与关闭。
- Neumaier 补偿求和、固定 6 位小数、空聚合、最近秩分位和样本规模。
- 迟到队列策略与 `E_LATE_EVENT` 原子拒绝。
- `OPEN -> CLOSED -> EMITTED` 合法迁移及 `E_INVALID_TRANSITION`。
- 并发同 ID 批次只提交一次，冲突内容 `E_CONFLICTING_BATCH`。
- 写入中途查询仍只看到旧完整版本。
- 分页跨新提交保持稳定、不重不漏。
- 快照版本非单调、摘要损坏时 `E_SNAPSHOT_INVALID` 且原状态可用。
- 合法快照整体替换旧状态。
- 固定 `samples/snapshot_sample.json` 可加载。
- 硬容量上限、soft limit 淘汰和 HTTP 错误结构。
- 三场景真实 `exit 137`、重启与后续摄入恢复。

## 强制终止恢复验证

执行：

```bash
.venv/bin/python scripts/verify.py
```

实际摘要：

```json
{
  "all_recovery_paths_item_by_item_equal": true,
  "uninterrupted_windows": 82,
  "recovery_scenarios": {
    "crash-after-intent": {
      "windows_equal": true,
      "late_queue_equal": true,
      "differences": []
    },
    "crash-during-snapshot-temp": {
      "windows_equal": true,
      "late_queue_equal": true,
      "differences": []
    },
    "crash-after-snapshot-replace": {
      "windows_equal": true,
      "late_queue_equal": true,
      "differences": []
    }
  },
  "report": "artifacts/verification/verification_report.json"
}
```

基线未中断路径：

- 状态版本：12
- 窗口结果：82
- 迟到队列：20
- 完整快照：12
- 分页：12 页，无重复、无遗漏
- 窗口结果 SHA-256：`3e26aa26ecc9761ff8cebe024c54ee27b3531a56800427c338dcede8bb50f31c`
- 迟到队列 SHA-256：`70d6243a7f2a7c5deac5e7c3bbbd8b6394ef9569810bfbcff349ac16d15915db`

三个恢复场景：

| 场景 | 硬终止点 | 进程退出 | 恢复动作 | 差异清单 |
| --- | --- | --- | --- | --- |
| `crash-after-intent` | SQLite 已写 pending 意图，尚未合并 | 137 | 回滚版本 6，标记意图中止，重试批次 | 空 |
| `crash-during-snapshot-temp` | 临时快照已 `fsync`，尚未 `os.replace` | 137 | 忽略 `.tmp`，回滚版本 6，标记意图中止，重试批次 | 空 |
| `crash-after-snapshot-replace` | 快照已原子替换，尚未完成内存发布/SQLite 收尾 | 137 | 加载版本 7，证明意图已提交；重试请求直接重放 | 空 |

逐项比较对象包括窗口类型、起点、终点、分组、状态、计数、非空计数、固定小数和、最值、分位/样本规模、首末事件时间、发射版本及迟到队列全部字段。

完整报告内同时保存未中断路径的 82 条窗口结果、20 条迟到队列结果、各路径规范化摘要和逐项差异清单：

- `artifacts/verification/verification_report.json`

## 固定快照样本

```text
file: samples/snapshot_sample.json
version: 100
checksum: 052a44d8a3c2dd46f7c350b2858ba756e9db0a47daca262f075e0992d471b5d8
config_fingerprint: 4a60541efc099bff74db686f0c13ff46bc1ab97c31076fb9d1a060403db55bd3
window_count: 38
late_queue_length: 14
```

该文件由 `scripts/build_sample_snapshot.py` 从固定 phase1 数据生成，并通过与 HTTP 加载相同的校验器。

## 业务错误码实测

| 错误码 | 触发方式 | 实测结果 |
| --- | --- | --- |
| `E_EVENT_INVALID` | 事件缺失 `group` | 400，状态版本保持 0 |
| `E_INVALID_TRANSITION` | `OPEN -> EMITTED` 验证接口/状态机 | 409，窗口状态不变 |
| `E_LATE_EVENT` | `late_policy=reject` 后向关闭窗口投递 | 409，整批不提交 |
| `E_SNAPSHOT_INVALID` | 非单调版本、篡改 checksum | 409，加载前状态保持可用 |
| `E_CONFLICTING_BATCH` | 同 `batch_id` 不同内容 | 409 |
| `E_CAPACITY_EXCEEDED` | 小 hard limit 单批创建过多窗口 | 429，状态版本和窗口数保持原值 |

## 说明

本次实测环境是工作区提供的 WSL/Linux。Windows 下运行命令及跨平台持久化注意事项已记录在 README；原生 Windows 进程终止与目录行为未在本工作区重复实测。
