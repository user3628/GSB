# 实际验证记录

验证环境：Linux/amd64 开发容器（Go 工具链可交叉构建 Windows），SQLite 驱动为纯 Go `modernc.org/sqlite v1.34.5`。

## 自动化测试

```text
$ go test ./... -count=1
?    local/objectstore/cmd/objectstore   [no test files]
ok   local/objectstore/internal/engine   1.013s
ok   local/objectstore/internal/httpapi  0.540s
?    local/objectstore/internal/model    [no test files]
?    local/objectstore/internal/store    [no test files]
```

覆盖内容：

- 固定样本 `samples/scenario.json`：乱序 `3,1,2`、重复分片、取消、代际重建；
- 乱序合并后的对象 SHA 与字节顺序一致；
- 重复摘要幂等计数，冲突摘要保留首次提交；
- 24 路同键并发创建只有一个成功；
- 取消后分片 `void`、槽位释放、新代际可合并；
- TTL 到期后新会话回收旧会话；
- 缺片、请求总大小错误、摘要复核失败均不写对象元数据；
- 零预算确定性跳过清单；
- 全局复核并发槽与零长度等待队列触发 `E_VERIFY_QUEUE_FULL`；
- 模拟意图已落盘但数据未提交后重启：报告显示 `applied=false`，状态与完整提交一致，随后重试合并成功；
- HTTP 同请求标识并发重试只创建一条会话，响应正文复用；
- HTTP 请求结果缓存跨进程重启复用，不会重复创建会话；
- HTTP 同键不同标识并发创建只允许一个成功；
- 恢复与在途变更并发执行时，恢复等待完整两段式提交，不会误放弃未中断意图；
- 统一 JSON 错误码 `E_CONFLICTING_REQUEST`、`E_SESSION_CONFLICT`、`E_PART_MISMATCH`、`E_PART_MISSING`、`E_SIZE_MISMATCH`。

## 竞态检测

```text
$ go test -race ./internal/engine ./internal/httpapi -count=1
ok   local/objectstore/internal/engine   2.615s
ok   local/objectstore/internal/httpapi  1.782s
```

## Windows 构建

```text
$ GOOS=windows GOARCH=amd64 go build -o /tmp/objectstore-final-windows.exe ./cmd/objectstore
$ ls -lh /tmp/objectstore-final-windows.exe
-rwxr-xr-x 16M /tmp/objectstore-final-windows.exe
```

构建成功，无 CGO 依赖。

## 本地 HTTP 冒烟

启动：

```text
$ /tmp/objectstore-smoke -db /tmp/objectstore-smoke-1795455276-b.db \
    -addr 127.0.0.1:18081 -verify-budget-ms 2000
startup recovery report=1 interrupted_intents=0 interrupted_requests=0 checks_passed=true
listening on http://127.0.0.1:18081
```

按固定样本创建会话，并按 `3,1,2` 顺序提交三个分片，随后采用新的请求标识重传分片 2：

```text
create: session_id=upl_7684cf94aa7937a83679e1383c34a20b generation=1 status=active
part 3: size=23 duplicate_count=0
part 1: size=23 duplicate_count=0
part 2: size=22 duplicate_count=0
part 2 retry: duplicate=true duplicate_count=1
parts list: [1, 2, 3]
```

合并结果：

```json
{
  "object": {
    "object_key": "samples/catalog/items.json",
    "generation": 1,
    "total_size": 68,
    "part_count": 3,
    "sha256": "3bbcaa6e982f1c4e82bd9d858d2312e279245783172de0003b173ea430a3e898"
  },
  "session": {"status": "completed"},
  "verification": {
    "verified_part_numbers": [1, 2, 3],
    "skipped_part_numbers": [],
    "degraded": false
  }
}
```

对象摘要与 `samples/scenario.json` 的预期值一致。恢复报告 `report_id=2`：

```json
{
  "checks": {
    "passed": true,
    "completed_sessions_without_object": 0,
    "active_parts_in_inactive_sessions": 0,
    "expired_active_sessions": 0
  },
  "intent_counts": {"committed": 6},
  "objects": [{
    "key": "samples/catalog/items.json",
    "sha256": "3bbcaa6e982f1c4e82bd9d858d2312e279245783172de0003b173ea430a3e898",
    "generation": 1
  }]
}
```

随后对服务进程执行 `kill -9`，再用同一个 SQLite 文件重启。启动恢复直接产出 `report_id=3`，`checks.passed=true`；通过 HTTP 重新查询得到的对象元数据和会话 `completed` 状态与强杀前完全一致。该验证确认已提交状态仅依赖 SQLite 持久化事实即可恢复；待提交意图的恢复路径由自动化测试中的“意图已落盘、数据事务未提交”场景覆盖。

