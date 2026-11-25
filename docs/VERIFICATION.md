# 实际验证记录

验证日期：2026-11-26（Asia/Shanghai），最终验收完成于 `2026-11-26T00:57:39+08:00`。

环境：

```text
$ go version
go version go1.27.4 linux/amd64

$ sqlite3 --version
3.46.1 2024-08-13 13:58:30 ...
```

## 命令

```bash
go test ./... -count=1
go test -race ./... -count=1
./scripts/verify.sh
```

结果：

- `gofmt -l cmd internal`：无输出
- `go vet ./...`：通过
- 普通测试：全部通过
- `CGO_ENABLED=0 go test ./...` 与纯 Go 构建：通过
- 竞态测试：全部通过
- 确定性演示：通过
- 回环端点真实收到 3 次自愈下发
- 强制终止与重启恢复：通过

完整产物位于 `artifacts/verification/`。

## 确定性样本结果

SQLite 保存了 4 个连续完整版本：

```text
version  snapshot_version  payload_bytes
0        0                 292
1        1                 862
2        1                 20196
3        1                 20196
```

版本 2 是首次重建：

| 持有者 | 漂移 | 幅度 | 自愈动作 | 下发状态 |
| --- | --- | ---: | --- | --- |
| holder-alpha | 否 | 0ms | 无 | - |
| holder-bravo | 是 | 30ms | `REQUIRE_EARLY_RENEWAL` | `SENT` |
| holder-charlie | 是 | 100ms | `SHORTEN_LEASE` | `SENT` |
| holder-delta | 是 | 230ms | `REVOKE_LEASE` | `SENT` |

回环计数：`heals=3`。alpha 的重复投递为 1 次；原样重放整批后，alpha 唯一上报仍为 6 条、重复投递累计为 8 次，说明没有新增规范记录。

观测结果中：

- alpha 首条心跳 `elapsed=0`，首条续约 `elapsed=700ms`、安全点 `700ms`、状态 `ON_TIME`
- 相同客户端时间戳的记录按服务端到达时间与序号稳定排序
- 全量重排测试的重建 JSON 字节一致

## 陈旧与冲突拒绝

演示先提交版本 1 快照并完成版本 2 重建，然后：

```json
{
  "code": "E_STALE_READ",
  "message": "reconstruction version 2 is behind observed version 3",
  "current_version": 2,
  "observed_version": 3
}
```

```json
{
  "code": "E_STALE_SNAPSHOT",
  "message": "snapshot version 1 is not greater than current snapshot version 1",
  "current_version": 1,
  "observed_version": 1
}
```

摘要冲突：

```json
{
  "code": "E_REPORT_MISMATCH",
  "message": "report_seq 2 has conflicting digest; first digest sha256:c65f... was retained",
  "holder_id": "holder-alpha",
  "report_seq": 2
}
```

冲突批次没有推进 state version；首次摘要保留。

## 强制终止恢复

`scripts/kill-mid-batch.sh` 使用固定种子样本：

1. 提交完整基线快照（版本 1）
2. 写入 `batch_rebuild` 意图，目标版本 2
3. 真实发起 `holder-bravo` 的 `REQUIRE_EARLY_RENEWAL` 回环 POST
4. 端点观察到请求后写哨兵文件
5. 脚本对进程执行 `kill -9`
6. 新进程执行 `recover`

恢复后 SQLite 事实：

```text
version  snapshot_version  payload_bytes
0        0                 292
1        1                 862

kind             state      base_version  target_version
snapshot_switch  COMMITTED  0             1
batch_rebuild    ABORTED    1             2

recovery_reports
1
```

恢复报告的版本差异明细：

```text
base_version=1
last_committed_version=1
intended_target=2
changed=false
details:
  intent <id> (batch_rebuild) targeted version 2
  committed version remained 1
  intent payload retained on disk for postmortem review
```

意图 payload 中仍可看到计划动作：

```json
{"action":"REQUIRE_EARLY_RENEWAL","holder_id":"holder-bravo","trigger":"LATE_OUTSIDE_TOLERANCE"}
```

这证明恢复报告只依据已落盘事实，并把未完成意图标为中止，而不是把外部下发误报为已提交。

## 自动化测试覆盖

- 非法时长、非法摘要、采集顺序倒退、未知类型
- 幂等重复与摘要冲突保留首次记录
- 原始批次随机乱序后，去重与重建结果仍一致
- 随机重排后重建 JSON 完全一致
- 同时间戳按服务端到达与上报序号排序
- 安全点上下界、连续迟到、间隔阈值、租约过期
- 三类声明式自愈动作与 `AUTO` 选择
- 回环延迟、拒绝、连接不可达、超时真实断连
- 全局并发上限、有限队列、`E_QUEUE_FULL` 零启动
- 预算耗尽延后排序、剩余预算、漂移/去重状态持久化
- 快照整体替换、陈旧优先、失败不改旧状态
- 读取版本陈旧及双方版本
- SQLite 预写意图、原子提交、并发只读、恢复后续写
- 同一数据库的第二个写者被进程锁拒绝，关闭后锁可重新获取
- `SIGKILL` 中途终止后的真实重启恢复
- 公开 `arbiter` 门面完成快照、摄取、读取与恢复

