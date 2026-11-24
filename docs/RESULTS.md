# 实际验证记录

记录时间：2026-11-24（Asia/Shanghai）

## 环境

- Linux（WSL2 内核环境），Go `go1.27.4 linux/amd64`
- 模块 `go 1.22.0`
- `modernc.org/sqlite v1.34.4`
- Windows 交叉编译：`GOOS=windows GOARCH=amd64 go build ./...` 通过
- 未使用 Docker/容器，未访问未授权目标

## 自检命令

```text
go test ./...            PASS
go test -race ./...      PASS
go vet ./...             PASS
GOOS=windows GOARCH=amd64 go build ./...  PASS
```

测试覆盖：

- WAL 记录/段头校验和 `E_SEGMENT_CORRUPT`
- 组提交共享屏障、幂等、冲突与事务偏移
- 组写失败/fsync 失败整组回滚
- 物理/元数据回滚失败保留 `PENDING` 意图并封锁继续写入，重启后恢复
- 段轮转失败保留原段
- 跨段 LSN 连续
- 检查点序号/摘要校验与整体替换
- `E_SNAPSHOT_RETIRED`、`E_CHECKPOINT_LAG`
- 安全/不安全回收与强杀后继续回收
- 多读者快照不会被更新页污染
- 同一固定样本通过真实回环 HTTP 请求完整重放
- 子进程强杀后的组提交与检查点恢复
- HTTP API 成功与错误码

## 固定样本

生成命令：

```sh
go run ./cmd/walgroup generate -seed 42 -out samples/workload-42.json
```

- 文件：`samples/workload-42.json`
- 字节数：`9740`
- SHA-256：`7d567b9e180dfeabe7306c76bbf66c10b6a1f65219d5d515efc1d97c3896e0b8`
- 动作数：22
- 段参数：`segment_max_bytes=900`，`max_txns_per_segment=3`

`go run ./cmd/walgroup demo -workload samples/workload-42.json ...` 实际结果：

```text
transport = local-http
run.all_ok = true
verified = true
recovery_after_reopen.status = CLEAN
```

该演示的所有业务动作（组提交、检查点、回收、读、冲突）都通过本机 HTTP 接口执行。

实际观察到的业务码：

```text
commit_group                         OK
checkpoint                           OK
read(page=1, lsn=12)                 OK
read(page=1, lsn=12 after new writes) E_CHECKPOINT_LAG
reclaim(all covered sealed segments)  OK
duplicate group                      OK (fsync_count=0, all duplicate=true)
conflicting transaction              E_CONFLICTING_TXN
checkpoint(sequence=2)               OK
read(page=1, lsn=16)                  OK
read(page=1, lsn=4)                   E_SNAPSHOT_RETIRED
reclaim current/open segment         E_RECLAIM_UNSAFE
```

## 强杀演示

命令：

```sh
go run ./cmd/walgroup crash-demo -data var/crash -report var/crash/report.json
```

### 场景一：组提交 fsync 后、SQLite DURABLE 提交前强杀

实际恢复报告：

```text
status=RECOVERED
facts_only=true
intent=GROUP_COMMIT/PENDING
action=ROLLED_BACK_GROUP_AND_ABORTED_TXNS
aborted_txn_ids=[crash-a crash-b]
segment 1 action=TRUNCATED_PENDING_GROUP
baseline transaction=DURABLE
next_lsn=2
```

验证结论：

- 子进程确实在段文件 fsync 完成之后被外部 `Process.Kill` 终止；
- 同组两个事务均未进入 `DURABLE`；
- 段文件已按意图中的旧大小截断，段头记录数恢复；
- 先前 `baseline` 事务保持 `DURABLE`；
- 重启后可继续从 LSN 2 提交。

### 场景二：检查点意图已写、元数据替换前强杀

实际恢复报告：

```text
status=RECOVERED
intent=CHECKPOINT/PENDING
action=CHECKPOINT_METADATA_UNCHANGED
active_checkpoint_sequence=1
```

验证结论：

- 中断的 sequence 2 没有安装；
- sequence 1 的序号和摘要逐字节一致保留；
- 数据页版本仍从持久化事实恢复；
- 后续干净重启报告为 `CLEAN`，终态意图不会重复污染报告。

## 最终目录产物示例

一次 demo 的数据目录包含：

```text
meta.sqlite
recovery-report.json
wal-0000000000000009.seg
```

其中早期段已被检查点覆盖并真实删除，`meta.sqlite` 中对应记录保持 `RECLAIMED`。
