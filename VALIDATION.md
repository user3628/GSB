# 实际验证记录

- 验证日期：2026-11-25（Asia/Shanghai）
- 执行环境：Linux amd64，Go `go1.27.4`，纯 Go SQLite 驱动 `modernc.org/sqlite v1.34.5`
- 目标平台检查：`GOOS=windows GOARCH=amd64 go build ./...` 通过
- 所有数据由内置固定种子生成器写出；未访问外部数据集

## 自动测试

```text
$ go test ./...
?       motionlab/cmd/motionlab        [no test files]
ok      motionlab/internal/engine      1.275s
ok      motionlab/internal/httpapi     0.525s
?       motionlab/internal/model       [no test files]
ok      motionlab/internal/motion      0.460s
ok      motionlab/internal/store       0.273s
?       motionlab/internal/webui       [no test files]

$ go test -race ./internal/...
ok      motionlab/internal/engine     11.861s
ok      motionlab/internal/httpapi    6.178s
ok      motionlab/internal/motion     8.501s
ok      motionlab/internal/store      5.397s
RACE_EXIT=0

$ go vet ./...
VET_EXIT=0

$ GOOS=windows GOARCH=amd64 go build ./...
WINDOWS_BUILD_EXIT=0
```

覆盖点：

- 固定种子样本与真值帧跨目录逐字节一致；
- 分支 0 每步预测帧 SHA-256 与真值帧相同、MAE 为 0；
- 共享初始帧写保护及 8 位量化边界；
- 导入后清单/帧索引重载一致；
- 同配置并发分支连续执行两次，规范化报告字节一致；
- 仅派发剪枝后幸存分支时，状态/帧摘要、误差曲线、步记录和配额使用完全一致；
- 优雅停止后恢复与连续执行规范化报告字节一致；
- `stage_mode` 每次仅执行一个波次，波次边界暂停并在后续恢复后与连续执行的分支步骤一致；
- 时间线溢出及结构化上下文；
- 同进程与两个独立测试进程竞争检查点时恰好一个成功、另一个得到 `E_STALE_CHECKPOINT`；
- 策略 `advance`、`replacement`、`rollback` 三态；
- HTTP 页面、真值帧、预测帧、完整运行生命周期与错误响应体。

## 最终演示

```text
$ go build -o /tmp/motionlab-final-bin ./cmd/motionlab
$ /tmp/motionlab-final-bin demo --data /tmp/motionlab-final-231
```

最终自动验收：`PASS`。

### 三条内置序列

| 序列 | 完成检查点 | 完成步数 | 剪枝数 | 配额回收 | 最佳分支 | 最佳平均 MAE | 确定性 SHA-256 |
|---|---:|---:|---:|---:|---:|---:|---|
| `harbor_slide` | 40 | 40 | 3 | 8 | 0 | 0.000000 | `4f7139981c79da1b4432509d1db38154a1c8c483d569b131982df91c4f444df1` |
| `orchid_drift` | 28 | 28 | 2 | 8 | 0 | 0.000000 | `2b1eba6f42b584c2606c7ad9d86e1351f1ee520ab67063839cfaacbf397597de` |
| `signal_turn` | 54 | 54 | 4 | 6 | 0 | 0.000000 | `10f41de89a3f3ec021cf579b05d8fdf334dfe14e2c862caae34f933811b67f23` |

`harbor_slide` 分支明细：

| 分支 | 状态 | 已提交步数 | 平均 MAE | 最终 MAE | 裁决 |
|---:|---|---:|---:|---:|---|
| 0 | `quota_limited` | 12 | 0.000000 | 0.000000 | 真值基线；16 步请求被每分支 12 步配额截断 |
| 1 | `pruned` | 11 | 0.535866 | 2.591064 | 第 11 步 MAE 超过阈值 2.5 |
| 2 | `pruned` | 11 | 0.885646 | 4.182699 | 第 11 步 MAE 超过阈值 2.5 |
| 3 | `pruned` | 6 | 0.754666 | 2.698812 | 第 6 步 MAE 超过阈值 2.5 |

### 验收检查

```text
deterministic_replay                 PASS
prune_budget_equivalence             PASS
forced_termination_recovery          PASS
timeline_overflow                    PASS
shared_frame_write_guard             PASS
stale_checkpoint_rejected            PASS
time_budget_not_silent               PASS
policy_advance_replace_rollback      PASS
```

关键证据：

- 确定性重放两次指纹相同：
  `4f713998...44df1`
- 剪枝后仅派发幸存分支 0，投影 SHA 与完整运行中的分支 0 相同：
  `93a76ce81be981e2af2b35928c317cc6d0e70f7e43d45d94e3ee5682f29a8b45`
- 强制终止：子进程提交第 7 步后以退出码 `137` 终止；恢复一次后指纹仍为
  `4f713998...44df1`，与连续执行完全一致。
- 时间线溢出：
  `E_TIMELINE_OVERFLOW: requested 999 prediction steps but truth timeline has 14 steps (sequence=orchid_drift)`
- 共享帧写保护：
  `E_SHARED_FRAME_WRITE`，携带 `sequence_id=harbor_slide`、`branch_id=1`。
- 重复检查点：
  `E_STALE_CHECKPOINT: checkpoint sequence must advance consecutively: expected 1, stored 1`
- 轮次预算 1 vms：3 个分支均被持久化为 `not_started`，原因含 `E_TIME_BUDGET`。
- 策略：版本 1 → 2 正常推进；2 → 1 回退被拒；版本 3 改变运行参数被判为整体替换并失效重建。

## HTTP 本机冒烟

服务仅绑定 `127.0.0.1:18080`：

```text
GET  /                                     200  text/html  40890 bytes
GET  /api/health                           200  SQLite / deterministic-virtual-time
GET  /api/bootstrap                        200  3 sequences / policy balanced-v1
GET  /api/sequences/harbor_slide/truth/3.png  200 image/png
POST /api/runs                             201  run started
GET  /api/runs/{id}                        200  status=completed
GET  /api/runs/{id}/branches/0/frames/3.png 200 image/png
sha256(predicted b0 step3) == sha256(truth step3)
```

分支 0 的预测帧与真值帧 PNG 文件哈希均为：

```text
a1cfacfd5af81d0539d2765a6b410373de1c8730402671f10736f13d252e5fe9
```

预算 1 vms 的 HTTP 运行返回三个显式 `not_started` 分支；步数 99 的请求返回
HTTP 422 与 `E_TIMELINE_OVERFLOW`（真值时间线 14 步，超出 85 步）。

分阶段 HTTP 冒烟：`stage_mode=true` 首次启动后状态为 `paused`，仅波次 1 的分支有提交；
调用 `/resume` 后状态为 `completed`，40 个步骤全部落库：

```text
stage1 paused    completed_steps=23  branches=(0 completed, 1 pruned, 2 idle, 3 idle)
stage2 completed completed_steps=40  branches=(0 completed, 1 pruned, 2 pruned, 3 pruned)
```

## 复现

```bash
go test ./...
go test -race ./internal/...
go vet ./...
GOOS=windows GOARCH=amd64 go build ./...
go run ./cmd/motionlab demo --data ./demo-runtime
```

演示报告中的时间戳和本地路径会随运行变化；确定性 SHA、步骤摘要、错误曲线与裁决不应变化。
