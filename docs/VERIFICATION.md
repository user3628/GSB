# 实际验证记录

## 环境

```text
OS: linux/amd64 (Windows 交叉编译通过)
Go: go1.27.4
SQLite: modernc.org/sqlite v1.34.5（纯 Go）
HTTP: 127.0.0.1:18081
Run directory: ./run（每次验证前清理）
```

## 命令

```bash
go test ./...
go test -race ./...
go vet ./...
GOOS=windows GOARCH=amd64 go build ./...
go run ./cmd/mpckpt verify -dir ./run -port 18081
```

测试结果：

```text
ok  mpckpt/internal/api
ok  mpckpt/internal/engine
ok  mpckpt/internal/format
ok  mpckpt/internal/numeric
```

竞态检测、静态检查和 Windows 交叉编译均通过。

## 端到端输出

```text
MPCKPT VERIFY: PASS
run_directory=./run
import format=binary version=2 verdict=ACCEPT dataset_version=1 details=[]
import format=jsonl version=1 verdict=DEGRADE_ACCEPT dataset_version=2 details=[
  DEFAULT_POOL x6,
  LOSS_SCALE_PRECISION_UPCAST float32 -> fixed64
]
precision within_policy=true max_abs_error_fixed=217 max_rel_error_ppm=355
          cumulative_rounding_error=4645
          checksum=201120c021a1da061197f935709f1ba8934a0b2c075eeafaf0b62d3e3bdff28b
evictions=1 gpu_usage_after=64
crash_recovery_events=1 dataset_version_preserved=true
artifacts=run/verification-results.json
```

关键中间结果：

| 步骤 | 结果 |
|---|---|
| 二进制 v2 导入 | `ACCEPT`，数据版本 1 |
| JSONL v1 导入 | `DEGRADE_ACCEPT`，6 条默认池补齐，1 条损失缩放精度升级 |
| 全量检查点 | 数据版本 3，6 个分片全部在写预算内，GPU 使用 64B |
| 单分片精度增量 | 数据版本 4，策略版本 2，仅 `r2-attn-fp16-b` 派生失效并重建 |
| 驻留驱逐 | 数据版本 5，`r1-embed-fp16-a` 从 VRAM/RESIDENT 迁至 PERSISTED，释放 64B |
| 精度报告 | 最大绝对偏差 `217e-6`，最大相对偏差 `355ppm`，累计舍入误差 `4645e-6` |
| 重复精度查询 | 报告校验和与完整 JSON 逐位一致 |
| 同提交重放 | `same_commit_id_reused=true` |
| 同标识异内容 | `E_CONFLICTING_REQUEST` |
| SIGKILL 后重启 | 恢复事件 1 条，数据版本仍为 5 |
| 未提交分片回滚 | `r1-embed-master -> PENDING` |
| 未提交驱逐回滚 | `r3-ffn-fp16-extra -> VRAM/RESIDENT` |

`run/verification-results.json` 保存完整响应，包括每个 FP16 元素的 `master_fixed`、`restored_fixed`、`abs_error_fixed` 和 `rel_error_ppm`。

## 强制终止方法

验证器启动独立的 `mpckpt serve` 子进程。服务端测试接口先持久化 `RUNNING` 意图，再分开提交一个 `WRITING` 分片和一个尚未完成的 VRAM→PERSISTED 驱逐，随后保持连接。父验证器收到 ready 后 150ms 调用 `Process.Kill()`（SIGKILL），再用同一运行目录重启服务。重启扫描意图并产生恢复事件，而不是依赖进程内补偿。

## 覆盖的业务码

单元测试稳定触发并断言：

- `E_SPEC_INVALID`：非法精度/尺寸，验证失败不改变业务状态。
- `E_RECORD_CORRUPT`：二进制与 JSONL CRC/位图损坏，含记录序号。
- `E_FORMAT_UNSUPPORTED`：未知必需能力位、矩阵拒绝/未知版本。
- `E_POOL_FULL`：容量不足，含所需与可释放字节。
- `E_PRECISION_DRIFT`：阈值收紧后阻断提交并指明分片。
- `E_STALE_SNAPSHOT`：策略版本不递增。
- `E_STALE_BASE`：增量基础版本过期且不改变状态。
- `E_WRITE_QUEUE_FULL`：零预算 + 有界队列溢出。
- `E_CONFLICTING_REQUEST`：同提交标识异内容。
