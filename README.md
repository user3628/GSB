# h2diag：本地 HTTP/2 多路复用流调度诊断组件

`h2diag` 面向本地回环环境，用确定性帧序列复现 HTTP/2 多路流的窗口约束、状态迁移和加权发送顺序。组件只读取本地 JSON 样本，通过本地 HTTP 接口驱动，不连接外部服务，也不使用 Docker。

## 能做什么

- 解析 `HEADERS`、`DATA`、`WINDOW_UPDATE`、`RST_STREAM`、`SETTINGS` 帧序列，整批预校验后再落盘。
- 维护连接窗口、流窗口、已发送未确认字节，以及 `IDLE`、`OPEN`、`HALF_CLOSED_LOCAL`、`HALF_CLOSED_REMOTE`、`CLOSED`、`RESET` 六种流状态。
- 按严格优先级 + 同级整数加权最大余数法调度 DATA；权重份额固定为 `1,000,000` PPM。
- 可选 `log2(weight)` 分桶 + 等距采样估计路径，输出采样率、桶方法、置信说明、精确顺序、估计顺序和差异原因。
- 用 SQLite WAL、意图记录和完整版本事务保存状态；启动时恢复未完成意图。
- 提供历史版本快照、轮次轨迹、校验和恢复报告接口，以及统一业务错误码 JSON。

详细设计见 [docs/DESIGN.md](docs/DESIGN.md)，实际验证记录见 [docs/VALIDATION.md](docs/VALIDATION.md)。

## 环境与构建

要求 Go 1.22 或以上版本。唯一直接第三方依赖是 `modernc.org/sqlite`。本机已准备离线模块缓存；正常情况下执行：

```bash
go version
go test ./...
go build -o h2diag ./cmd/h2diag
```

若迁移到另一台离线机器，先准备模块缓存，然后使用：

```bash
GOPROXY=off GOSUMDB=off go test ./...
```

演示脚本另外使用 `curl`，但仍只访问 `127.0.0.1`。

## 运行

```bash
go run ./cmd/h2diag \
  -addr 127.0.0.1:18080 \
  -db data/h2diag.sqlite
```

服务默认只监听回环地址。健康检查：

```bash
curl --noproxy '*' http://127.0.0.1:18080/healthz
```

## 固定样本

`samples/` 是纳入版本控制的确定性样本：

- `baseline.json`：2 个连接、50 条流、200 帧，载荷 16 到 65,535 字节，窗口增量 65,535 与 4,194,304 字节；包含多个时间批次、优先级、窗口更新和 RST。
- `sampling.json`：同一拓扑，启用低阈值分桶估计路径。
- `blocked.json`、`window-overflow.json`、`invalid-transition.json`、`invalid-frame.json`：错误路径。
- `conflict-original.json` / `conflict-mutated.json`：同一样本标识、不同内容的冲突检测。
- `reset.json`：同一时间批次内先排队 DATA 再 RST，验证 RESET 流不发送 DATA。

样本由 `cmd/samplegen` 确定性生成：

```bash
go run ./cmd/samplegen -out samples
```

## HTTP 接口

### 提交完整样本

```bash
curl --noproxy '*' -H 'Content-Type: application/json' \
  --data-binary @samples/baseline.json \
  http://127.0.0.1:18080/v1/samples
```

首次成功提交返回 `201`，重复提交相同样本标识与内容返回 `200` 且 `reused=true`，不会再次推进；相同标识但内容哈希不同返回 `409 E_CONFLICTING_SAMPLE`。

### 按流分批提交

第一批携带 `metadata`，后续批次可省略；批次内帧无需连续，最终按 `(timestamp, sequence)` 规范化排序。`final=true` 的批次触发预校验和推进。

```json
{
  "batch_id": "conn-a-stream-1",
  "final": false,
  "metadata": {
    "config": { "round_bytes": 16384 },
    "connections": [],
    "streams": []
  },
  "frames": []
}
```

```bash
curl --noproxy '*' -H 'Content-Type: application/json' \
  --data-binary @batch.json \
  http://127.0.0.1:18080/v1/samples/baseline-v1/batches
```

### 查询

- `GET /v1/samples/{id}`：当前完整快照。
- `GET /v1/samples/{id}?version=42`：指定已提交版本快照。
- `GET /v1/samples/{id}/rounds?limit=1000&offset=0`：调度轮次轨迹。
- `GET /v1/samples/{id}/verify`：重算并校验快照摘要。
- `GET /v1/samples/{id}/recovery`：该样本的恢复报告。
- `GET /v1/recovery?sample_id={id}`：全局或按样本查询恢复报告。

快照查询在 SQLite 读事务内完成，只会看到某个完整提交版本。历史版本来自 `commits.state_json`，并同时校验保存的 SHA-256 摘要。

## 演示

```bash
./scripts/demo.sh
```

脚本自行构建二进制、选择空闲回环端口，并完成：

1. `baseline.json` 完整调度；
2. `blocked.json` 触发 `E_FLOW_CONTROL_BLOCKED` 且确认未落盘；
3. `reset.json` 流进入 `RESET` 且发送字节为 0；
4. 错误码扫描：`E_WINDOW_OVERFLOW`、`E_INVALID_TRANSITION`、`E_FRAME_INVALID`、`E_CONFLICTING_SAMPLE`；
5. 在首轮意图落盘后自发送 `SIGKILL`，重启并输出恢复报告与可复算快照。

默认产物在 `artifacts/demo/`。可以用 `DEMO_OUT=/path ./scripts/demo.sh` 改变输出位置。

## 测试

```bash
go test ./...
go test -race ./...
go vet ./...
```

测试覆盖：非法帧/流标识/时间戳/负增量、窗口阻塞上下文、窗口溢出、六态迁移、RESET 抑制调度、固定点归一化、采样审计、完整/按流分批逐版本一致、并发重复提交只推进一次、检查点损坏和两种崩溃恢复分支。

## 当前假设

- 时间戳在同一样本内按 `sequence` 非递减；同一时间戳视为一个调度批次，批次内帧全部应用后才执行该连接的调度轮次。
- `HEADERS` 描述对端打开/关闭流；`DATA` 是待发送的本地出站需求，`END_STREAM` 在最后一个字节实际发送后生效。
- 同一连接严格优先级；同级采用容量受限的整数比例分配，发送顺序固定为“字节数降序、权重降序、流 ID 升序”。
- 服务按单进程、单写者模型设计；启动恢复假定上一进程已经终止。
