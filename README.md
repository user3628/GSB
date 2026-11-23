# 审计事件关联与取证工作台 (forensicworkbench)

面向安全运营本地取证流程的纯 Go 工作台：解析三种互不兼容的 Windows 审计源格式，
规范化到统一时间轴，按会话与进程谱系关联，维护可版本识别的解析/关联快照，并对
畸形输入返回稳定、可追溯的业务码。无 CGO、无容器、无网络目标，所有读写限定在
本地工作目录。

## 技术约束与依赖

- Go 1.22+，仅使用标准库 + 纯 Go SQLite 驱动 `modernc.org/sqlite`（`CGO_ENABLED=0`）。
- 样本由 `internal/samplegen` 以固定字面量确定性生成，不依赖真实账号或时钟。
- 模块代理若不可达，可使用镜像：`GOPROXY=https://goproxy.cn,direct go mod tidy`。

## 目录结构

- `internal/codes`：稳定业务码、源/格式版本常量、统一中间事件模型。
- `internal/bincode`：自研紧凑二进制日志（固定版本头 + 变长记录 + 尾部 CRC32 校验块），v1/v2 字段布局。
- `internal/timeconv`：epoch 毫秒 / RFC3339 / Windows FILETIME(1601,100ns) → UTC 纳秒 int64，含溢出检查。
- `internal/normalizer`：三格式解析与归一化，未知事件降级保留、时间溢出保留原始值。
- `internal/correlate`：按会话隔离的进程谱系树、虚拟根、孤儿子进程 E_ORPHAN_REF、边来源定位。
- `internal/snapshot`：SQLite 版本化快照、内容哈希、连续版本链与格式版本兼容加载。
- `internal/scheduler`：并发多源会话、信号量 + 等待队列、单会话事件预算、按源路径×业务码聚合。
- `internal/report`：确定性 JSON 报告与稳定排序的统一时间轴。
- `internal/view`：会话过滤与单事件多格式来源对照（一次性查询 + 交互式 REPL）。
- `internal/samplegen`：确定性样本与五类畸形样本生成。
- `internal/pipeline`：端到端编排与 continue/abort 畸形处理策略。
- `cmd/fwbench`：演示/视图/快照 CLI；`cmd/fwsample`：独立样本生成器。

## 二进制线路格式

- 头部（18 字节）：magic `FWRK`(4) + version(1) + created_at_nano(8) + record_count(4) + reserved(1)。
- 记录：4 字节大端长度前缀 + 记录体（type、time 编码、变长字符串、PID；v2 额外含 PPID 与 module_load）。
- 尾部（16 字节）：magic `FWTB`(4) + body_len(4) + record_count(4) + CRC32-IEEE(4)。

## 稳定业务码

| 代码 | 触发场景 |
| --- | --- |
| `E_UNKNOWN_EVENT` | 未知事件类型枚举，降级保留原始载荷 |
| `E_TIME_OVERFLOW` | 时间戳换算 int64 溢出/精度问题，保留原始值与偏移 |
| `E_ORPHAN_REF` | 父进程创建事件缺失，挂接会话虚拟根 |
| `E_SNAPSHOT_VERSION_INCOMPAT` | 快照格式版本不兼容 / 版本链跳号 / 同版本内容漂移 |
| `E_TRUNCATED_RECORD` | 二进制记录被截断 |
| `E_JSON_FIELD_TYPE` | JSON 行字段原始类型错误 |
| `E_LENGTH_OVERRUN` | 文本块长度前缀超出剩余字节 |
| `E_UNKNOWN_BINARY_VERSION` | 二进制版本号不受支持 |
| `E_EVENT_BUDGET` | 单会话事件数超限，保留已解析部分 |
| `E_CHECKSUM` / `E_BAD_HEADER` | 尾部校验失败 / 头部 magic 错误 |

## 快速开始

```bash
go mod tidy
CGO_ENABLED=0 go build ./...
CGO_ENABLED=0 go test ./...

# 一键确定性演示（生成样本 -> 并发解析 -> 关联 -> 快照 -> 报告）
go run ./cmd/fwbench demo -workdir .work/run -concurrency 3 -strategy continue

# 仅生成确定性样本集
go run ./cmd/fwsample -out .work/samples

# 视图：会话过滤 / 单事件多格式对照 / 交互式 REPL
go run ./cmd/fwbench view -report .work/run/report.json -session S-B
go run ./cmd/fwbench view -report .work/run/report.json -event evt_xxxx
go run ./cmd/fwbench view -report .work/run/report.json

# 校验快照版本链
go run ./cmd/fwbench snapshot -db .work/run/forensic.sqlite
```

CLI 参数：

- `demo -version v1|v2|all`：选择目标二进制格式版本（同步过滤对应版本的畸形源）。
- `demo -strategy continue|abort`：畸形处理策略；两种策略的正常事件结论一致，差异体现在
  报告的 `malformed_strategy` 与 `abandoned_error_records`。
- `demo -budget N`：单会话事件上限，超限返回 `E_EVENT_BUDGET` 并保留已解析部分。
- `demo -concurrency N`：并发解析会话数，超出容量的源进入等待队列而非拒绝。

## 确定性保证

- 样本、事件 ID（内容哈希）、所有切片在序列化前显式排序；报告与快照不嵌入机器相关绝对路径。
- 报告中不记录运行时观测的非确定值（并发度取配置值），因此相同输入重复运行报告字节级一致。
- 时间换算为纯函数，同一事件重复换算得到字节级一致的 UTC 纳秒整数。

## 五类畸形样本与稳定码

`malformed_truncated.bin→E_TRUNCATED_RECORD`、`malformed_json_type.jsonl→E_JSON_FIELD_TYPE`、
`malformed_text_len.txt→E_LENGTH_OVERRUN`、`malformed_filetime.bin→E_TIME_OVERFLOW`、
`malformed_unknown_version.bin→E_UNKNOWN_BINARY_VERSION`。每个错误记录携带源路径、业务码与
跳过字节范围；畸形源不中断其他源，也不污染已提交快照。

## 验证结果

见 `VERIFICATION.md`（本机 `go build`、`go vet`、`go test`、重复运行字节一致性与 CLI 冒烟记录）。
