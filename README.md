# dnswatch DNS 缓存投毒检测工作台

`dnswatch` 是面向递归解析器运维团队的离线 DNS 观测关联工具。它同时解析二进制报文日志、JSONL 摘要和长度前缀文本块，把查询、应答与后续缓存观测归一为同一事务视图，检出四类缓存注入迹象，并在 SQLite 中维护可校验、可回放的版本化快照链。

实现不访问 DNS 服务、账号或其他线上资源，不使用 Docker，不依赖 C 编译器；SQLite 驱动为纯 Go 的 `modernc.org/sqlite`。

## 环境与构建

- Go 1.22 或更高版本。`go.mod` 固定为 `go 1.22`，依赖版本均可由 Go 1.22 构建。
- 首次拉取依赖需要网络；可执行 `GOPROXY=https://goproxy.cn,direct go mod download`。构建完成后运行完全离线。
- SQLite CLI 仅用于人工查看 `.db`，程序本身不需要系统 SQLite 库或服务。
- 终端与文件均为 UTF-8；报告与样本中不包含墙钟时间。

```bash
go build -o artifacts/dnswatch ./cmd/dnswatch
go test ./...
go vet ./...
```

Windows 可直接用 `go run ./cmd/dnswatch ...` 或生成 `dnswatch.exe`，其余参数相同。

## 快速开始

生成确定性样本并扫描干净窗口：

```bash
go run ./cmd/dnswatch generate --out artifacts/samples
go run ./cmd/dnswatch scan \
  --input artifacts/samples/clean \
  --db artifacts/dnswatch.db \
  --report artifacts/report.json \
  --fresh-db --target-parser v2
```

预期摘要：4 个源、10 个事件、4 个事务、4 条命中，分别为 `E_UNSOLICITED`、`E_ADDITIONAL_INJECT`、`E_TTL_MISMATCH`、`E_FLIP_FLOP`。

一键执行生成、两次隔离扫描、字节比较、事务查询、快照回放、v1 不兼容拒绝和五类注入验收：

```bash
go run ./cmd/dnswatch demo --out artifacts/demo
```

指定快照查询事务与回放：

```bash
go run ./cmd/dnswatch query --db artifacts/dnswatch.db --version 1 --txid 0x3003 --parser v2
go run ./cmd/dnswatch replay --db artifacts/dnswatch.db --version 1 --parser v2
go run ./cmd/dnswatch snapshots --db artifacts/dnswatch.db --parser v2
go run ./cmd/dnswatch switch-version --db artifacts/dnswatch.db --version 1 --parser v1
```

最后一条命令会以 `E_SNAPSHOT_VERSION` 拒绝 v1 加载依赖 v2 格式的快照；改用 `--parser v2` 则展示兼容结论。

## 子命令

| 子命令 | 作用 |
| --- | --- |
| `generate` | 生成干净样本与五个注入样本目录 |
| `scan` | 并发解析三源、关联裁决并在单个 SQLite 事务中提交快照 |
| `query` | 按 32 位事务标识查询配对事件与裁决详情 |
| `replay` | 回放指定快照；不兼容时整体拒绝 |
| `switch-version` | 对照 v1/v2 展示快照兼容性或返回 `E_SNAPSHOT_VERSION` |
| `snapshots` | 校验并展示版本链、内容哈希、父链哈希和格式依赖 |
| `verify-injections` | 执行五类外部异常注入断言 |
| `demo` | 可重复演示并断言两次报告字节级一致 |

`scan` 的关键参数：

- `--max-concurrency 3`：并发解析会话上限；超出的源在任务队列中等待。
- `--max-events 10000`：单源事件预算；超限保留已成功事件并记录 `E_EVENT_BUDGET`。`0` 表示不限制。
- `--window-ticks 10`：应答到后续同名缓存观测的关联窗口。
- `--flip-window-ticks 3`：同名地址快速翻转窗口。
- `--snapshot-jump N`：验证参数，故意尝试跳过 N 个版本；事务回滚并返回 `E_SNAPSHOT_VERSION`。
- `--no-commit`：只解析和出报告，不提交快照。
- `--fresh-db`：演示前删除旧数据库；生产快照链中不要使用。

## 业务码

| 业务码 | 含义 |
| --- | --- |
| `E_VER_UNSUPPORTED` | 未知二进制版本头；记录按长度头跳过 |
| `E_NAME_CONFLICT` | 同一事务号跨源对应不同问题名，附两侧源与偏移 |
| `E_UNSOLICITED` | 应答事务没有同事务号、同问题名的已发出查询 |
| `E_ADDITIONAL_INJECT` | 附加区地址记录与问题区名称无关 |
| `E_TTL_MISMATCH` | 应答与后续缓存中同地址 TTL 不一致 |
| `E_FLIP_FLOP` | 同名地址在配置窗口内快速切换 |
| `E_EVENT_BUDGET` | 单源事件预算耗尽，保留此前成功事件 |
| `E_TRUNCATED` | 二进制记录长度或头部超出剩余字节 |
| `E_LENGTH_OVERFLOW` | 文本块长度前缀超出剩余字节 |
| `E_MAGIC_INVALID` | 二进制魔数不合法 |
| `E_PARSE` | 行、块或字段编码错误 |
| `E_SOURCE_OPEN` | 源文件打开失败或输入目录无受支持源 |
| `E_SNAPSHOT_VERSION` | 快照跳号、链替换、版本不兼容或加载被整体拒绝 |

`E_SNAPSHOT_VERSION` 同时覆盖版本不兼容和链完整性失败；报告详情会区分具体原因。

## 文档与验证

- 三源字节级格式：[docs/FORMATS.md](docs/FORMATS.md)
- 架构、关联规则与快照设计：[docs/TECHNICAL.md](docs/TECHNICAL.md)
- 本次实际验证记录：[docs/VALIDATION.md](docs/VALIDATION.md)

代码主要入口：

- `cmd/dnswatch/main.go`：CLI
- `internal/workbench/`：解析、归一、关联、快照、报告
- `internal/sample/generate.go`：确定性样本与注入生成器
- `internal/verify/verify.go`：五类注入验收链路
