# 验证记录

环境：WSL2 Ubuntu 26.04，`go version go1.22.12 linux/amd64`，构建与测试均使用
`CGO_ENABLED=0`（纯 Go，SQLite 驱动为 `modernc.org/sqlite v1.34.5`），未使用容器。

## 构建与静态检查

- `CGO_ENABLED=0 go build ./...` → BUILD_OK
- `CGO_ENABLED=0 go vet ./...` → VET_OK
- `CGO_ENABLED=0 go test ./... -count=1` → 全部包 `ok`，共 34 个测试函数。

包覆盖率（`go test -cover`）：

| 包 | 覆盖率 |
| --- | --- |
| internal/timeconv | 84.2% |
| internal/scheduler | 83.1% |
| internal/correlate | 80.0% |
| internal/bincode | 79.6% |
| internal/snapshot | 76.4% |
| internal/pipeline | 76.3% |
| internal/normalizer | 75.0% |

## 业务要求对应测试

1. 三格式解析与归一化：`bincode` 往返（v1/v2 字段布局、偏移、CRC）、`normalizer`
   JSONL/文本块/未知事件降级（保留载荷，标记 `E_UNKNOWN_EVENT`）。
2. 时间戳互操作：`timeconv` 验证 FILETIME 锚点与 RFC3339/epoch 一致、重复换算确定性、
   上下界与 uint64 最大值溢出返回 `E_TIME_OVERFLOW` 并保留原始值/偏移。
3. 会话与进程谱系：`correlate` 验证正常父子边、孤儿 `E_ORPHAN_REF` 挂虚拟根、
   PPID=0 根进程不算孤儿、同 PID 跨会话不串连、边可定位来源格式与偏移。
4. 版本化快照：`snapshot` 验证版本递增与连续链加载、不兼容格式版本整体拒绝
   （`E_SNAPSHOT_VERSION_INCOMPAT`，无部分加载）、版本链跳号回退、同版本不同内容哈希
   识别、相同内容幂等提交。
5. 五类畸形：端到端 `pipeline` 断言五类样本各返回一个稳定业务码且带跳过范围，
   畸形不中断其他源、不污染快照。
6. 并发调度与资源控制：`scheduler` 验证并发上限、等待队列（不拒绝）、事件预算
   `E_EVENT_BUDGET` 保留已解析部分、按源路径×业务码聚合、`-race` 无数据竞争。
7. 报告与视图：`pipeline` 验证时间轴稳定排序、快照版本号来自实际提交、`view`
   会话过滤与单事件多格式来源对照。
8. 确定性演示与版本/策略切换：`pipeline` 验证跨目录重复运行报告字节级一致、
   v1/v2 切换过滤对应二进制格式、continue/abort 正常事件结论一致而错误处理摘要不同。

## 一次实际演示的关键数字

- 快照版本 `1`，依赖格式 `binary/v1`、`binary/v2`、`jsonl/v1`、`text/v1`。
- 统一时间轴 20 条事件（正常 19 条，1 条 FILETIME 溢出事件保留且时间为 null）。
- 进程谱系边 10 条，其中孤儿边 6 条；S-B 内 200→201 正常父子，300 孤儿，S-C 独立会话不串连。
- 错误码集合恰为五类：`E_JSON_FIELD_TYPE`、`E_LENGTH_OVERRUN`、`E_TIME_OVERFLOW`、
  `E_TRUNCATED_RECORD`、`E_UNKNOWN_BINARY_VERSION`。
- 两次独立目录运行 `cmp report.json` → 字节级一致。
- `-budget 2` 时多个源产出 `E_EVENT_BUDGET` 且保留前 2 条已解析事件。
- 多格式对照：会话 S-A、PID 100 的 process_create 在 binary/jsonl/text 三来源各有一条记录。

## 复现命令

```bash
GOPROXY=https://goproxy.cn,direct go mod tidy
CGO_ENABLED=0 go build ./...
CGO_ENABLED=0 go vet ./...
CGO_ENABLED=0 go test ./... -count=1
go run ./cmd/fwsample -out .work/samples
go run ./cmd/fwbench demo -workdir .work/run -concurrency 3 -strategy continue
go run ./cmd/fwbench view -report .work/run/report.json
go run ./cmd/fwbench snapshot -db .work/run/forensic.sqlite
```
