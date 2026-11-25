# 基础设施配置一致性数值审计 — 技术说明

## 1. 运行环境与依赖

- Go：本机 `go version` 为 go1.27.4（`go.mod` 声明 `go 1.22`，满足“1.21 或更高”）。
- SQLite 驱动：`modernc.org/sqlite v1.34.5`，纯 Go，无需 CGO、无需容器，版本写入 `go.mod`。
- 标准库外无其他运行期依赖；样本由固定种子 `20261126` 在本地确定性生成，不访问外部网络。
- 工作目录、SQLite 文件与样本目录均由程序自行创建（默认 `-dir data`）。

## 2. 目录结构

```
cmd/configaudit/      命令行入口：generate / audit / serve / recover / crash-demo
internal/fixedpoint/  int64 定点运算、换算链求值、big.Rat 高精度对照、错误码
internal/units/       单位注册表与两代来源描述解析归一化
internal/model/       配置项、来源通道、节点模型与字段校验
internal/sample/      固定种子样本生成器（JSONL 分片 + 高精度参考）
internal/parser/      JSONL 加载、行号/字段校验、checksum 校验
internal/report/      审计结果数据结构（解耦 audit 与 store 的 import 环）
internal/audit/       审计引擎、优先级叠加、遮蔽/冲突、参考比对、服务编排
internal/store/       SQLite 持久化、意图日志、快照、崩溃恢复
internal/httpapi/     本地 HTTP 测试端点
```

## 3. 定点与误差模型

- 内部表示：`Value{Mantissa int64, Scale int}`，允许 scale 集合 `{0,3,6,9}`。
- 全程 int64 运算；每次乘/加/重缩放都显式做越界检查，越界返回 `E_NUMERIC_OVERFLOW` 并标注链步 `step`。
- 舍入统一采用 round-half-away-from-zero；单次除法/重缩放引入至多 0.5 ulp 误差。
- 误差传播：
  - 精确整数乘法把已有 ulp 误差同比放大，不产生新舍入；
  - 除法把历史误差 `acc/divisor` 折到输出尺度，并新增 0.5 ulp；
  - 分数部分用 `big.Rat` 保留，整 ulp 部分用 int64 累计，合成整链绝对误差上界 `BoundRat`。
- 同时用 `big.Rat` 维护精确真值 `ExactFinal`，可复核实际误差 `ActualAbsError <= BoundRat`。
- 参考比对输出绝对偏差、相对偏差；判定上界取“链合成上界”与“声明上界”中更紧者（链上界为 0 时回退到声明值）。突破返回 `E_ERROR_BOUND_EXCEEDED`，附 item 标识、实测偏差与上界。

## 4. 两代来源格式

- 新版 v2：结构化 `{value, unit, priority, ...}`，unit 走枚举表。
- 旧版 v1：带后缀文本，如 `512Mi`、`1.5 GiB`、`250m`，容忍一个空白分隔。
- 二者归一化为同一 `units.Record`，输出原始文本、归一化值、舍入位与累计舍入误差。
- IEC 二进制（Ki/Mi/Gi/Ti）与 SI 十进制（k/M/G）后缀严格区分，裸字母保留给 SI，避免同一文本两代解析到不同单位；无法识别后缀返回 `E_UNIT_INVALID`。

## 5. 多来源叠加与优先级

- 归一化后按“优先级降序、通道顺序升序”确定性排序。
- 同优先级且归一化值不同 => `E_PRIORITY_CONFLICT`。
- 标记 `exclusive` 的高优先级通道命中后，后续通道进入遮蔽列表并记录原因；有效结果按确定次序累加，输出每来源贡献、叠加次序与累计误差。
- 比对按 节点 × 配置项 × 来源通道 × 换算链 完整遍历，并校验组合总数与参考条目数一致，禁止抽样。

## 6. 快照、增量与并发

- 快照携带单调版本、生成时间、checksum（canonical JSON 的 sha256）。版本非递增或摘要不符返回 `E_SNAPSHOT_INVALID`，旧快照保持可查。
- 增量片段携带基础版本；基础版本非当前版本返回 `E_STALE_BASE` 且不改变已提交状态；成功后以新版本完整落盘，读者只会看到完整提交版本。
- 单写者：写操作经 `writeMu` 串行；只读查询不加写锁，可并发。
- 幂等：同一 request_id 只执行一次，并发重复请求在 in-flight 表上汇合复用结果；同 id 不同内容哈希返回 `E_CONFLICTING_REQUEST`。

## 7. 持久化与崩溃恢复

- 表：`snapshots`、`audits`、`error_records`、`intents`；WAL + `synchronous=FULL`。
- 审计/快照切换先写 `intents(status=pending)` 再提交事务，提交完成后回写 committed。
- 启动或调用 `/v1/recover` 时扫描未完成意图：未完成审计标记 aborted，未完成快照意图在无完整快照时中止；回滚到最后一个完整快照版本。恢复报告只包含磁盘事实（恢复版本、被中止审计/意图 id）。

## 8. 错误码与 HTTP 映射

| 错误码 | 触发场景 | HTTP |
| --- | --- | --- |
| E_SPEC_INVALID | 键路径/scale/换算系数/摘要非法，含字段与行号 | 422 |
| E_UNIT_INVALID | 无法识别的单位后缀/枚举 | 422 |
| E_NUMERIC_OVERFLOW | int64 定点越界，含链步 | 422 |
| E_PRIORITY_CONFLICT | 同优先级取值冲突 | 422 |
| E_ERROR_BOUND_EXCEEDED | 偏差突破上界，含 item/偏差/上界 | 422 |
| E_SNAPSHOT_INVALID | 版本非单调或摘要不符 | 422 |
| E_STALE_BASE | 增量基础版本过期 | 422 |
| E_CONFLICTING_REQUEST | 同 request_id 内容不一致 | 409 |
| E_NOT_FOUND | 快照/请求不存在 | 404 |

## 9. 常用命令

```bash
export GOPROXY=https://goproxy.cn,direct   # 仅本机代理受限场景需要
go test ./...                              # 全量单元/HTTP 测试
go vet ./...

go run ./cmd/configaudit generate -dir data
go run ./cmd/configaudit audit    -dir data
go run ./cmd/configaudit crash-demo -dir run -db demo.db   # 强制终止+恢复演示
go run ./cmd/configaudit serve    -dir data -addr 127.0.0.1:8080
go run ./cmd/configaudit recover  -dir data
```

HTTP 端点：`GET /healthz`、`POST /v1/audits`、`POST /v1/audits/incremental`、
`GET /v1/snapshots/latest`、`GET /v1/snapshots/{version}`、`GET /v1/audits/{requestID}`、`POST /v1/recover`。

## 10. 本地验收结果（实测）

- 固定种子样本：2 节点、4 配置项、28 个 节点×键×通道 组合，全部与 big.Rat 参考逐项比对通过。
- 单位换算：除法链（如 `div 3 -> mul 7`）实测误差为正且不超过合成上界；精确乘法上界为 0。
- 多来源：node-a 的 exclusive `script` 通道稳定遮蔽其余通道并输出明细；node-b 按优先级叠加。
- 快照/增量：重复提交幂等返回同版本；冲突返回 409；过期基线返回 `E_STALE_BASE`；成功增量推进 v1->v2，旧版本仍可查。
- 并发：8 个并发相同 request 汇合到同一版本；8 个并发只读快照均 200。
- 强制终止：子进程写意图后 `os.Exit(9)` 模拟 SIGKILL，重启恢复报告 `recovered_version=1`、中止未完成审计，完整快照保持可用。
