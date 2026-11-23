在一台没有历史代码的机器上，需要构建一个面向本地模型服务集群的推理网关路由与诊断工作台。网关依据带版本号的模型路由表把推理请求分发到本地模型后端测试端点，路由表需与外部推理平台互换数据，因此必须支持两种可互换的持久化格式：紧凑二进制格式（含魔数、格式版本字节、大小端标记、每条记录长度前缀、CRC32 校验与可选字段存在位图）与分片 JSON 行格式（每行含模型标识、后端端点、显存预算、并发上限、工具链版本与保留期）。历史版本存在字段缺省与精度差异，导入时需按兼容矩阵裁决接受、降级接受或拒绝，并在新旧格式间归一化时间与容量语义。所有数据来自本地确定性样本与本地 HTTP 测试服务，运维人员通过本地接口提交路由表、触发请求回放并查看诊断视图。

任务环境与准备：
安装 Go 1.22 或以上版本（Windows 官方安装包），确认 go version 可执行并启用本地模块缓存。
准备本地确定性样本：包含模型清单、路由表二进制与 JSON 行各一份、若干含缺省字段的旧版本记录，全部放在工作区 samples 目录。
启动本地 HTTP 测试服务模拟模型后端，提供确定性延迟与结果摘要，并可通过参数模拟连接拒绝与超时；服务端口从环境变量读取。
使用 SQLite 作为持久化存储，通过 Go 的纯 Go 驱动或系统自带库接入，数据库文件放在工作区 data 目录。
提供可重复运行的演示脚本，使用固定样本完成导入、导出、回放与恢复演练，所有命令在 Windows 命令行下执行。

业务要求：
1. 实现两种持久化格式的编解码：紧凑二进制必须含魔数、格式版本字节、大小端标记、每条记录长度前缀、CRC32 校验与可选字段存在位图；分片 JSON 行每行含模型标识、后端端点、显存预算、并发上限、工具链版本与保留期；任一记录损坏或长度前缀与实际不符返回 E_RECORD_CORRUPT 并指明偏移。
2. 实现格式协商与版本兼容裁决：导入前检测输入格式与格式版本，按显式声明的兼容矩阵给出接受、降级接受或拒绝三种结论；旧版本缺工具链版本或显存预算精度较低时降级接受，按声明默认值补齐并记录降级明细；高版本含未知必填字段返回 E_FORMAT_UNSUPPORTED；两种格式互转后语义等价的往返校验必须逐字段通过。
3. 实现时间与容量归一化：二进制毫秒时间戳与 JSON 行微秒时间戳须归一化为同一内部毫秒表示，按调用方声明的截断规则处理亚毫秒部分并输出每条记录的舍入位与累计舍入误差；JSON 相对保留期须依据记录自带基准时间换算为绝对过期时刻；显存预算从 Mi/Gi 量纲归一化为字节整数并做溢出检查，越界返回 E_UNIT_OVERFLOW。
4. 实现导入冲突解决与批次原子生效：导入记录与现有路由同模型标识时，按工具链版本优先级、后续端点可用性与最后写入时间打破平局的确定规则解决，结局分为覆盖、跳过与拒绝三类并写入导入报告；整批导入须原子生效，任一条被拒绝则整批回滚并保持旧路由表完全可用。
5. 实现基于路由表的请求转发与请求回放：按模型标识把推理请求路由到对应本地后端测试端点并回传结果摘要，后端不可达时返回 E_BACKEND_UNAVAILABLE 并附端点标识；回放使用固定种子生成的确定性请求序列，同一路由版本与样本重复回放必须得到逐条一致的路由决策与结果摘要。
6. 实现快照整体替换与版本识别：路由表快照文件携带格式版本、快照序号与生成时间，加载时须校验版本兼容性与序号单调，加载过程在内存路由索引与 SQLite 表之间原子切换，切换成功后旧快照资源与派生统计才可释放；任何校验失败须保持旧快照完全可用并返回 E_SNAPSHOT_INVALID。
7. 实现单写者提交与多读者查询的并发一致及强制终止恢复：路由导入、快照切换与转发推进按单写者串行执行，多个只读诊断查询可并发读取某一完整提交版本；写操作先写 SQLite 意图记录再提交事务，进程被强制终止后重启须扫描未完成意图把路由表回滚到最后一个完整版本，恢复报告只基于已落盘事实。
8. 提供本地 HTTP 接口与诊断视图：接口覆盖路由表导入、二进制与 JSON 行导出、请求回放、按模型查询路由决策与命中统计、快照切换与恢复报告查询，失败响应统一为含业务码与上下文的 JSON，至少覆盖 E_RECORD_CORRUPT、E_FORMAT_UNSUPPORTED、E_SNAPSHOT_INVALID、E_CONFLICTING_IMPORT、E_BACKEND_UNAVAILABLE。

执行约束：不使用 Docker 或其他容器环境；使用本地确定性样本验证，不访问未授权目标。按上述任务意图交付相应代码、测试或技术说明，记录实际验证结果。

验收：
用本地样本验证两种格式互转、降级与拒绝裁决、冲突导入回滚及快照切换后路由与命中统计一致，附运行结果。

---

# 推理网关路由与诊断工作台 — 技术说明

## 项目结构

```
cmd/gateway/      网关主程序（HTTP 接口 + 诊断视图）
cmd/backend/      本地 HTTP 测试服务（模拟模型后端，确定性延迟与摘要）
cmd/gensamples/   确定性样本生成器（输出到 samples/）
internal/format/  两种持久化格式编解码、兼容矩阵裁决、时间/容量归一化
internal/router/  路由核心：单写者提交、冲突解决、快照切换、转发与回放
internal/store/   SQLite 持久化（纯 Go 驱动 modernc.org/sqlite）、写意图与恢复
internal/api/     本地 HTTP 接口与统一错误响应
internal/gwerr/   业务错误码定义
scripts/demo.sh   可重复演示脚本（Linux/WSL）
scripts/demo.bat  可重复演示脚本（Windows 命令行）
samples/          确定性样本（模型清单、两种格式路由表、旧版/损坏/溢出/冲突/快照样本）
data/             SQLite 数据库文件目录
```

## 运行

```bat
go build ./...
go test ./...
scripts\demo.bat    :: Windows 命令行完整演练
bash scripts/demo.sh  :: Linux/WSL 完整演练
```

环境变量：`GATEWAY_PORT`（默认 8080）、`DATA_DIR`（默认 data）、`BACKEND_PORT`、`BACKEND_MODE`（ok/refuse/timeout）、`GATEWAY_ENABLE_DEBUG_CRASH=1`（恢复演练用）。

## 格式设计

### 紧凑二进制（grt-bin）
- 头部：魔数 `GRTB`(4B) + 格式版本字节 + 大小端标记（0x01=LE/0x00=BE）+ 记录数 u32
- 每条记录：长度前缀 u32（含 CRC）+ payload + CRC32(IEEE) u32
- payload：可选字段存在位图 u16 + model_id/endpoint（u16 长度前缀字符串）+ vram_bytes u64 + concurrency u32 + 位图门控的可选字段（toolchain、base_time_ms、retention_ms、last_write_ms）
- v1 旧版：显存为低精度 float32 GiB，无工具链字段
- 任一记录损坏或长度前缀与实际不符 → `E_RECORD_CORRUPT` 并指明字节偏移

### 分片 JSON 行（grt-jsonl）
- 首行 `_meta` 声明格式/版本/分片；其余每行含 model_id、endpoint、vram_budget（"8192Mi"/"8Gi"）、concurrency_cap、toolchain_version、base_time_us、retention_us、last_write_us
- 微秒时间戳；保留期为相对量，按记录自带 base_time 换算绝对过期时刻

## 兼容矩阵（显式声明）

| 格式 | v1 | v2 | v3+ |
|---|---|---|---|
| grt-bin | 降级接受（补默认工具链、低精度 GiB 归一化） | 接受 | 拒绝 `E_FORMAT_UNSUPPORTED` |
| grt-jsonl | 降级接受（补默认工具链/并发上限） | 接受 | 拒绝（含未知必填字段） |

降级默认值：`toolchain=tc-0.0.0-legacy`、`concurrency_cap=4`，全部降级明细写入导入报告。

## 归一化语义
- 时间：二进制毫秒 / JSONL 微秒统一为内部毫秒；截断规则由调用方声明（`?trunc=truncate|round-half`）；逐条输出舍入位与累计舍入误差（微秒）
- 保留期：`expires_at_ms = base_time_ms + retention_ms`
- 显存：Mi/Gi 归一化为字节整数，`bits.Mul64` 溢出检查，越界 `E_UNIT_OVERFLOW`

## 冲突解决与原子生效
同 model_id 依次按：工具链版本优先级（tc-X.Y.Z 元组比较）→ 后续端点可用性（/health 探测）→ 最后写入时间 裁决，结局为覆盖/跳过/拒绝；全部平局但内容不同 → 拒绝，整批回滚（`E_CONFLICTING_IMPORT`），旧路由表完全可用。

## 并发与恢复
- 写操作（导入/快照切换）经 `writeMu` 单写者串行；只读查询经 `RWMutex` 并发读取完整提交版本
- 写协议：先独立事务落盘意图记录（pending）→ 数据变更 + 意图完成 + committed_version 同事务提交
- 崩溃重启后扫描 pending 意图标记 aborted 并出具恢复报告（只基于已落盘事实）；路由表保持最后完整版本

## HTTP 接口
`POST /api/import`、`GET /api/export?format=bin|jsonl`、`POST /api/replay`、`POST /api/forward`、`GET /api/route/{model}`、`GET /api/stats`、`POST /api/snapshot`、`GET /api/recovery`、`GET /api/diagnostics`。失败响应统一为 `{"error":{"code","message","context"}}`，覆盖 E_RECORD_CORRUPT / E_FORMAT_UNSUPPORTED / E_UNIT_OVERFLOW / E_CONFLICTING_IMPORT / E_BACKEND_UNAVAILABLE / E_SNAPSHOT_INVALID。

## 实际验证结果（本机 WSL, go1.27.4）

- `go test ./...`：format 9 项 + router 7 项全部通过（编解码往返、大小端、CRC/长度前缀损坏偏移、降级、拒绝、舍入累计、溢出、冲突三类结局、整批回滚、快照校验与切换、强制终止恢复）
- `bash scripts/demo.sh` 全 14 步退出码 0，关键输出：
  - 两种格式互转：bin(369B) ↔ jsonl 逐字段往返校验通过（`TestCrossFormatRoundtrip`）
  - v1 降级导入：`verdict=downgrade_accept`，5 条降级明细，累计舍入误差 1368µs（123+456+789）
  - v3 拒绝：`E_FORMAT_UNSUPPORTED`；CRC 损坏：`E_RECORD_CORRUPT offset=100`；长度前缀不符：`长度前缀 4278190166 与实际剩余 355 不符`
  - 溢出：`E_UNIT_OVERFLOW`（18014398509481984Gi）
  - 冲突：tc-2.0.0 覆盖 tc-1.4.2；平局拒绝 `E_CONFLICTING_IMPORT`，整批回滚后 `llm-omega` 查询返回 `E_MODEL_NOT_FOUND`，表版本保持
  - 回放：seed=42 两次 8 条逐条一致（diff PASS）；快照切换后路由与命中统计一致（派生统计已释放，hits 重置）
  - 后端不可达：`E_BACKEND_UNAVAILABLE` 附 endpoint；强制终止（意图 5 落盘后退出）重启恢复：`回滚 1 条未完成意图，路由表保持最后完整版本 3`
