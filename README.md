# NonceGuard

内部代理网关的请求签名、时效窗口与 Nonce 去重裁决引擎。它不是配额限流器；每个请求只回答：签名是否有效、时间戳是否在容忍区间、Nonce 是否在同一客户端的时效窗口内首次出现，以及结论绑定哪个版本化窗口快照。

应用为纯 Go 实现，SQLite 驱动使用 `modernc.org/sqlite`，无需 CGO 或外部数据库；密钥、样本和回环对端均在本机生成/运行。

## 快速开始

```bash
go version                 # 开发验证使用 go1.27.4；go.mod 目标为 go 1.21
GOPROXY=https://goproxy.cn,direct go mod tidy
go test ./... -count=1

# 一键重复演示：固定样本、五类异常、kill -9、重启恢复和续跑
./scripts/demo.sh

# 如需保留演示服务用于点击 UI
./scripts/demo.sh --stay
```

手动启动交互式应用：

```bash
go run ./cmd/gate \
  --addr 127.0.0.1:18090 \
  --db data/nonceguard.db \
  --peer-addr 127.0.0.1:18091 \
  --fixed-reference-ms 1795507200000

# 另一个终端：独立回环代理对端（gate 也已在 /peer/* 挂载同款对端）
go run ./cmd/peer -addr 127.0.0.1:18091
```

打开 <http://127.0.0.1:18090/>。

`--fixed-reference-ms` 默认 `0`，表示使用系统时钟；演示脚本会显式传入固定值。固定样本批次也自带 `reference_ms`，HTTP 调用可用该参数做可复核裁决，单调 high-water 会防止参考时刻回退。

## 验收演示内容

`scripts/demo.sh` 自动选择空闲端口，并使用固定参考时刻 `1795507200000`：

1. 十条固定样本覆盖 `OK`、`E_SIGNATURE_MISMATCH`、`E_TIMESTAMP_EARLY`、`E_TIMESTAMP_LATE`、`E_NONCE_REPLAYED`、幂等复用和 `E_CONFLICTING_REQUEST`。
2. 快速模式预检前 6 条，输出采样率 `0.6`；6 条可核对结论全部匹配，4 条明确为 `E_FAST_NOT_PRECHECKED`。
3. 查询配置 v1 的 8 条唯一结论和 4 条 Nonce 占用。
4. 非法 Nonce/权重溢出返回 `E_REQUEST_INVALID`，整批不入库。
5. 配置 v2 原子替换；重复 v2 返回 `E_STALE_SNAPSHOT`，v3 跳号返回 `E_CONFIG_VERSION_GAP`。
6. 真实回环连接产生 `E_PEER_TIMEOUT`、`E_PEER_REFUSED`、`E_PEER_LATE_ACK`。
7. `fault=batch_interrupt` 返回 `E_BATCH_INTERRUPTED`；120 条慢批次处理中执行 `kill -9`，重启后扫描到 120 个 open intent，并可 resume 全部 120 条。

实际输出保存在：

- `docs/demo-output.txt`
- `docs/test-output.txt`
- `docs/VERIFICATION.md`

## 主要 HTTP 接口

成功响应包络为 `{"ok":true,"data":...}`；失败响应在顶层包含 `ok:false`、`code`、`message`、`context`。

```bash
# 固定样本：canonical | valid | invalid | recycle | config_v2
curl 'http://127.0.0.1:18090/v1/sample?scenario=canonical'

# 批次裁决
curl -X POST 'http://127.0.0.1:18090/v1/batches?mode=full' \
  -H 'Content-Type: application/json' --data-binary @batch.json
curl -X POST 'http://127.0.0.1:18090/v1/batches?mode=fast&precheck_limit=6' \
  -H 'Content-Type: application/json' --data-binary @batch.json
curl -X POST 'http://127.0.0.1:18090/v1/batches?mode=compare&precheck_limit=6' \
  -H 'Content-Type: application/json' --data-binary @batch.json

# 配置
curl 'http://127.0.0.1:18090/v1/config/current'
curl -X POST 'http://127.0.0.1:18090/v1/config/snapshots?template=v2'
curl -X POST 'http://127.0.0.1:18090/v1/config/snapshots?fault=version_gap'

# 按配置版本查询
curl 'http://127.0.0.1:18090/v1/decisions?config_version=1'
curl 'http://127.0.0.1:18090/v1/nonces?config_version=1'
curl 'http://127.0.0.1:18090/v1/batches/demo-canonical-batch'

# 恢复与手工回收
curl 'http://127.0.0.1:18090/v1/recovery'
curl -X POST 'http://127.0.0.1:18090/v1/recovery/run'
curl -X POST 'http://127.0.0.1:18090/v1/nonces/reclaim?config_version=1&as_of_ms=1795507800000'
```

## 异常注入参数

这些异常只能通过运行时参数和真实连接行为触发，不需要修改源码：

```bash
# peer 延迟 1500ms，客户端 250ms 超时
curl -X POST 'http://127.0.0.1:18090/v1/batches?fault=peer_timeout&peer_addr=127.0.0.1:18091' ...

# 连接一个未监听端口
curl -X POST 'http://127.0.0.1:18090/v1/batches?fault=peer_refused&peer_addr=127.0.0.1:28799' ...

# peer 800ms 后 ACK，超过 300ms ACK 期限但未超过 3s HTTP 超时
curl -X POST 'http://127.0.0.1:18090/v1/batches?fault=peer_late_ack&peer_timeout_ms=3000' ...

# 配置 current+2
curl -X POST 'http://127.0.0.1:18090/v1/config/snapshots?fault=version_gap'

# 意图落盘后、裁决事务前中断
curl -X POST 'http://127.0.0.1:18090/v1/batches?fault=batch_interrupt' ...
```

## 裁决语义

- 请求字段：`request_id`、毫秒 `timestamp_ms`、`route_key`、`client_id`、`nonce`、64 位小写十六进制 `signature_digest`、正整数 `weight`。
- Nonce 规则：长度 16–128，字符集 `[A-Za-z0-9_-]`。
- 权重：必须是可解析的正 `int64`，批次总权重也做溢出检查。
- 签名：固定种子派生本地密钥，对配置声明的规范化字段做长度前缀编码和 HMAC-SHA256。
- 偏移：`offset = timestamp_ms - reference_ms`，闭区间 `[min_offset_ms, max_offset_ms]`。
- Nonce 窗口：`[first_seen_ms, first_seen_ms + nonce_window_ms)`，左闭右开；到达右端点即可回收。
- 亚毫秒规则：所有对外时刻和时长均为整数毫秒，非负亚毫秒差值向零/floor 截断，不产生小数提前到期。
- 回收依据：持久化 `first_seen_ms` 加当前绑定快照的窗口长度，以及单调参考高水位，而不是调用方时钟。

## 代码布局

```text
cmd/gate              HTTP/UI 服务入口
cmd/peer              独立回环异常对端
internal/model        领域类型、严格解析、错误码、边界声明
internal/canonical    固定密钥、规范化和摘要
internal/sample       固定种子确定性样本
internal/store        SQLite schema、WAL、恢复扫描
internal/engine       单写者裁决、快速模式、并发幂等、查询
internal/httpapi      HTTP API 与嵌入 UI
internal/peer         回环对端 server/client
web/index.html        零外链交互式控制台
scripts/demo.sh       可重复验收演示
docs/ARCHITECTURE.md  事务、并发、恢复和需求追踪
```
