# casengine — 企业 SSO 一次性票据（ST）校验引擎

模拟 CAS 中心认证服务签发一次性服务票据，多个受保护服务（SP）凭 ST 向校验端换取身份断言。
引擎在并发票据校验、异常注入与强制终止重启下保证同一票据仅成功兑换一次，并输出可交互的
延迟统计、版本化快照与字节级可重复的 JSON 报告。

- 纯 Go 实现，唯一第三方依赖为纯 Go SQLite 驱动 `modernc.org/sqlite`（免 CGO）。
- 无需外部数据库、容器、虚拟机或网络账号；样本全部由固定种子确定性生成。
- 数据库与报告均写入 `-workdir` 指定的本地可写目录。

## 快速开始

```bash
go version          # 本机 Go 1.27；受 SQLite 驱动传递依赖约束，go.mod 指令为 1.26
go build -o casengine ./cmd/casengine

./casengine init   -workdir ./work     # 生成确定性租户/SP/用户/密钥/场景脚本
./casengine demo   -workdir ./work     # 全流程演示并导出 reports/demo-report.json
./casengine inject -workdir ./work     # 五类异常注入断言
./casengine replay concurrent-one-time -workdir ./work
./casengine snapshot-gap -workdir ./work
./casengine status tenant-acme -workdir ./work
./casengine stats  -workdir ./work
./casengine crash-recovery -workdir ./work
./casengine serve :8080                # HTTP 服务（真实单调时钟）
```

Windows 终端把 `./casengine` 换成 `casengine.exe`，路径分隔符换成 `\` 即可；
`CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build` 可直接交叉编译出单文件 exe。

## 业务码

| 代码 | 含义 |
| --- | --- |
| `E_OK` | 票据合法且完成一次性兑换 |
| `E_TICKET_SIGNATURE` | HMAC 签名不合法 |
| `E_TICKET_AUDIENCE` | 出示 SP 与票据目标 SP 不匹配 |
| `E_TICKET_EXPIRED` | 票据已过期 |
| `E_TICKET_CONSUMED` | 票据已被兑换（并发竞争失败方） |
| `E_TICKET_CONTENTION` | 超出并发等待阈值 |
| `E_SNAPSHOT_VERSION` | 快照链跳号或内容摘要不符 |
| `E_TICKET_UNKNOWN` / `E_INTERNAL` | 票据不存在 / 存储内部错误 |

## 关键设计

- 票据线格式 `base64url(JSON).base64url(HMAC-SHA256)`，校验顺序：签名 → 受众 → 过期 → 兑换。
- 兑换的状态迁移、兑换记录、快照版本在同一个 `BEGIN IMMEDIATE` SQLite 事务内完成。
- 延迟用单调时钟差值采样；线上为真实单调时钟，回放为按序号确定的亚毫秒虚拟延迟。
- 每租户维护递增快照链与内容 SHA-256，跳号或整体替换都可识别并回退到最近连续版本。
- 确定性回放固定输入、并发赢家与时间，因此同输入重复运行报告字节级一致。

## 文档

- `docs/DESIGN.md`：架构、模块职责、并发/快照/时钟/恢复设计。
- `docs/VERIFICATION.md`：实际命令输出、测试与字节级一致性验证结果。
