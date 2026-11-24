# 验证记录（本地确定性样本）

环境：Go 1.27.4（go.mod 声明 `go 1.22`），linux/amd64 开发验证，
依赖 `modernc.org/sqlite v1.34.5`（纯 Go，无 CGO）。
所有样本均由内置确定性生成器产出，不访问网络或外部目标。

## 1. 自动化测试

`go test ./... -count=1` 全部通过，覆盖：

| 包 | 覆盖点 |
| --- | --- |
| `fixedpoint` | 乘法截断余数、纳秒换算余数、误差上界向上取整、溢出拒绝且账本不写入 |
| `sample` | JSONL/长度前缀解析、源偏移、重复 PID、非法账户、掩码越界、块截断 |
| `detector` | 三类裁决、区间一致性、孤儿虚拟根、句柄归因、乱序顺序无关、账本非空 |
| `snapshot` | 版本递增与重载、整批替换识别、跳号 `E_SNAPSHOT_VERSION` 与回退、进度标记 |
| `session` | 超并发排队、预算终止保留已提交快照、会话隔离、单调时钟采样 |
| `inject` | 五类注入业务码断言、解析/溢出场景快照不推进 |
| `recovery` | 模拟强制终止后已提交不回退、未提交丢弃、批次可重检、重启后跳号拒绝 |
| `report` | 归一化后跨运行字节一致、按 PID/业务码稳定排序 |

`go test -race ./internal/session/ ./internal/detector/` 无数据竞争；`go vet ./...` 干净。

## 2. 定点区间裁决（baseline，阈值 56.000000）

| PID | 业务码 | 定点评分 | 误差上界 | 区间 [low, high] |
| --- | --- | --- | --- | --- |
| 1001 | E_PRIV_ESCALATION | 106000000 | 1 | [105999999, 106000001] |
| 1002 | E_INDETERMINATE_RANGE | 56000000 | 1 | [55999999, 56000001] |
| 1003 | E_OK | 10500000 | 0 | [10500000, 10500000] |
| 1004 | E_OK | 2000000 | 0 | [2000000, 2000000] |

1002 的区间跨越阈值，演示由截断误差上界驱动的不确定性裁决；
1001 下界仍高于阈值，判确定性提权。全程无浮点，余数逐项可在 `ledger` 子命令审计。

## 3. 异常注入断言

`privesc inject --db <path>` 实测输出：

```
mask_out_of_range -> E_SAMPLE_PARSE
multiply_overflow -> E_SCORE_OVERFLOW
handle_abuse      -> E_PRIV_ESCALATION
orphan            -> E_ORPHAN_REF
snapshot_gap      -> E_SNAPSHOT_VERSION
```

掩码越界与乘法溢出场景快照版本均未推进；跳号后 `replay` 以
`E_SNAPSHOT_VERSION` 拒绝加载（退出码 1）。

## 4. 确定性报告

`report --normalize` 连续两次导出 SHA-256 完全一致：

```
6d34c38a76a056ea67930ef60713127019e8237f0a74dfff9e8c4c0115fe8f03
```

时间字段固定为 `T0+SAMPLE_MARK`，归一化报告中实测耗时与易变版本号归零。

## 5. Windows 可移植性

`GOOS=windows GOARCH=amd64 go build ./cmd/privesc` 产出 PE32+ 控制台可执行文件，
纯 Go 驱动无需 CGO/编译器，Windows 命令行可直接调用，临时工作目录使用普通
读写权限即可创建样本与快照库。

## 6. 复现步骤

```
go test ./... -count=1
go build -o privesc ./cmd/privesc
privesc generate --dir work/base --scenario baseline
privesc detect   --dir work/base --threshold-micro 56000000
privesc trace    --dir work/base --pid 1003
privesc report   --dir work/base --normalize --out work/report.json
privesc inject   --db work/inject.db
privesc replay   --db work/base/snapshots.db --version 1
privesc ledger   --db work/base/snapshots.db --version 1
```
