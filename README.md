# 本地提权检测诊断工作台（privesc workbench）

面向 Windows 主机安全团队的本地提权检测领域可视化与诊断工作台。读取内置确定性
生成器产出的进程/服务声明清单（JSONL）与提权嫌疑事件流（长度前缀文本块），
以纯整数定点数完成评分、截断误差记账与区间裁决，并通过 SQLite 维护带版本号的
检测快照，支持并发会话隔离、强制终止恢复、异常注入与确定性 JSON 报告。

## 快速开始

```
go test ./... -count=1
go build -o privesc ./cmd/privesc

privesc generate --dir work/base --scenario baseline
privesc detect   --dir work/base --threshold-micro 56000000
privesc trace    --dir work/base --pid 1003
privesc replay   --db work/base/snapshots.db --version 1
privesc ledger   --db work/base/snapshots.db --version 1
privesc report   --dir work/base --normalize --out work/report.json
privesc inject   --db work/inject.db
```

Windows 构建（纯 Go，无 CGO）：

```
GOOS=windows GOARCH=amd64 go build -o privesc.exe ./cmd/privesc
```

## 稳定业务码

`E_SAMPLE_PARSE`、`E_SCORE_OVERFLOW`、`E_PRIV_ESCALATION`、
`E_INDETERMINATE_RANGE`、`E_ORPHAN_REF`、`E_SNAPSHOT_VERSION`、
`E_SESSION_BUDGET`、`E_INJECTION`、`E_INTERNAL`、`E_OK`。

## 文档

- 架构与定点/误差/快照设计：`docs/ARCHITECTURE.md`
- 实际验证结果与复现步骤：`docs/VALIDATION.md`
