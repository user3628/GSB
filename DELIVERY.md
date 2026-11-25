# 交付入口

这是一个可离线构建的 Go + SQLite 主机安全基线漂移审计引擎。

## 快速验收

```bash
go version          # 开发验证使用 go1.27.4；go.mod 目标为 go 1.22
sqlite3 --version
bash scripts/acceptance.sh
```

验收脚本会：

1. 运行全部 Go 测试。
2. 生成固定种子基线、JSONL 和旧版长度前缀样本。
3. 验证加权配额降级。
4. 验证单调版本、`E_STALE_BASE` 和增量级联。
5. 启动真实 loopback HTTP 探测相关测试。
6. 连续两次 `kill -9` 审计进程，重启恢复并比较报告。
7. 使用 `sqlite3` 核对 `PENDING` 意图为 0、主机汇总与结论计数一致。

## 文档

- 设计与语义：`docs/design.md`
- 实际验证结果：`docs/validation.md`
- 完整测试日志：`artifacts/full-test.log`
- 竞态检测日志：`artifacts/race-test.log`
- 验收脚本日志：`artifacts/acceptance-run.log`

## 常用命令

```bash
go test -mod=vendor -count=1 ./...
go build -o auditdrift ./cmd/auditdrift
./auditdrift seed --dir .demo/seed
./auditdrift init --db .demo/audit.db --quota 25
./auditdrift audit --db .demo/audit.db --concurrency 3 --budget 5s
./auditdrift report --db .demo/audit.db
./auditdrift recover --db .demo/audit.db
```

清理验收产物：

```bash
rm -rf .acceptance-tmp .demo tmp
```
