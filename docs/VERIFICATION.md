# 实际验证记录

验证时间：2026-11-24（Asia/Shanghai）

环境：

```text
Go 1.27.4 linux/amd64（go.mod 最低 1.22）
modernc.org/sqlite v1.34.5
SQLite WAL, synchronous=FULL
GOOS=windows GOARCH=amd64 go build ./...  PASS（交叉构建复核）
```

命令：

```bash
gofmt -l *.go cmd/demo/*.go
go test -count=1 ./...
go test -count=1 -race ./...
go vet ./...
GOOS=windows GOARCH=amd64 go build ./...
./scripts/demo.sh -port 28096
```

结果：

```text
gofmt                无待格式化文件
go test ./...        ok local/workbench
go test -race ./...  ok local/workbench 4.096s
go vet ./...         PASS
GOOS=windows build   PASS
demo                 VERIFIED 12/12 checks PASS（端口 28096）
```

演示逐项：

1. 固定样本分区与待决事务可见；
2. 严格顺序从分区序号 0 开始消费；
3. 待决提交在同一状态推进中变为可见并将前沿推进到 3；
4. 同分区并发拉取一个成功、另一个返回 `E_LEASE_HELD`；
5. 租约释放后消费从下一个序号继续，无跳号；
6. 待决回滚为 `ROLLED_BACK` 且不参与消费；
7. 协调器挂起在 120ms 超时返回 `E_TXN_TIMEOUT`，新请求重试成功提交；
8. 再次强制复核得到矛盾结论时返回 `E_TXN_CONFLICT`，原结论未被覆盖；
9. 重放期间新消息写入但前沿保持被重放上限钳制，重放追平后才可用；
10. 租约超时抢占后旧租约提交返回 `E_LEASE_STALE`；
11. 队列容量 2 时第 3 个任务返回 `E_QUEUE_FULL`，1ms 预算轮次全部确定性跳过，后续轮次完成；
12. 意图持久化后 `os.Exit(75)`，重启扫描到 1 个未完成拉取意图，报告动作与诊断页一致。

机器可读结果：`artifacts/demo-verification.json`；服务日志：`artifacts/demo-server.log`。

自动化测试额外覆盖：

- 消息重复内容幂等、内容冲突、哈希/显式路由、分区内并发序号无缺口；
- 租约版本过期、空闲超时真实释放并同序号恢复；
- 非法消息迁移不修改持久化状态；
- 同一请求 ID 并发去重与不同内容冲突；
- 事务超时、重试、回滚、矛盾结论、重复裁决复用；
- 重放不重复推进前沿，保留位点导致 `E_REPLAY_EXPIRED`；
- 预算降级、队列满、同分区事务去重、全局并发受限；
- 读快照不出现“消息可见但前沿未推进”的部分提交；
- 意图恢复、请求清理和恢复报告一致性。
