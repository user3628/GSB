# 实际验证记录

## 环境

- 当前执行环境：Python 3.14.4、SQLite 3.46.1、FastAPI 0.165.0、uvicorn 0.54.0、httpx 0.28.1、pydantic 2.14.4。
- 项目声明最低支持 Python 3.11。
- 未使用 Docker、容器、外部账号或在线服务。
- 路径与子进程启动方式兼容 Windows `cmd.exe` / PowerShell；测试和演示只绑定 `127.0.0.1`。

## 自动化测试

执行：

```text
python -m pytest -q
```

结果：

```text
16 passed, 1 warning in ~3s
```

覆盖点：

1. 全量摘要错误与过期版本不发布任何业务状态。
2. 同一基础版本双并发增量只有一个成功，另一个为 `E_STALE_BASE`。
3. 单键增量只影响直接依赖组件及其下游，无关 `east-audit` 不进入影响集合。
4. 拓扑批次顺序正确，派生引用包含配置键、组件声明和产物依赖。
5. 依赖环通过 `impact`、`plan`、`dispatch` 返回 `E_DEPENDENCY_CYCLE` 且不创建分发运行。
6. 合法/非法状态迁移；非法迁移无状态或事件变化。
7. 并发读事务只观察到完整版本，快照配置与期望摘要一致。
8. 真实 HTTP 超时在截止时间内返回，且端点记录客户端断连。
9. 连接失败与业务拒绝、冲突、超时的错误码区分。
10. `ROLLBACK` 策略回滚本轮已应用组件，失败组件保留 `FAILED`。
11. 同一快照重复分发复用持久化结果，不产生第二次推送。
12. 重建中途 `os._exit(91)` 后恢复到最后完整版本，并可重试提交。
13. 推送中途 `os._exit(92)` 后分发标记 `ABORTED`，未确认组件 `DISPATCHED -> ROLLED_BACK`。
14. FastAPI 统一错误体。
15. 全量快照与增量共享版本空间。
16. 同一快照两个并发分发只有一个执行推送，另一个复用结果。

## 端到端演示

执行：

```text
python scripts/demo.py
```

退出码 0。关键结果：

### 全量分发

- 版本 1，受影响组件 7 个。
- 批次：`[audit, gateway x3]`、`[orders x3]`。
- 七个组件全部 `APPLIED`，结论 `ALL_APPLIED`。
- 所有一致度评分达到 `1.0`。

### 单键增量级联

- 只将 `feature.checkout` 从 `true` 改为 `false`，产生版本 2。
- 受影响组件 6 个：三个 gateway 为直接依赖，三个 orders 为下游依赖。
- `east-audit` 不在影响集合。
- 批次为 gateway 批次后 orders 批次。
- 失效产物 18 个：6 个期望摘要、6 个一致度评分、6 个批次索引；重建总产物 21 个，全部原子发布。

### 拒绝、超时与回滚

- 版本 3 修改 `app.timeout`。
- `east-gateway` 返回 `E_PUSH_REJECTED`。
- `west-gateway` 在 0.20 秒读取截止时间到达后返回 `E_PUSH_TIMEOUT`，客户端 socket 关闭。
- 本轮成功应用的 4 个组件按逆序 `ROLLED_BACK`。
- 最终结论 `ROLLED_BACK`。

### 强制终止与恢复

- 子进程在版本 4 重建事务中写入快照、版本指针和派生行后直接 `os._exit(91)`，没有提交事务。
- 恢复前当前版本仍为 3，版本 4 不可见。
- `recover()` 读到 1 条 `PENDING SUBMIT_INCREMENT`，将其标记 `ABORTED`。
- 恢复报告：`last_complete_version=3`、`current_version_after=3`。
- 使用同一增量重试成功提交版本 4，证明恢复后版本序列可继续推进。

演示的最终摘要：

```text
full_dispatch:         version=1, 7 affected, ALL_APPLIED
incremental_cascade:   version=2, 6 affected, 18 invalidated artifacts
failure_disposition:   version=3, ROLLED_BACK, 2 failures, 4 rollbacks
recovery:              crash exit=91, complete version 3 -> retry version 4
```

