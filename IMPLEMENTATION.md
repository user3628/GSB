# 设备授权与信任度量本地引擎 — 技术说明

## 结构

```
cmd/engine/main.go          CLI 入口（gen / run / run-all）
internal/engine/            引擎核心
  codes.go                  业务码定义
  types.go                  声明、策略、参数、样本集类型
  score.go                  整数定点评分（scale=1e6，溢出检测）与截断误差账本
  register.go               注册声明校验（不通过则不写任何状态）
  cred.go                   凭证签发（HMAC-SHA256）与持续校验
  scheduler.go              风险加权确定性调度与预算裁决
  snapshot.go               SQLite 快照存储、版本链校验、整体替换识别
  engine.go                 引擎、会话隔离、设备粒度互斥、竞争检查
  round.go                  一轮评估编排（注册→评分→调度→并发评估→签发→校验→快照）
  report.go                 确定性 JSON 报告
internal/samples/           内置确定性样本与密钥生成器（无网络依赖）
internal/app/               场景运行器与异常注入驱动
```

## 关键设计

- **定点评分**：事件按逻辑 seq 排序后累加 `delta*mult`，全程 int64，
  乘/加溢出分别检测并返回 `E_SCORE_OVERFLOW`；`Truncate` 记录截断余数账本。
- **凭证**：HMAC-SHA256 绑定设备、租户、能力集合、评分版本、签发/过期逻辑时间；
  校验顺序为签名→过期→能力→评分版本回退，对应
  `E_CRED_SIGNATURE / E_CRED_EXPIRED / E_CAPABILITY_DENIED / E_SCORE_REGRESSED`。
- **调度**：按风险分降序（设备 ID 升序兜底）确定性排队；超出并发上限标记等待而非拒绝；
  单设备超时预算 → `E_DEVICE_BUDGET`；全局 CPU/内存不足时该设备及全部更低风险设备
  → `E_GLOBAL_BUDGET_EXHAUSTED`，且仍计入报告。实际执行用 worker 池 + 在途信号量，
  结果按下标写回，因此报告与 goroutine 交织无关。
- **快照**：单事务内写入快照行并整体替换设备状态表；内容哈希链接前一版本，
  加载时校验版本连续性与内容哈希（识别整体替换），跳号/篡改返回
  `E_SNAPSHOT_VERSION` 并给出最近连续版本，引擎回退到该版本继续提交。
- **恢复**：依赖 SQLite 事务原子性；崩溃恢复测试用子进程在事务未提交时
  `os.Exit(0)` 模拟强杀，重启后未提交事务整体丢弃、已提交签发不回退、
  设备可重新参与评估。
- **会话与竞争**：每租户独立会话与评分账本；同一设备授权按设备粒度互斥，
  竞争者得 `E_AUTH_CONTENTION`（`RunContentionCheck` 确定性地产出 1 成功 + N-1 竞争）。
- **异常注入**：仅通过样本与参数驱动（`InjectRawSnapshot` 等输入入口），
  覆盖指纹重复、评分溢出、凭证过期复用、快照跳号、并发竞争、全局预算耗尽六类；
  故障轮不提交快照，版本不推进，非法设备不落库。
- **确定性报告**：`Workers` 不参与序列化；设备按（租户、设备、业务码）稳定排序；
  时间字段只用固定采样标记与样本逻辑时间；并发度 1..8 与重复运行均字节级一致。

## 运行

```
go run ./cmd/engine gen --dir <dir>            # 生成样本与密钥材料
go run ./cmd/engine run --scenario normal      # 运行单场景并导出报告
go run ./cmd/engine run-all --dir <dir>        # 运行全部场景
go test ./...                                  # 全部测试
```

## 实际验证结果（本机，Go 1.27.4，modernc.org/sqlite v1.63.0）

- `go test ./...` 全部通过：
  - 单元：评分溢出/排序、截断账本、凭证四类拒绝码、调度降级顺序与等待标记、
    注册校验（格式/租户/重复 ID/重复指纹且不落库）、快照链与跳号、整体替换识别、
    崩溃恢复（子进程强杀）、竞争（1 成功 7 `E_AUTH_CONTENTION`）、会话隔离、
    授权互斥与阈值/能力裁决。
  - 场景：六类异常注入均断言到对应业务码、故障轮快照版本未推进、非法设备未落库；
    正常场景两轮提交版本链 [1,2]；快照跳号场景回退到最近连续版本 1 并续交版本 2，
    跳号行保留可回放。
  - 确定性：全部场景在 workers=1/2/4/8 下报告字节级一致；重复运行字节级一致。
- `go run ./cmd/engine run-all` 输出：
  - normal: `OK=6`, snapshot_version=2
  - fingerprint-dup: `E_DEVICE_INVALID=1,OK=2`, version=0
  - score-overflow: `E_SCORE_OVERFLOW=1,OK=1`, version=0
  - cred-expired: `OK=2`（verify_code=`E_CRED_EXPIRED`）, version=0
  - snapshot-gap: fault=`E_SNAPSHOT_VERSION`(latest=1), 链 [1✓,2✓,3✗], version=2
  - auth-contention: 1 成功 / 7 `E_AUTH_CONTENTION`, version=1
  - budget-exhausted: `OK=2,E_DEVICE_BUDGET=1,E_GLOBAL_BUDGET_EXHAUSTED=3`, version=0
