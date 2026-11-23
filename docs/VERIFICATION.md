# 实际验证记录

## 环境

```text
Go:      go1.27.4 linux/amd64
模块:    secverify
Go 指令: 1.22
SQLite:  modernc.org/sqlite v1.35.0（纯 Go）
采样标记: T0_FIXED_SAMPLE
```

## 静态与测试检查

```text
$ go test ./...
ok  secverify/cmd/secverify
ok  secverify/internal/config
ok  secverify/internal/deps
ok  secverify/internal/engine
ok  secverify/internal/probe
ok  secverify/internal/report
ok  secverify/internal/snapshot
?   secverify/internal/model      [no test files]
?   secverify/internal/sample     [no test files]

$ go vet ./...
exit 0

$ GOOS=windows GOARCH=amd64 go build ./...
exit 0

$ go test -race ./internal/engine ./internal/snapshot ./cmd/secverify
ok  secverify/internal/engine
ok  secverify/internal/snapshot
ok  secverify/cmd/secverify
```

自动化测试覆盖：

- 基线严格解析、重复 ID、非法范围、未知字段、错误偏移、单位换算、继承；
- 长度前缀块截断和候选构建不修改原基线；
- 缺失引用、最小环（含自环/尾节点排除）、反向闭包和逐项来源链；
- 真实 worker 进程读取文件权限、post-materialize 权限篡改、符号链接逃逸、父目录遍历；
- SQLite 连续版本链、旧版本保留、同版本内容替换、内容哈希篡改、跳号 fallback、过期版本提交拒绝；
- 同命名空间竞争超时、不同命名空间并行、事务槽位上限、探测槽位上限；
- 报告稳定排序和空集合编码；
- CLI 级 demo 字节一致、五类注入、两种强制终止恢复、外部 `run`。

## 最终报告

构建与运行：

```bash
go build -o /tmp/secverify-final ./cmd/secverify
/tmp/secverify-final demo --workdir <fresh-dir> --report demo.json
/tmp/secverify-final inject --workdir <fresh-dir> --report inject.json
/tmp/secverify-final crash-demo --workdir <fresh-dir> --report crash.json
```

结果与 SHA-256：

| 场景 | 结果 | 断言 | SHA-256 |
|---|---|---|---|
| demo | PASS | 12 checks | `88f3e35644d4028918c875e14abe5289c6e014f2fd1294915efbf2e9b2ecebda` |
| inject | PASS | 5/5 injections | `0789b938cf03f9622503353d890f9e9b8c15434d48814b321d4c8c626497fbb1` |
| crash-demo | PASS | 2/2 crash checks | `e1c78b096919e60a740131d151207573d3baf246aa32f69ef3841db41499e731` |
| 外部 run | PASS | 2 checks | `c6415f7df336aaed79b5c9a29070ea9c6cbc5ff48daf4e5b31a44bc874be4914` |

每组报告均用两个全新工作目录重复运行：

```text
cmp demo-1.json demo-2.json    => 0
cmp inject-1.json inject-2.json => 0
cmp crash-1.json crash-2.json  => 0
```

## 场景观察

### demo

- 第 1 轮账户最低长度 `12 -> 14`，失效闭包包含 1 个触发项和 7 个传递受影响项。
- 第 2 轮审计目录 `0750 -> 0700` 且保留期 `30 -> 60` 天，闭包只沿依赖者方向传播。
- 第 3 轮新增账户历史长度项并将服务 `automatic -> disabled`。
- 第 4 轮删除 `svc.defender`，同一变更集中重定向 `reg.hklm.defender`，闭包仍通过旧图记录历史依赖边。
- 最终版本链 `0,1,2,3,4`，每轮每个探测均匹配，报告结果 PASS。

### inject

| 注入 | 观察业务码 | 版本推进 | 状态污染 |
|---|---|---|---|
| 依赖缺失 | `E_DEP_MISSING` | 否 | 否 |
| 依赖环 | `E_DEP_CYCLE` | 否 | 否 |
| `../escape-canary` 探测越界 | `E_PROBE_ESCAPE` | 否 | 否，外部金丝雀保持 `CANARY_MUST_NOT_CHANGE` |
| 删除已提交版本 1 制造跳号 | `E_SNAPSHOT_VERSION`，fallback 0 | 否 | 否，连续前缀保持版本 0 |
| 持锁探测 + 40ms 等待阈值 | `E_CHANGE_CONTENTION` | 失败请求不推进；成功请求仅推进 1 版 | 仅成功请求的终态 |

### crash-demo

| 崩溃点 | 已提交版本恢复 | 未提交版本 | 重启继续 |
|---|---|---|---|
| probe | 版本 1，内容哈希一致 | 版本 2 丢弃 | 正常提交版本 2 |
| before-commit | 版本 1，内容哈希一致 | 版本 2 丢弃 | 正常提交版本 2 |

未承诺被强制终止的探测生成退出报告；验证重点为 SQLite 已提交边界和版本连续性。

## 确定性检查

- 报告内没有真实时间戳、PID、临时目录绝对路径或随机值。
- 时间字段固定为 `T0_FIXED_SAMPLE`。
- JSON 对项标识、业务码、检查名、注入场景和版本链稳定排序。
- 两个全新目录的 demo/inject/crash 报告均字节级相同。
