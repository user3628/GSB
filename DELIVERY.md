# 交付说明

## 代码结构

- `internal/dataset`：固定种子样本生成器、CSV/manifest 读写、样本 SHA-256。
- `internal/config`：配置规范化、默认路径网格、配置版本指纹。
- `internal/preprocess`：列类型/缺失统计、均值/中位数/零填充、众数/`__missing__` 填充、标准化、独热和折内目标编码。
- `internal/cv`：确定性分层 K 折、类别分布偏移、分折指纹和查询缓存键。
- `internal/model`：逻辑回归目标函数、FISTA/近端梯度、L1 软阈值、L2/弹性网惩罚、轨迹、指标和 AUC。
- `internal/checkpoint`：SQLite 检查点表、原子 JSON 记录、发布锁、连续序号、阶段/折顺序校验。
- `internal/filelock`：Unix `flock` 与 Windows `LockFileEx` 真实文件锁。
- `internal/experiment`：会话、训练编排、实时快照、延迟预算查询、缓存、选择、导出、增量和一致性比较。
- `internal/api`：本地 HTTP API 与嵌入式控制台。
- `cmd/workbench`：CLI 入口。
- `internal/demo`、`internal/verify`：固定摘要演示和一键验收。

## 需求追踪

1. **预处理与分折**：`preprocess.Prepare` 依据 manifest 的 `semantic` 和解析统计识别 continuous/categorical/missing；`cv.Stratified` 固定种子分层分折并写每折正负样本比例、训练/验证差和绝对偏移。测试见 `internal/cv/folds_test.go`。
2. **确定性求解核心**：`model.Fit` 使用同一 FISTA/近端梯度实现；每轮记录 objective、系数 L2/欧氏变化、归一化变化、步长和轮次。`lambda=0` 强制无惩罚并输出 `unregularized=true`。路径按 lambda、l1_ratio 排序；同点重解由 `internal/model/fit_test.go` 验证字节一致。
3. **延迟预算与缓存**：`Session.Query` 先分配 `query-000001` 风格查询 ID，读取 `atomic.Pointer[Snapshot]`，不等待训练锁；预算失败返回含 `query_id`、`elapsed_ms`、`budget_ms` 的 `E_LATENCY_BUDGET`。缓存键结构见 `cv.CacheKey`。
4. **CV 与选择**：`buildValidationSummary` 输出每个 lambda/ratio 的逐折指标、均值、标准差、选中点、选择规则和全部淘汰原因。K<2、单类折、NaN/Inf 指标返回带 `fold`、`reason` 的 `E_INVALID_FOLD`。
5. **增量重算**：`AnalyzeChange` 将变更映射到 `preprocess`、`split`、`fold_model`、`selection`、`query_settings`；`RunIncremental` 按折指纹、特征矩阵指纹和求解器指纹复用旧路径点。`CompareRunDirectories` 在不一致时给出 `first_diff_fold` 和 `field`。
6. **检查点与恢复**：阶段顺序为 `preprocess -> split -> fold#0..K-1 -> selection -> final`。记录含 `seq`、`stage`、`fold`、`input_fingerprint`、`config_version`、`coefficient_digest` 和完整 payload。JSON 先写同目录临时文件、`Sync`、再原子 rename；SQLite 行在事务中插入。停止请求在当前求解轮次后的检查点生效。
7. **并发保护与数值链路**：`checkpoint.Manager.Publish` 在跨进程文件锁内读取最大序号并要求 `seq=last+1`；落后、重复、跳号、跨折错位返回 `E_STALE_CHECKPOINT`，非法阶段跳转/缺前置折返回 `E_INVALID_STAGE`。NaN/Inf 在初始目标、每轮系数/目标检查中转换为带折号和轮次的 `E_NUMERIC`。
8. **HTTP/CLI/演示**：CLI 覆盖 generate/demo/verify/serve/train/query/stop/resume/export/incremental；HTTP 提供同等操作和单页控制台。`demo.Run` 生成 `artifacts/demo/demo-summary.json`。

## 错误码

| Code | 触发场景 |
| --- | --- |
| `E_LATENCY_BUDGET` | 查询超过声明预算；含 query ID、elapsed/budget |
| `E_INVALID_FOLD` | K<2、单类折、折覆盖错误、指标 NaN/Inf |
| `E_INCREMENTAL_MISMATCH` | 增量与全量产物不同；含首个差异折/字段 |
| `E_STALE_CHECKPOINT` | 序号落后/重复/跳号、跨折错位发布 |
| `E_INVALID_STAGE` | 非法阶段跳转、缺少前置阶段或折产物 |
| `E_NUMERIC` | 系数、目标函数或指标出现 NaN/Inf |
| `E_CONFIG` / `E_NOT_FOUND` / `E_CONFLICT` | 配置、资源和会话运行冲突 |

## Windows 说明

- `internal/filelock/filelock_windows.go` 使用 `golang.org/x/sys/windows.LockFileEx/UnlockFileEx`。
- 完整程序依赖 `github.com/mattn/go-sqlite3` 的 CGO；Windows 需安装 Go 1.22+ 和 MSYS2 UCRT64 GCC。
- 默认构建使用驱动内 SQLite amalgamation；`-tags libsqlite3` 使用系统 `sqlite3`，此时需要开发头文件/链接库，运行时需要 `sqlite3.dll`。
- 已在 Linux 验证 `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./internal/filelock`；完整 CGO Windows 二进制需在带 mingw 的 Windows/交叉工具链环境构建。

## 主要产物

- `samples/manifest.json`：两档文件、行数、列数和特征语义。
- `sessions/<id>/config.json`：规范化配置和版本输入。
- `sessions/<id>/workbench.db`：SQLite 检查点。
- `sessions/<id>/checkpoints/*.json`：原子 JSON 检查点。
- `sessions/<id>/exports/coefficients.json`：最终系数与截距。
- `sessions/<id>/exports/validation_summary.json`：CV 汇总与选择依据。
- `sessions/<id>/exports/trajectory.json`：逐折/逐点目标轨迹。
