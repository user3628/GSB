# 技术设计与验证说明

## 1. 模块结构

```
cmd/matbench            CLI 与 HTTP 入口
internal/
  recipe    配方类型、splitmix64 确定性随机源、固定种子生成器、增量补丁
  textnorm  Unicode 规范化（NFKC 子集）、空白折叠、分词
  control   固定哈希函数、词项权重、32 维单位控制向量
  noise     可平铺值噪声、逐八度合成、Kahan 求和、big.Float 误差界、扰动重放
  palette   色卡 PNG、调色板提取、调制、重采样、8 位量化、参数误差界
  pipeline  四阶段编排、版本指纹、精确失效、增量校验、停止/恢复、报告聚合
  store     SQLite 事务检查点、单调序号、条件发布、真实文件锁
  server    本地 HTTP 诊断 API
  demo      固定种子工作区引导与停止哨兵
  fp        域分离 SHA-256 指纹与确定性规范化 JSON
  errorsx   业务错误码
```

## 2. 确定性与固定种子

- 所有随机来源都是自实现的 splitmix64（`internal/recipe/rng.go`、噪声晶格哈希、色卡像素哈希），
  不依赖 `math/rand` 全局源、map 遍历顺序或时间，跨平台/跨 Go 版本结果一致。
- 内置生成器种子固定为 `recipe.DemoSeed = 20261125`，产出 4 条材质：
  拉丝铝（4 八度/32px）、锈蚀金属（6 八度/48px）、雾面陶瓷（3 八度/24px）、青苔石（5 八度/40px），
  覆盖中英混排描述、不同八度数与目标分辨率。
- 所有指纹都经 `fp.CanonicalJSON`（递归排序键、紧凑 JSON）后做域分离 SHA-256，
  避免 Go map 序列化顺序影响。

## 3. 阶段一：规范化与控制向量

- `textnorm.Normalize` 实现 NFKC 的确定性子集：
  全宽 ASCII（U+FF01–FF5E）转半角、全宽空格折叠、半角片假名（含浊音/半浊音组合）转全宽、
  常见兼容标点（弯引号、长短破折号、省略号等）映射、`unicode.IsSpace` 空白折叠、`strings.ToLower`。
  该实现只使用标准库；覆盖本管线生成器与测试涉及的全部字符块，作为 NFKC 子集明确记录。
- 分词规则：拉丁字母/数字取最长连续片段，CJK 表意文字与假名逐字成词，标点与空白为分隔符。
- 词项权重由词长与位置解析地导出并归一化为概率分布；哈希函数为 FNV 素数混合后接 splitmix64，
  种子 `control.HashSeed` 一并写入诊断。每个词对 32 个分量各取独立哈希，加权求和后做 L2 单位化。
- 空描述/仅标点返回带 `material_id` 的 `E_EMPTY_PROMPT`；分量非有限或范数为零返回
  `E_HASH_DEGENERATE`。

## 4. 阶段二：逐八度噪声与数值诊断

- 值噪声在 `girth = round(frequency * 2^octave)` 的环面晶格上采样，边界用取模环绕做无缝插值，
  双线性插值采用 3t²−2t³ smoothstep。每八度振幅按 `|a_i| / Σ|a|` 归一化。
- 合成对所有像素按固定八度顺序、固定像素顺序累加；多线程只并行构建彼此独立的八度晶格层，
  累加阶段不并行，因此 workers=1 与 workers=N 的 `[]float64` 字段逐字节相等，指纹相同。
- 累加采用 Kahan 补偿求和，减少浮点顺序误差。
- 退化（八度数 0 或振幅全 0）在结果中显式标注 `degenerate` 与原因，字段保持全 0，
  绝不静默产出空白纹理。
- 误差上界（满足“由显式高精度参考累加推导”）：
  - 在确定性 16×16 参考网格上，用 96 位 `math/big.Float` 重新计算每层采样并逐层累加；
  - 每八度记录与 float64 Kahan 路径的最大实测差 `ref_max_delta`；
  - 再叠加解析舍入裕度 `theory_bound = Σ(|s|·2^-53 + 2^-53)`，二者之和作为该八度 `error_bound`，
    并给出 `cumulative_bound`；这些数值来自真实高精度参考，而非缺失信息推断。
  - `log_contribution = mean(log2(1+|v|))` 记录对数贡献。
- 扰动重放归因：对目标八度施加确定性 +1 ulp 扰动（`math.Nextafter`），重算整个 Kahan 链路，
  以参考网格 L1 响应作为第二种排序依据；对数贡献排序与扰动排序不一致的八度置 `review_flag`，
  报告中同时保留两种依据（`contrib_rank`、`perturb_rank`、`log_contribution`、`perturb_l1`）。

## 5. 阶段三：调色板匹配与量化

- 色卡 PNG 由固定种子像素哈希本地生成；加载时严格比对声明宽高，不符返回
  `E_CARD_DIM_MISMATCH`；提取不到颜色返回 `E_PALETTE_EMPTY`。
- 噪声灰度经控制向量调制：像素按 `i mod 32` 取固定分量映射到 (0,1] 标量并裁剪到 [0,1]。
- 按目标分辨率做确定性双线性重采样（环面取模），与基础色相乘后在调色板上做最近邻
- （平方欧氏距离，平局取调色板前置项，保证确定性），输出 8 位 RGBA PNG。
- 量化后全通道为零返回 `E_QUANT_ALL_ZERO`。
- 材质参数（roughness、metallic、base RGB）量化到 1/256；每个参数给出误差上界
  `|量化差| + 1/512 + 噪声累计误差界` 与有效十进位数 `floor(-log10(bound))`。
- 产物中只保存工作区相对路径，保证跨工作区指纹稳定、增量逐字节可比。

## 6. 阶段四：版本指纹、增量与失效传播

- 每个阶段有独立的“输入内容指纹”：
  - control 输入：material_id + description
  - noise 输入：material_id + 完整 NoiseParams
  - palette 输入：card_name、target_resolution、base_color、roughness、metallic
  - report 输入：material_id + schema 版本
- 增量补丁按字段映射到最早受影响阶段（`EarliestStage`）；该阶段及其全部下游强制重建，
  未受影响的上游继续从检查点复用。下游即使自身输入指纹未变，也通过 `ForceFromStage`
  随上游产物级联失效，失效轨迹中区分“输入变化”与“downstream propagation”。
- 每次增量在独立临时工作区执行一次从零全量参考运行，逐字节比对每个阶段的检查点 payload、
  产物指纹与纹理 PNG；任一不一致返回 `E_INCREMENTAL_MISMATCH`，并给出首个差异的
  material_id 与阶段。

## 7. SQLite 检查点、发布互斥与替换识别

- 表 `checkpoints(material_id, stage, seq, config_ver, recipe_fp, artifact_fp, payload, active)`，
  部分唯一索引保证同一 material_id + stage 只有一条 `active=1` 有效记录；历史版本保留为非活跃。
- 发布在单事务内完成：旧活跃记录置 `active=0` + 插入 `seq+1` 新记录。
  - 首次发布必须 `seq=1`；后续必须严格 `seq = old+1`，否则 `E_STALE_CHECKPOINT`（stale）；
  - `seq <= old`：同序号但指纹/摘要变化判定为 tampered（整体替换/篡改），否则为 rollback（回退/重复）；
  - 跨材质错位发布（新材质却带非 1 序号）同样拒绝。
- 互斥采用“真实文件锁 + SQLite 条件更新”双层：
  `matbench.lock` 在 Unix 用 `flock(2)`、Windows 用 `LockFileEx/UnlockFileEx`（分平台文件，
  纯 Go 通过 `golang.org/x/sys` 调用，无需 CGO），事务内再做序号条件判断。
- 失效轨迹写入 `invalidations` 表，含阶段、原因、新旧指纹与时间戳。

## 8. 优雅停止与恢复

- 停止通过工作区 `STOP` 哨兵文件请求；运行器在每个不可拆分材质单元（一个材质的完整四阶段）
  边界轮询，收到请求后完成当前单元、提交事务再退出，`RunSummary.Stopped=true`。
- 恢复时 `tryReuse` 用输入指纹匹配活跃检查点，命中则直接反序列化复用、跳过计算；
  因为所有产物内容寻址且路径相对化，恢复后的最终摘要/纹理与一次连续运行逐字节一致（有测试覆盖）。

## 9. 验证结果（本机实际执行）

环境：Linux amd64，`go version go1.27.4`（`go.mod` 声明 `go 1.22`，满足最低版本要求），
同时通过 `GOOS=windows GOARCH=amd64 go build` 交叉编译验证（免 CGO 纯 Go 驱动）。

- `go vet ./...`：无告警；`gofmt -l .`：无输出。
- `go test ./... -count=1`：6 个含测试包全部 `ok`（control、noise、palette、pipeline、store、textnorm）。
  覆盖：全半角/片假名规范化、分词、空描述错误码、单位向量确定性、单/多线程逐字节一致、
  两类噪声退化、误差界与双排序复核、色卡尺寸不符/空调色板/全零量化、增量逐字节一致与
  `E_INCREMENTAL_MISMATCH`、停止恢复一致性、单调序号与回退/篡改/错位识别。
- 端到端固定摘要（4 条演示材质，一次完整运行，workers=1 与 workers=4 相同、重复运行稳定）：

```
report_digest = 118a781fd08fcc396a6b67b4b2625f0e58c9fbeb55dc37317d4633505034ca98
MAT-0001 dc101e5c7cf33fe8112739464076c637c4cc3f0fdac16c0eff22720806dff8b8
MAT-0002 f7b94d795ca1403c5fb82d208f54361be218b0664efe7aec7181ad6c4be19a25
MAT-0003 e3006fd6180f7031190c85132ff64e7a1929c80e2a7c99b454f9a40440ffcbf7
MAT-0004 5b5fb292d43a888852cb32436061daef38c78ef886253cdea413b486462acdff
```

- 增量实测（MAT-0003 仅改 `noise_frequency=3.5`）：control 保持 `seq=1` 复用，
  noise/palette/report 推进到 `seq=2`，失效轨迹含 1 条输入变化 + 2 条 downstream propagation；
  与临时工作区全量参考运行逐字节一致。
- 中断恢复实测：预置 `STOP` 后运行在首个材质单元前退出（0 材质完成、`stopped=true`），
  `resume` 后 4 材质全部完成，摘要与连续运行完全相同。
- HTTP 冒烟：health/run/status/material/incremental/export 全部返回预期 JSON。

## 10. 库形式集成

上游材质服务可直接导入内部包（同模块内）或以子模块方式封装：

```go
st, _ := store.Open("/path/to/workspace")
p := &pipeline.Pipeline{Dir: "/path/to/workspace", Store: st}
book, _ := demo.Load("/path/to/workspace")
summary, err := p.RunFull(book, pipeline.RunOptions{Workers: 4})
```

所有阶段产物均内容寻址（指纹）、无隐式全局状态、无后台 goroutine 泄漏（噪声 worker 受
WaitGroup 与信号量约束），适合作为服务内嵌库使用。
