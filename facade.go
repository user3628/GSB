// Package mitmpin 是中间人检测与证书固定校验管线的对外集成入口。
//
// 导出五组宿主服务可直接调用的能力：
//   - LoadBatch / LoadBatchFile：观测与策略加载
//   - ApplyPolicy：策略快照应用与热更新提交
//   - VerifyPinning：固定校验与中间人判定
//   - OpenStore / Recover：检查点快照回放与中断恢复
//   - Run / ExportReport：整批运行与确定性报告导出
package mitmpin

import (
	"encoding/json"
	"os"
	"path/filepath"

	"mitmpin/internal/certutil"
	"mitmpin/internal/engine"
	"mitmpin/internal/generator"
	"mitmpin/internal/pin"
	"mitmpin/internal/pipeline"
	"mitmpin/internal/policy"
	"mitmpin/internal/replay"
	"mitmpin/internal/report"
	"mitmpin/internal/sample"
	"mitmpin/internal/store"
)

// 重新导出业务码，宿主服务可直接引用 mitmpin.E_MITM_SUSPECTED 等。
const (
	EOK                  = sample.E_OK
	EPolicyMissing       = sample.E_POLICY_MISSING
	EHashAlgoUnsupported = sample.E_HASH_ALGO_UNSUPPORTED
	EEventSeqGap         = sample.E_EVENT_SEQ_GAP
	EChainBroken         = sample.E_CHAIN_BROKEN
	EMITMSuspected       = sample.E_MITM_SUSPECTED
	ESNIMismatch         = sample.E_SNI_MISMATCH
	EProtocolNotAllowed  = sample.E_PROTOCOL_NOT_ALLOWED
	ESnapshotVersion     = sample.E_SNAPSHOT_VERSION
	EEventBudget         = sample.E_EVENT_BUDGET
	EGlobalBudget        = sample.E_GLOBAL_BUDGET
)

// 类型别名：宿主无需引入 internal 包即可使用全部数据结构。
type (
	Batch       = sample.Batch
	Observation = sample.Observation
	Event       = sample.Event
	Policy      = sample.Policy
	PinItem     = sample.PinItem
	Snapshot    = sample.Snapshot
	Budget      = sample.Budget
	FaultSpec   = sample.FaultSpec
	Verdict     = pin.Verdict
	ConnResult  = engine.ConnResult
	TraceEntry  = engine.TraceEntry
	ConnReplay  = replay.ConnReplay
	Report      = report.Report
	RunResult   = pipeline.Result
	Store       = store.Store
)

// LoadBatch 从 JSON 字节加载并校验批次（观测 + 策略 + 热更新 + 故障注入）。
func LoadBatch(data []byte) (Batch, error) {
	var b Batch
	if err := json.Unmarshal(data, &b); err != nil {
		return b, err
	}
	if err := sample.ValidateBatch(b); err != nil {
		return b, err
	}
	return b, nil
}

// LoadBatchFile 从配置文件加载批次。
func LoadBatchFile(path string) (Batch, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Batch{}, err
	}
	return LoadBatch(data)
}

// OpenStore 打开/创建 SQLite 数据文件（自动创建父目录）。
func OpenStore(dbPath string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, err
	}
	return store.Open(dbPath)
}

// ApplyPolicy 在单事务内提交策略快照；内容未变化返回 E_POLICY_UNCHANGED。
func ApplyPolicy(st *Store, snap Snapshot) error {
	return st.CommitSnapshot(snap)
}

// VerifyPinning 对单条观测执行固定校验与中间人判定（纯内存、无网络）。
func VerifyPinning(obs Observation, snap Snapshot) (Verdict, error) {
	if err := sample.ValidateObservation(obs, snap); err != nil {
		return Verdict{}, err
	}
	var policyDef Policy
	for _, candidate := range snap.Policies {
		if candidate.PolicyID == obs.PolicyID {
			policyDef = candidate
		}
	}
	chain, err := certutil.ParseChain(obs.ChainPEM)
	if err != nil {
		return Verdict{}, err
	}
	return pin.Evaluate(obs, chain, policyDef, snap.Version), nil
}

// ReplayObservation 交互侧独立重放单条观测，不读写检查点。
func ReplayObservation(obs Observation, snap Snapshot) (ConnReplay, error) {
	return replay.Replay(obs, snap)
}

// Recover 从最近已提交检查点恢复一条连接，未提交事件整体重做。
func Recover(st *Store, obs Observation, bound Snapshot, maxEvents int) (ConnResult, error) {
	eng := engine.New(st)
	return eng.Process(obs, bound, maxEvents)
}

// Run 执行完整管线并返回可回放结果。
func Run(st *Store, b Batch, budget Budget) (*RunResult, error) {
	p := pipeline.New(st)
	return p.Run(b, budget)
}

// InjectSnapshotVersionGap 在检查点表中制造版本链跳号（异常注入，不改源码）。
func InjectSnapshotVersionGap(st *Store, connID string) error {
	p := pipeline.New(st)
	return p.InjectSnapshotGap(connID)
}

// ExportReport 序列化确定性 JSON 报告。
func ExportReport(rep Report) ([]byte, error) {
	return rep.Marshal()
}

// PolicyContentHash 返回快照规范化内容哈希，用于识别整体替换式版本变化。
func PolicyContentHash(snap Snapshot) string {
	return policy.ContentHash(snap)
}

// GenerateSampleBatch 生成覆盖五类场景的确定性批次。
func GenerateSampleBatch() (Batch, error) {
	material, err := generator.BuildMaterial()
	if err != nil {
		return Batch{}, err
	}
	return generator.Batch(material)
}

// GenerateMaterial 暴露确定性证书素材，便于宿主自定义样本。
func GenerateMaterial() (*generator.Material, error) {
	return generator.BuildMaterial()
}
