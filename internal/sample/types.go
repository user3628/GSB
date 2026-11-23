package sample

// 事件类型：覆盖观测窗口内一次 TLS 连接的状态演进。
const (
	EvHandshakeStart = "handshake_start"
	EvCertReceived   = "cert_received"
	EvPinCheck       = "pin_check"
	EvHandshakeDone  = "handshake_done"
	EvAborted        = "aborted"
)

// 固定策略的两种形态。
const (
	PinModeSPKI = "spki_pins"     // 显式 SPKI 哈希集合
	PinModeRoot = "trusted_roots" // 受信根集合，执行路径构建与验证
)

// Event 是连接状态演进中的单个事件，Seq 从 1 开始且须连续。
type Event struct {
	Seq    int    `json:"seq"`
	Type   string `json:"type"`
	Marker string `json:"marker"`         // 固定采样标记，代替真实时间戳
	Note   string `json:"note,omitempty"` // 异常终止原因等可读说明
}

// PinItem 是单条固定项：算法 + base64 标准编码的 SPKI 摘要，或受信根 PEM。
type PinItem struct {
	ID      string `json:"id"`
	Algo    string `json:"algo,omitempty"`     // sha256 / sha384（受信根模式可空）
	Digest  string `json:"digest,omitempty"`   // base64(SPKI 摘要)
	RootPEM string `json:"root_pem,omitempty"` // 受信根模式下的根证书 PEM
}

// Policy 是某个服务（按 PolicyID 标识）的固定策略。
type Policy struct {
	PolicyID        string    `json:"policy_id"`
	Mode            string    `json:"mode"`
	AllowedVersions []string  `json:"allowed_versions"` // 允许的 TLS 协议版本
	Pins            []PinItem `json:"pins"`
}

// Snapshot 是带递增版本号的策略快照，更新时整体替换生效集合。
type Snapshot struct {
	Version  int      `json:"version"`
	Marker   string   `json:"marker"`
	Policies []Policy `json:"policies"`
}

// Observation 是一次 TLS 连接的确定性观测记录。
type Observation struct {
	ConnID   string  `json:"conn_id"`
	PolicyID string  `json:"policy_id"` // 引用生效策略标识
	SNI      string  `json:"sni"`
	Protocol string  `json:"protocol"`  // tls1.2 / tls1.3
	ChainPEM string  `json:"chain_pem"` // 叶 -> 中间 -> ... -> 根 的 PEM 串
	Events   []Event `json:"events"`
}

// UpdateStep 描述批次内一次策略热更新的生效时机与新快照。
// AfterSeq 表示全局调度序中该更新在第 AfterSeq 个连接开始之前生效。
type UpdateStep struct {
	AfterSeq int      `json:"after_seq"`
	Snapshot Snapshot `json:"snapshot"`
}

// FaultSpec 是通过命令行/配置注入的异常链路，不修改任何源码。
type FaultSpec struct {
	// pin_mismatch / hot_update_race / chain_broken / event_seq_gap /
	// snapshot_version_gap
	Kind     string `json:"kind"`
	Target   string `json:"target"` // 受影响连接标识（或快照标识）
	PolicyID string `json:"policy_id,omitempty"`
}

// Batch 是一次管线运行的完整输入。
type Batch struct {
	BatchID      string        `json:"batch_id"`
	Marker       string        `json:"marker"`
	Snapshots    []Snapshot    `json:"snapshots"`
	Updates      []UpdateStep  `json:"updates,omitempty"`
	Observations []Observation `json:"observations"`
	Faults       []FaultSpec   `json:"faults,omitempty"`
}

// Budget 是资源预算参数。
type Budget struct {
	Concurrency     int `json:"concurrency"`        // 并发度上限
	GlobalMemory    int `json:"global_memory"`      // 全局内存预算（事件成本单位）
	MaxEventPerConn int `json:"max_event_per_conn"` // 单连接事件数上限
}
