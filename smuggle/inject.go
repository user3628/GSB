package smuggle

// Anomaly 是异常注入样本类型，仅通过输入参数生效。
type Anomaly string

const (
	AnomalyCLTE             Anomaly = "cl-te"
	AnomalyCLConflict       Anomaly = "cl-conflict"
	AnomalyChunkLeadingZero Anomaly = "chunk-leading-zero"
	AnomalyObsFoldChunk     Anomaly = "obs-fold-chunk"
	AnomalyUnknownTE        Anomaly = "unknown-te"
)

// AllAnomalies 返回全部五类畸形样本类型。
func AllAnomalies() []Anomaly {
	return []Anomaly{
		AnomalyCLTE,
		AnomalyCLConflict,
		AnomalyChunkLeadingZero,
		AnomalyObsFoldChunk,
		AnomalyUnknownTE,
	}
}

// BuildAnomaly 构造指定类型的畸形报文连接样本。
func BuildAnomaly(a Anomaly) Connection {
	id := "inject/" + string(a)
	pair, ok := sampleSpecs[id]
	if !ok {
		return Connection{ID: id}
	}
	return Connection{ID: id, Proxy: []byte(pair[0]), Backend: []byte(pair[1])}
}

// expectedViewCode 给出各类畸形样本期望的视角解析业务码。
func expectedViewCode(a Anomaly) Code {
	switch a {
	case AnomalyCLTE:
		return CodeCLTEConflict
	case AnomalyCLConflict:
		return CodeCLConflict
	case AnomalyChunkLeadingZero:
		return CodeOK
	case AnomalyObsFoldChunk:
		return CodeChunkObsFold
	case AnomalyUnknownTE:
		return CodeTEUnknown
	}
	return CodeOK
}

// InjectionResult 记录一条异常注入链路的断言结果。
type InjectionResult struct {
	Anomaly            Anomaly `json:"anomaly"`
	Conn               string  `json:"conn"`
	ExpectedCode       Code    `json:"expected_code"`
	ObservedViewCode   Code    `json:"observed_view_code"`
	Verdict            Verdict `json:"verdict"`
	VerdictCode        Code    `json:"verdict_code"`
	SnapshotVersion    int64   `json:"snapshot_version"`
	ResubmittedVersion int64   `json:"resubmitted_version"`
	Isolated           bool    `json:"isolated"`
	Pass               bool    `json:"pass"`
}

// RunInjection 执行异常注入验证链路：
// 断言各类畸形样本的业务码、金丝雀连接不受污染、快照版本变化符合预期。
func RunInjection(eng *Engine, store *Store, anomalies []Anomaly) ([]InjectionResult, error) {
	canaries := []Connection{
		{ID: "inject/canary-a", Proxy: []byte(sampleSpecs["normal/consistent"][0]), Backend: []byte(sampleSpecs["normal/consistent"][1])},
		{ID: "inject/canary-b", Proxy: []byte(sampleSpecs["normal/consistent"][0]), Backend: []byte(sampleSpecs["normal/consistent"][1])},
	}
	var conns []Connection
	for _, a := range anomalies {
		conns = append(conns, BuildAnomaly(a))
	}
	conns = append(conns, canaries...)

	reports := eng.Run(conns)
	byConn := make(map[string]ConnReport, len(reports))
	for _, r := range reports {
		byConn[r.Conn] = r
	}

	isolated := true
	for _, c := range canaries {
		r := byConn[c.ID]
		if r.Verdict != VerdictConsistent || r.Code != CodeOK {
			isolated = false
		}
	}

	var results []InjectionResult
	for _, a := range anomalies {
		conn := BuildAnomaly(a)
		r := byConn[conn.ID]
		res := InjectionResult{
			Anomaly:          a,
			Conn:             conn.ID,
			ExpectedCode:     expectedViewCode(a),
			ObservedViewCode: r.ProxyCode,
			Verdict:          r.Verdict,
			VerdictCode:      r.Code,
			SnapshotVersion:  r.SnapshotVersion,
			Isolated:         isolated,
		}
		// 快照版本变化断言：同一连接再次提交必须递增。
		if store != nil {
			if snap, err := store.Commit(conn.ID, r.canonicalPayload()); err == nil {
				res.ResubmittedVersion = snap.Version
			}
		}
		res.Pass = res.ObservedViewCode == res.ExpectedCode &&
			res.Isolated &&
			res.SnapshotVersion >= 1 &&
			(res.ResubmittedVersion == 0 || res.ResubmittedVersion == res.SnapshotVersion+1)
		results = append(results, res)
	}
	return results, nil
}
