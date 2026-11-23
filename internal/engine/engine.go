package engine

import (
	"encoding/json"
	"fmt"

	"mitmpin/internal/certutil"
	"mitmpin/internal/pin"
	"mitmpin/internal/sample"
	"mitmpin/internal/store"
)

// TraceEntry 是事件轨迹中的逐事件记录，绑定策略版本全程可追溯。
type TraceEntry struct {
	Seq           int          `json:"seq"`
	Type          string       `json:"type"`
	Marker        string       `json:"marker"`
	PolicyVersion int          `json:"policy_version"`
	Code          string       `json:"code"`
	PinVerdict    *pin.Verdict `json:"pin_verdict,omitempty"`
	Detail        string       `json:"detail,omitempty"`
}

// ConnResult 是一条连接的结算结论。
type ConnResult struct {
	ConnID          string       `json:"conn_id"`
	PolicyVersion   int          `json:"policy_version"`
	FinalCode       string       `json:"final_code"`
	ProcessedEvents int          `json:"processed_events"`
	Trace           []TraceEntry `json:"trace"`
}

// Engine 负责单条连接的重放、检查点与恢复。
type Engine struct {
	store *store.Store
}

// New 构造引擎。
func New(st *store.Store) *Engine { return &Engine{store: st} }

func lookupPolicy(snap sample.Snapshot, policyID string) (sample.Policy, error) {
	for _, policy := range snap.Policies {
		if policy.PolicyID == policyID {
			return policy, nil
		}
	}
	return sample.Policy{}, sample.NewBizError(sample.E_POLICY_MISSING,
		fmt.Sprintf("策略 %s 在快照 v%d 中缺失", policyID, snap.Version))
}

// Process 重放一条连接，maxEvents 为单连接事件数上限（<=0 表示不限）。
// 从已提交检查点继续；未提交事件整体重做。返回结算结论与业务码。
func (e *Engine) Process(obs sample.Observation, bound sample.Snapshot, maxEvents int) (ConnResult, error) {
	if err := sample.ValidateObservation(obs, bound); err != nil {
		res := ConnResult{ConnID: obs.ConnID, PolicyVersion: bound.Version,
			FinalCode: sample.CodeOf(err), Trace: []TraceEntry{parseFailureEntry(obs, bound, err)}}
		_ = e.persistResult(obs, res)
		return res, err
	}
	policyDef, err := lookupPolicy(bound, obs.PolicyID)
	if err != nil {
		res := ConnResult{ConnID: obs.ConnID, PolicyVersion: bound.Version,
			FinalCode: sample.CodeOf(err), Trace: []TraceEntry{parseFailureEntry(obs, bound, err)}}
		_ = e.persistResult(obs, res)
		return res, err
	}

	// 恢复：校验检查点版本链连续性，跳号返回 E_SNAPSHOT_VERSION 并回退。
	trace, nextSeq, err := e.recoverTrace(obs.ConnID)
	if err != nil {
		return ConnResult{ConnID: obs.ConnID, PolicyVersion: bound.Version,
			FinalCode: sample.CodeOf(err), Trace: trace}, err
	}

	chain, chainErr := certutil.ParseChain(obs.ChainPEM)
	finalCode := sample.E_OK
	budgetCode := ""

	for idx := nextSeq - 1; idx < len(obs.Events); idx++ {
		ev := obs.Events[idx]

		// 单连接事件预算：超限终止，保留已处理事件结论。
		if maxEvents > 0 && ev.Seq > maxEvents {
			budgetCode = sample.E_EVENT_BUDGET
			break
		}

		entry := TraceEntry{Seq: ev.Seq, Type: ev.Type, Marker: ev.Marker,
			PolicyVersion: bound.Version, Code: sample.E_OK}

		switch ev.Type {
		case sample.EvHandshakeStart:
			entry.Detail = fmt.Sprintf("绑定策略快照 v%d", bound.Version)
		case sample.EvCertReceived:
			if chainErr != nil {
				entry.Code = sample.E_CHAIN_BROKEN
				entry.Detail = chainErr.Error()
				finalCode = sample.E_CHAIN_BROKEN
			}
		case sample.EvPinCheck:
			if chainErr != nil {
				entry.Code = sample.E_CHAIN_BROKEN
				entry.Detail = chainErr.Error()
				finalCode = sample.E_CHAIN_BROKEN
			} else {
				verdict := pin.Evaluate(obs, chain, policyDef, bound.Version)
				entry.PinVerdict = &verdict
				entry.Code = verdict.Code
				if verdict.Code != sample.E_OK && finalCode == sample.E_OK {
					finalCode = verdict.Code
				}
			}
		case sample.EvHandshakeDone:
			entry.Detail = "握手完成"
		case sample.EvAborted:
			entry.Detail = ev.Note
			if finalCode == sample.E_OK {
				finalCode = sample.EvAborted
			}
		}

		trace = append(trace, entry)
		if err := e.commitCheckpoint(obs, bound.Version, entry, trace); err != nil {
			return ConnResult{ConnID: obs.ConnID, PolicyVersion: bound.Version,
				FinalCode: sample.CodeOf(err), Trace: trace}, err
		}

		// 致命业务码（链断裂/MITM/SNI/协议）在产生后终止后续重放。
		if isTerminal(entry.Code) {
			break
		}
	}

	if budgetCode != "" {
		finalCode = budgetCode
	}
	result := ConnResult{
		ConnID:          obs.ConnID,
		PolicyVersion:   bound.Version,
		FinalCode:       finalCode,
		ProcessedEvents: len(trace),
		Trace:           trace,
	}
	if err := e.persistResult(obs, result); err != nil {
		return result, err
	}
	return result, nil
}

// persistResult 把连接结算终态写入 SQLite。
func (e *Engine) persistResult(obs sample.Observation, result ConnResult) error {
	traceJSON, _ := json.Marshal(result.Trace)
	firstMarker := ""
	if len(obs.Events) > 0 {
		firstMarker = obs.Events[0].Marker
	}
	return e.store.SaveResult(obs.ConnID, result.PolicyVersion, result.FinalCode,
		len(result.Trace), string(traceJSON), firstMarker)
}

func isTerminal(code string) bool {
	switch code {
	case sample.E_CHAIN_BROKEN, sample.E_MITM_SUSPECTED,
		sample.E_SNI_MISMATCH, sample.E_PROTOCOL_NOT_ALLOWED:
		return true
	}
	return false
}

// recoverTrace 校验并恢复已提交检查点，返回连续轨迹与下一个待处理事件序号。
// 检查点版本链跳号时返回 E_SNAPSHOT_VERSION，并回退到最近连续版本。
func (e *Engine) recoverTrace(connID string) ([]TraceEntry, int, error) {
	cps, err := e.store.ListCheckpoints(connID)
	if err != nil {
		return nil, 1, err
	}
	if len(cps) == 0 {
		return []TraceEntry{}, 1, nil
	}

	keep := 0
	var gapErr error
	for i, cp := range cps {
		want := i + 1
		if cp.CheckpointVer != want {
			gapErr = sample.NewBizError(sample.E_SNAPSHOT_VERSION,
				fmt.Sprintf("连接 %s 检查点版本链跳号: 期望 %d 实际 %d", connID, want, cp.CheckpointVer))
			break
		}
		keep = cp.CheckpointVer
	}

	trace := make([]TraceEntry, 0, keep)
	for i := 0; i < keep; i++ {
		var entries []TraceEntry
		if err := json.Unmarshal([]byte(cps[i].TraceJSON), &entries); err != nil {
			return nil, 1, err
		}
		if len(entries) > 0 {
			trace = append(trace, entries[len(entries)-1])
		}
	}

	if gapErr != nil {
		if err := e.store.RollbackCheckpointsAfter(connID, keep); err != nil {
			return trace, keep + 1, err
		}
		// 已提交结论不回退：恢复后从最近连续版本之后重做。
		return trace, keep + 1, gapErr
	}

	nextSeq := keep + 1
	if len(cps) > 0 {
		nextSeq = cps[len(cps)-1].EventSeq + 1
	}
	return trace, nextSeq, nil
}

func (e *Engine) commitCheckpoint(obs sample.Observation, policyVersion int, entry TraceEntry, trace []TraceEntry) error {
	pinCode := ""
	if entry.PinVerdict != nil {
		pinCode = entry.PinVerdict.Code
	}
	traceJSON, err := json.Marshal(trace)
	if err != nil {
		return err
	}
	cp := store.Checkpoint{
		ConnID:        obs.ConnID,
		CheckpointVer: entry.Seq,
		EventSeq:      entry.Seq,
		EventType:     entry.Type,
		PolicyVersion: policyVersion,
		PinCode:       pinCode,
		TraceJSON:     string(traceJSON),
		Marker:        entry.Marker,
	}
	return e.store.SaveCheckpoint(cp)
}

// parseFailureEntry 为解析期失败生成可追溯的首事件轨迹。
func parseFailureEntry(obs sample.Observation, bound sample.Snapshot, err error) TraceEntry {
	marker := ""
	evType := sample.EvHandshakeStart
	if len(obs.Events) > 0 {
		marker = obs.Events[0].Marker
		evType = obs.Events[0].Type
	}
	return TraceEntry{
		Seq: 1, Type: evType, Marker: marker, PolicyVersion: bound.Version,
		Code: sample.CodeOf(err), Detail: err.Error(),
	}
}
