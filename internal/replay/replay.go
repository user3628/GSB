package replay

import (
	"fmt"
	"sort"

	"mitmpin/internal/certutil"
	"mitmpin/internal/engine"
	"mitmpin/internal/pin"
	"mitmpin/internal/sample"
)

// EventCheck 是单个 pin_check 事件的重放比对结果。
type EventCheck struct {
	ConnID         string `json:"conn_id"`
	EventSeq       int    `json:"event_seq"`
	ReplayCode     string `json:"replay_code"`
	SettlementCode string `json:"settlement_code"`
	Consistent     bool   `json:"consistent"`
}

// ConnReplay 是一条连接的重放结论。
type ConnReplay struct {
	ConnID        string       `json:"conn_id"`
	PolicyVersion int          `json:"policy_version"`
	ReplayCode    string       `json:"replay_code"`
	Checks        []EventCheck `json:"checks"`
	AllConsistent bool         `json:"all_consistent"`
}

// Replay 独立重放一条连接的全部 pin_check 事件，不读取/写入检查点与报告。
// bound 为连接开始时绑定的策略快照（与结算侧同源输入）。
func Replay(obs sample.Observation, bound sample.Snapshot) (ConnReplay, error) {
	if err := sample.ValidateObservation(obs, bound); err != nil {
		return ConnReplay{ConnID: obs.ConnID, PolicyVersion: bound.Version,
			ReplayCode: sample.CodeOf(err), AllConsistent: false}, err
	}
	var policyDef sample.Policy
	for _, policy := range bound.Policies {
		if policy.PolicyID == obs.PolicyID {
			policyDef = policy
		}
	}
	chain, err := certutil.ParseChain(obs.ChainPEM)
	out := ConnReplay{ConnID: obs.ConnID, PolicyVersion: bound.Version,
		ReplayCode: sample.E_OK, Checks: []EventCheck{}, AllConsistent: true}
	if err != nil {
		out.ReplayCode = sample.E_CHAIN_BROKEN
		out.AllConsistent = false
		return out, nil
	}
	for _, ev := range obs.Events {
		if ev.Type != sample.EvPinCheck {
			continue
		}
		verdict := pin.Evaluate(obs, chain, policyDef, bound.Version)
		out.Checks = append(out.Checks, EventCheck{
			ConnID: obs.ConnID, EventSeq: ev.Seq,
			ReplayCode: verdict.Code, Consistent: false,
		})
		if verdict.Code != sample.E_OK && out.ReplayCode == sample.E_OK {
			out.ReplayCode = verdict.Code
		}
	}
	return out, nil
}

// Compare 把重放结论与结算侧最终结论逐条比对。
func Compare(replayResult ConnReplay, settled engine.ConnResult) ConnReplay {
	settledBySeq := make(map[int]string)
	for _, entry := range settled.Trace {
		if entry.Type == sample.EvPinCheck {
			settledBySeq[entry.Seq] = entry.Code
		}
	}
	allConsistent := true
	for i := range replayResult.Checks {
		check := &replayResult.Checks[i]
		check.SettlementCode = settledBySeq[check.EventSeq]
		check.Consistent = check.ReplayCode == check.SettlementCode
		if !check.Consistent {
			allConsistent = false
		}
	}
	// 连接级主结论也须一致（预算截断等非固定码以结算侧为准时仅比对固定事件）。
	if replayResult.ReplayCode != settled.FinalCode &&
		settled.FinalCode != sample.E_EVENT_BUDGET &&
		settled.FinalCode != sample.E_GLOBAL_BUDGET &&
		settled.FinalCode != sample.EvAborted {
		allConsistent = false
	}
	replayResult.AllConsistent = allConsistent
	return replayResult
}

// BatchReplay 对整批连接重放并比对，按连接标识稳定排序。
func BatchReplay(obs []sample.Observation, binding map[string]sample.Snapshot,
	settled map[string]engine.ConnResult) ([]ConnReplay, error) {
	out := make([]ConnReplay, 0, len(obs))
	for _, o := range obs {
		bound, ok := binding[o.ConnID]
		if !ok {
			return nil, sample.NewBizError(sample.E_POLICY_MISSING,
				fmt.Sprintf("连接 %s 缺少策略绑定，无法重放", o.ConnID))
		}
		replayResult, err := Replay(o, bound)
		if err != nil {
			// 解析类错误同样生成可比对结论。
			replayResult.AllConsistent = false
		}
		if settledResult, ok := settled[o.ConnID]; ok {
			replayResult = Compare(replayResult, settledResult)
		}
		out = append(out, replayResult)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ConnID < out[j].ConnID })
	return out, nil
}
