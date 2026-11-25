package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"queryrewrite/internal/spec"
)

// Report is the full deterministic adjudication export.
type Report struct {
	PolicyVersion int               `json:"policy_version"`
	PolicyDigest  string            `json:"policy_digest"`
	Turns         []spec.TurnOutput `json:"turns"`
	Slots         []*spec.Slot      `json:"slots"`
	Summary       string            `json:"summary"`
}

// BuildReport reconstructs every turn output from committed checkpoints and
// appends a fixed summary digest, so a restart produces the same report bytes.
func (e *Engine) BuildReport() (Report, error) {
	cps, err := e.store.Checkpoints()
	if err != nil {
		return Report{}, err
	}
	turns := map[string]map[int]*spec.TurnOutput{}
	orderKeys := []string{}
	for _, cp := range cps {
		if _, ok := turns[cp.SessionID]; !ok {
			turns[cp.SessionID] = map[int]*spec.TurnOutput{}
			orderKeys = append(orderKeys, cp.SessionID)
		}
		to, ok := turns[cp.SessionID][cp.Turn]
		if !ok {
			to = &spec.TurnOutput{
				SessionID: cp.SessionID, Turn: cp.Turn,
				PolicyVer: cp.PolicyVersion, Digests: map[string]string{},
				Tokens: []spec.Token{}, Candidates: []spec.Candidate{},
				Hits: []spec.HitResult{}, Adj: []spec.AdjResult{},
				Ledger: spec.BudgetLedger{
					PerMilleBudget: map[string]int{}, PerMilleUsed: map[string]int{},
					Accepted: []string{}, Skips: []spec.Skip{}, DegradeReasons: []string{},
				},
			}
			turns[cp.SessionID][cp.Turn] = to
		}
		to.Digests[cp.Stage] = cp.ProductDigest
		switch cp.Stage {
		case spec.StageGenerate:
			var prod struct {
				Tokens     []spec.Token     `json:"tokens"`
				Candidates []spec.Candidate `json:"candidates"`
				Error      *spec.BizError   `json:"error"`
			}
			_ = json.Unmarshal([]byte(cp.Payload), &prod)
			to.Tokens, to.Candidates, to.StageError = prod.Tokens, prod.Candidates, prod.Error
			if to.Tokens == nil {
				to.Tokens = []spec.Token{}
			}
			if to.Candidates == nil {
				to.Candidates = []spec.Candidate{}
			}
		case spec.StagePrefix:
			var prod struct {
				Hits []spec.HitResult `json:"hits"`
			}
			_ = json.Unmarshal([]byte(cp.Payload), &prod)
			to.Hits = prod.Hits
		case spec.StageAdjudge:
			var prod struct {
				Adj []spec.AdjResult `json:"adjudications"`
			}
			_ = json.Unmarshal([]byte(cp.Payload), &prod)
			to.Adj = prod.Adj
		case spec.StageSchedule:
			var prod struct {
				Ledger spec.BudgetLedger `json:"ledger"`
			}
			_ = json.Unmarshal([]byte(cp.Payload), &prod)
			to.Ledger = prod.Ledger
		}
	}

	flat := make([]spec.TurnOutput, 0)
	seen := map[string]bool{}
	for _, cp := range cps {
		key := cp.SessionID + "#" + itoa(cp.Turn)
		if seen[key] {
			continue
		}
		seen[key] = true
		flat = append(flat, *turns[cp.SessionID][cp.Turn])
	}
	ver, dig := e.VersionFingerprint()
	rep := Report{
		PolicyVersion: ver, PolicyDigest: dig,
		Turns: flat, Slots: e.cache.Slots(),
	}
	rep.Summary = summarize(rep)
	return rep, nil
}

func summarize(r Report) string {
	b, _ := json.Marshal(struct {
		V     int               `json:"v"`
		D     string            `json:"d"`
		Turns []spec.TurnOutput `json:"turns"`
		Slots []*spec.Slot      `json:"slots"`
	}{r.PolicyVersion, r.PolicyDigest, r.Turns, r.Slots})
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
