package decision

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"sort"

	"vpnbench/internal/model"
)

const WeightScale int64 = 1_000_000

type compiledPolicy struct {
	policy  model.Policy
	network *net.IPNet
}

type Engine struct {
	policies []compiledPolicy
}

type Outcome struct {
	Decision model.Decision
	Ledger   []model.LedgerEntry
	Error    *model.CodedError
}

func New(policySet model.PolicySet) (*Engine, error) {
	compiled := make([]compiledPolicy, 0, len(policySet.Policies))
	for _, p := range policySet.Policies {
		_, network, err := net.ParseCIDR(p.DestinationCIDR)
		if err != nil {
			return nil, model.NewError(model.CodePolicyInvalid, "invalid normalized CIDR "+p.DestinationCIDR, p.InputOffset)
		}
		compiled = append(compiled, compiledPolicy{policy: p, network: network})
	}
	return &Engine{policies: compiled}, nil
}

// Decide selects active tunnels and allocates integer micro-units of weight.
//
// All matching routes are considered. Only routes at the highest matching
// priority are active; lower priorities are fallback routes and receive no
// weight for this request. Active routes are ordered by bandwidth descending,
// then tunnel ID ascending. Their weights are proportional to bandwidth in
// millionths. Floor truncations are recorded, and the integer remainder is
// distributed one micro-unit at a time by largest fractional remainder.
func (e *Engine) Decide(sessionID string, request model.Request, ledgerSequenceStart int64) Outcome {
	destination := net.ParseIP(request.Destination)
	if destination == nil {
		issue := model.NewError(model.CodeRequestInvalid, "destination is not a valid IP address", request.InputOffset).
			Locate(sessionID, request.StreamID, request.RequestSeq)
		return Outcome{Decision: model.Decision{Code: issue.Code}, Error: issue}
	}

	var candidates []compiledPolicy
	for _, candidate := range e.policies {
		if candidate.network.Contains(destination) {
			candidates = append(candidates, candidate)
		}
	}
	if len(candidates) == 0 {
		issue := model.NewError(model.CodeNoTunnel, "no tunnel policy matches destination "+request.Destination, request.InputOffset).
			Locate(sessionID, request.StreamID, request.RequestSeq)
		return Outcome{Decision: model.Decision{Code: issue.Code}, Error: issue}
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].policy.Priority != candidates[j].policy.Priority {
			return candidates[i].policy.Priority > candidates[j].policy.Priority
		}
		if candidates[i].policy.Bandwidth != candidates[j].policy.Bandwidth {
			return candidates[i].policy.Bandwidth > candidates[j].policy.Bandwidth
		}
		return candidates[i].policy.TunnelID < candidates[j].policy.TunnelID
	})
	topPriority := candidates[0].policy.Priority
	active := make([]compiledPolicy, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.policy.Priority != topPriority {
			break
		}
		active = append(active, candidate)
	}
	sort.Slice(active, func(i, j int) bool {
		if active[i].policy.Bandwidth != active[j].policy.Bandwidth {
			return active[i].policy.Bandwidth > active[j].policy.Bandwidth
		}
		return active[i].policy.TunnelID < active[j].policy.TunnelID
	})

	var totalBandwidth int64
	for _, candidate := range active {
		if candidate.policy.Bandwidth > math.MaxInt64/WeightScale {
			issue := model.NewError(model.CodeFixedOverflow, "bandwidth * weight scale overflows int64", candidate.policy.InputOffset).
				Locate(sessionID, request.StreamID, request.RequestSeq)
			return Outcome{Decision: model.Decision{Code: issue.Code}, Error: issue}
		}
		if candidate.policy.Bandwidth > math.MaxInt64-totalBandwidth {
			issue := model.NewError(model.CodeFixedOverflow, "bandwidth sum overflows int64", candidate.policy.InputOffset).
				Locate(sessionID, request.StreamID, request.RequestSeq)
			return Outcome{Decision: model.Decision{Code: issue.Code}, Error: issue}
		}
		totalBandwidth += candidate.policy.Bandwidth
	}
	if totalBandwidth == 0 {
		issue := model.NewError(model.CodePolicyInvalid, "active tunnels have zero aggregate bandwidth", request.InputOffset).
			Locate(sessionID, request.StreamID, request.RequestSeq)
		return Outcome{Decision: model.Decision{Code: issue.Code}, Error: issue}
	}

	type share struct {
		index              int
		numerator          int64
		remainderNumerator int64
		initialMicros      int64
		compensationMicros int64
	}
	shares := make([]share, len(active))
	var allocated int64
	for i, candidate := range active {
		numerator := candidate.policy.Bandwidth * WeightScale
		shares[i] = share{
			index:              i,
			numerator:          numerator,
			remainderNumerator: numerator % totalBandwidth,
			initialMicros:      numerator / totalBandwidth,
		}
		allocated += shares[i].initialMicros
	}
	remaining := WeightScale - allocated
	if remaining < 0 || remaining >= int64(len(shares)) {
		issue := model.NewError(model.CodeInternal, "invalid integer weight remainder", request.InputOffset).
			Locate(sessionID, request.StreamID, request.RequestSeq)
		return Outcome{Decision: model.Decision{Code: issue.Code}, Error: issue}
	}

	compensationOrder := append([]share(nil), shares...)
	sort.SliceStable(compensationOrder, func(i, j int) bool {
		if compensationOrder[i].remainderNumerator != compensationOrder[j].remainderNumerator {
			return compensationOrder[i].remainderNumerator > compensationOrder[j].remainderNumerator
		}
		return compensationOrder[i].index < compensationOrder[j].index
	})
	for i := int64(0); i < remaining; i++ {
		shares[compensationOrder[i].index].compensationMicros = 1
	}

	allocations := make([]model.Allocation, len(active))
	ledger := make([]model.LedgerEntry, len(active))
	var sumWeights int64
	for i, candidate := range active {
		weight := shares[i].initialMicros + shares[i].compensationMicros
		allocations[i] = model.Allocation{TunnelID: candidate.policy.TunnelID, WeightMicros: weight}
		sumWeights += weight
		upperBound := int64(0)
		if shares[i].remainderNumerator != 0 {
			upperBound = 1
		}
		ledger[i] = model.LedgerEntry{
			Sequence:             ledgerSequenceStart + int64(i),
			SessionID:            sessionID,
			StreamID:             request.StreamID,
			RequestSeq:           request.RequestSeq,
			TunnelID:             candidate.policy.TunnelID,
			ExactNumerator:       shares[i].numerator,
			Denominator:          totalBandwidth,
			InitialMicros:        shares[i].initialMicros,
			RemainderNumerator:   shares[i].remainderNumerator,
			CompensationMicros:   shares[i].compensationMicros,
			TruncationUpperBound: upperBound,
		}
	}
	if sumWeights != WeightScale {
		issue := model.NewError(model.CodeInternal, fmt.Sprintf("allocated weight %d does not equal scale %d", sumWeights, WeightScale), request.InputOffset).
			Locate(sessionID, request.StreamID, request.RequestSeq)
		return Outcome{Decision: model.Decision{Code: issue.Code}, Error: issue}
	}

	primary := active[0].policy
	return Outcome{
		Decision: model.Decision{
			Code:            model.CodeOK,
			PrimaryTunnelID: primary.TunnelID,
			Allocations:     allocations,
			ForwardingCost:  primary.ForwardingCost,
		},
		Ledger: ledger,
	}
}

func Fingerprint(decision model.Decision) string {
	encoded, _ := json.Marshal(decision)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func Equal(a, b model.Decision) bool {
	return Fingerprint(a) == Fingerprint(b)
}

func AggregateLedger(entries []model.LedgerEntry) []model.LedgerStreamSummary {
	byStream := make(map[string]*model.LedgerStreamSummary)
	order := make([]string, 0)
	for _, entry := range entries {
		summary, ok := byStream[entry.StreamID]
		if !ok {
			summary = &model.LedgerStreamSummary{StreamID: entry.StreamID}
			byStream[entry.StreamID] = summary
			order = append(order, entry.StreamID)
		}
		summary.AllocationCount++
		if entry.RemainderNumerator != 0 {
			summary.TruncationCount++
		}
		summary.CompensationMicros += entry.CompensationMicros
		summary.ErrorUpperBoundMicros += entry.TruncationUpperBound
	}
	sort.Strings(order)
	result := make([]model.LedgerStreamSummary, 0, len(order))
	for _, streamID := range order {
		result = append(result, *byStream[streamID])
	}
	return result
}
