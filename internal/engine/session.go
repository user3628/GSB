package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"vpnbench/internal/decision"
	"vpnbench/internal/model"
	"vpnbench/internal/monotonic"
)

type Config struct {
	SessionID     string
	Policy        model.PolicySet
	Requests      []model.Request
	RequestBudget int
	Clock         monotonic.Clock
}

type RequestAdjudication struct {
	StreamID              string         `json:"stream_id"`
	RequestSeq            int64          `json:"request_seq"`
	Source                string         `json:"source"`
	Destination           string         `json:"destination"`
	PacketBytes           int64          `json:"packet_bytes"`
	InputOffset           int64          `json:"input_offset"`
	ArrivalOrdinal        int64          `json:"arrival_ordinal"`
	SettlementOrdinal     int64          `json:"settlement_ordinal"`
	Interactive           model.Decision `json:"interactive_preliminary"`
	ReconciledInteractive model.Decision `json:"interactive_reconciled"`
	Settlement            model.Decision `json:"settlement_final"`
	Consistent            bool           `json:"consistent"`
	CorrectionCode        model.Code     `json:"correction_code,omitempty"`
}

type Consistency struct {
	Total      int                    `json:"total"`
	Consistent int                    `json:"consistent"`
	Basis      string                 `json:"basis"`
	Mismatches []model.CodeOccurrence `json:"mismatches"`
}

type Outcome struct {
	SessionID         string                      `json:"session_id"`
	WindowID          string                      `json:"window_id"`
	Status            string                      `json:"status"`
	PolicyVersion     int64                       `json:"policy_version"`
	PolicyDigest      string                      `json:"policy_digest"`
	RequestBudget     int                         `json:"request_budget"`
	RequestsProcessed int                         `json:"requests_processed"`
	Requests          []RequestAdjudication       `json:"requests"`
	Consistency       Consistency                 `json:"consistency"`
	ErrorLedger       []model.LedgerEntry         `json:"error_ledger"`
	ErrorByStream     []model.LedgerStreamSummary `json:"error_by_stream"`
	LatencySamples    []model.LatencySample       `json:"latency_samples"`
	Latency           []model.LatencyStat         `json:"latency"`
	LatencyMode       string                      `json:"latency_mode"`
	LatencyMarker     string                      `json:"latency_marker"`
	Corrections       []model.Correction          `json:"corrections"`
	BusinessCodes     []model.CodeOccurrence      `json:"business_codes"`
	InteractiveHash   string                      `json:"interactive_hash"`
	SettlementHash    string                      `json:"settlement_hash"`
}

func Run(cfg Config) (Outcome, error) {
	if cfg.Clock == nil {
		cfg.Clock = monotonic.NewFixed()
	}
	if cfg.RequestBudget < 0 {
		return Outcome{}, model.NewError(model.CodeRequestBudget, "request budget must be non-negative", 0).Locate(cfg.SessionID, "", 0)
	}
	engine, err := decision.New(cfg.Policy)
	if err != nil {
		return Outcome{}, err
	}

	outcome := Outcome{
		SessionID:     cfg.SessionID,
		PolicyVersion: cfg.Policy.Version,
		PolicyDigest:  cfg.Policy.Digest,
		RequestBudget: cfg.RequestBudget,
		Status:        "FINALIZED",
		LatencyMode:   cfg.Clock.Mode(),
		LatencyMarker: cfg.Clock.Marker(),
	}

	type mutableResult struct {
		RequestAdjudication
		key string
	}
	var processed []mutableResult
	seen := make(map[string]model.Request)
	ledgerSequence := int64(0)
	var consistencyMismatches []model.CodeOccurrence

	for _, request := range cfg.Requests {
		if cfg.RequestBudget >= 0 && len(processed) >= cfg.RequestBudget {
			outcome.Status = "TERMINATED_REQUEST_BUDGET"
			outcome.BusinessCodes = append(outcome.BusinessCodes, model.CodeOccurrence{
				Code:       model.CodeRequestBudget,
				SessionID:  cfg.SessionID,
				StreamID:   request.StreamID,
				RequestSeq: request.RequestSeq,
				Offset:     request.InputOffset,
				Message:    "per-session request budget exhausted; session terminated",
			})
			break
		}

		key := requestKey(request.StreamID, request.RequestSeq)
		if previous, duplicate := seen[key]; duplicate {
			outcome.Status = "FAILED_RESULT_MISMATCH"
			message := fmt.Sprintf("duplicate request key; first at byte %d", previous.InputOffset)
			mismatch := model.CodeOccurrence{
				Code:       model.CodeResultMismatch,
				SessionID:  cfg.SessionID,
				StreamID:   request.StreamID,
				RequestSeq: request.RequestSeq,
				Offset:     request.InputOffset,
				Message:    message,
			}
			outcome.BusinessCodes = append(outcome.BusinessCodes, mismatch)
			consistencyMismatches = append(consistencyMismatches, mismatch)
			break
		}
		seen[key] = request

		start := cfg.Clock.Now()
		decisionOutcome := engine.Decide(cfg.SessionID, request, ledgerSequence)
		elapsed := cfg.Clock.Since(start)
		if elapsed < 0 {
			elapsed = 0
		}
		tunnelID := ""
		if decisionOutcome.Decision.Code == model.CodeOK {
			tunnelID = decisionOutcome.Decision.PrimaryTunnelID
		}
		outcome.LatencySamples = append(outcome.LatencySamples, model.LatencySample{
			TunnelID:   tunnelID,
			ResultCode: decisionOutcome.Decision.Code,
			DurationNS: elapsed.Nanoseconds(),
		})
		if decisionOutcome.Error != nil {
			outcome.BusinessCodes = append(outcome.BusinessCodes, occurrenceFromError(decisionOutcome.Error))
		}
		outcome.ErrorLedger = append(outcome.ErrorLedger, decisionOutcome.Ledger...)
		ledgerSequence += int64(len(decisionOutcome.Ledger))

		processed = append(processed, mutableResult{
			RequestAdjudication: RequestAdjudication{
				StreamID:       request.StreamID,
				RequestSeq:     request.RequestSeq,
				Source:         request.Source,
				Destination:    request.Destination,
				PacketBytes:    request.PacketBytes,
				InputOffset:    request.InputOffset,
				ArrivalOrdinal: request.ArrivalOrdinal,
				Interactive:    cloneDecision(decisionOutcome.Decision),
				Settlement:     model.Decision{},
				Consistent:     false,
			},
			key: key,
		})
	}

	outOfOrderStreams := make(map[string]bool)
	lastSeqByStream := make(map[string]int64)
	for _, item := range processed {
		previousSeq, exists := lastSeqByStream[item.StreamID]
		if exists && item.RequestSeq < previousSeq {
			outOfOrderStreams[item.StreamID] = true
		}
		lastSeqByStream[item.StreamID] = item.RequestSeq
	}

	sort.SliceStable(processed, func(i, j int) bool {
		if processed[i].StreamID != processed[j].StreamID {
			return processed[i].StreamID < processed[j].StreamID
		}
		if processed[i].RequestSeq != processed[j].RequestSeq {
			return processed[i].RequestSeq < processed[j].RequestSeq
		}
		return processed[i].ArrivalOrdinal < processed[j].ArrivalOrdinal
	})

	mismatches := consistencyMismatches
	outcome.Requests = make([]RequestAdjudication, len(processed))
	for i := range processed {
		item := &processed[i]
		item.SettlementOrdinal = int64(i + 1)
		request := model.Request{
			StreamID:       item.StreamID,
			RequestSeq:     item.RequestSeq,
			Source:         item.Source,
			Destination:    item.Destination,
			PacketBytes:    item.PacketBytes,
			InputOffset:    item.InputOffset,
			ArrivalOrdinal: item.ArrivalOrdinal,
		}
		settled := engine.Decide(cfg.SessionID, request, 0).Decision
		item.Settlement = cloneDecision(settled)

		if outOfOrderStreams[item.StreamID] {
			item.CorrectionCode = model.CorrectionOrder
			outcome.Corrections = append(outcome.Corrections, model.Correction{
				SessionID:   cfg.SessionID,
				StreamID:    item.StreamID,
				RequestSeq:  item.RequestSeq,
				Code:        model.CorrectionOrder,
				Reason:      "settlement normalized out-of-order request sequence",
				Before:      cloneDecision(item.Interactive),
				After:       cloneDecision(settled),
				BeforeOrder: item.ArrivalOrdinal,
				AfterOrder:  item.SettlementOrdinal,
			})
		}

		if !decision.Equal(item.Interactive, settled) {
			item.CorrectionCode = model.CorrectionDecision
			outcome.Corrections = append(outcome.Corrections, model.Correction{
				SessionID:   cfg.SessionID,
				StreamID:    item.StreamID,
				RequestSeq:  item.RequestSeq,
				Code:        model.CorrectionDecision,
				Reason:      "settlement recomputation changed the preliminary decision",
				Before:      cloneDecision(item.Interactive),
				After:       cloneDecision(settled),
				BeforeOrder: item.ArrivalOrdinal,
				AfterOrder:  item.SettlementOrdinal,
			})
		}

		item.ReconciledInteractive = cloneDecision(settled)
		item.Consistent = decision.Equal(item.ReconciledInteractive, item.Settlement)
		if !item.Consistent {
			mismatch := model.CodeOccurrence{
				Code:       model.CodeResultMismatch,
				SessionID:  cfg.SessionID,
				StreamID:   item.StreamID,
				RequestSeq: item.RequestSeq,
				Offset:     item.InputOffset,
				Message:    "interactive and settlement adjudications differ after correction",
			}
			mismatches = append(mismatches, mismatch)
			outcome.BusinessCodes = append(outcome.BusinessCodes, mismatch)
			outcome.Status = "FAILED_RESULT_MISMATCH"
		}
		outcome.Requests[i] = item.RequestAdjudication
		outcome.RequestsProcessed++
	}

	outcome.Consistency = Consistency{
		Total:      len(outcome.Requests),
		Basis:      "reconciled_interactive_vs_settlement_final",
		Mismatches: mismatches,
	}
	for _, request := range outcome.Requests {
		if request.Consistent {
			outcome.Consistency.Consistent++
		}
	}
	outcome.ErrorByStream = decision.AggregateLedger(outcome.ErrorLedger)
	outcome.Latency = monotonic.Aggregate(outcome.LatencySamples)
	sort.SliceStable(outcome.Corrections, func(i, j int) bool {
		if outcome.Corrections[i].StreamID != outcome.Corrections[j].StreamID {
			return outcome.Corrections[i].StreamID < outcome.Corrections[j].StreamID
		}
		if outcome.Corrections[i].RequestSeq != outcome.Corrections[j].RequestSeq {
			return outcome.Corrections[i].RequestSeq < outcome.Corrections[j].RequestSeq
		}
		return outcome.Corrections[i].Code < outcome.Corrections[j].Code
	})
	sort.SliceStable(outcome.BusinessCodes, func(i, j int) bool {
		if outcome.BusinessCodes[i].StreamID != outcome.BusinessCodes[j].StreamID {
			return outcome.BusinessCodes[i].StreamID < outcome.BusinessCodes[j].StreamID
		}
		if outcome.BusinessCodes[i].RequestSeq != outcome.BusinessCodes[j].RequestSeq {
			return outcome.BusinessCodes[i].RequestSeq < outcome.BusinessCodes[j].RequestSeq
		}
		return outcome.BusinessCodes[i].Code < outcome.BusinessCodes[j].Code
	})

	outcome.WindowID = WindowID(cfg.SessionID, cfg.Policy, cfg.Requests)
	outcome.InteractiveHash = hashRequests(outcome.Requests, true)
	outcome.SettlementHash = hashRequests(outcome.Requests, false)
	return outcome, nil
}

func WindowID(sessionID string, policy model.PolicySet, requests []model.Request) string {
	type identity struct {
		SessionID     string `json:"session_id"`
		PolicyDigest  string `json:"policy_digest"`
		PolicyVersion int64  `json:"policy_version"`
		StreamID      string `json:"stream_id"`
		RequestSeq    int64  `json:"request_seq"`
		Source        string `json:"source"`
		Destination   string `json:"destination"`
		PacketBytes   int64  `json:"packet_bytes"`
	}
	identities := make([]identity, len(requests))
	for i, request := range requests {
		identities[i] = identity{
			SessionID:     sessionID,
			PolicyDigest:  policy.Digest,
			PolicyVersion: policy.Version,
			StreamID:      request.StreamID,
			RequestSeq:    request.RequestSeq,
			Source:        request.Source,
			Destination:   request.Destination,
			PacketBytes:   request.PacketBytes,
		}
	}
	sort.Slice(identities, func(i, j int) bool {
		if identities[i].StreamID != identities[j].StreamID {
			return identities[i].StreamID < identities[j].StreamID
		}
		if identities[i].RequestSeq != identities[j].RequestSeq {
			return identities[i].RequestSeq < identities[j].RequestSeq
		}
		return identities[i].Destination < identities[j].Destination
	})
	encoded, _ := json.Marshal(identities)
	sum := sha256.Sum256(encoded)
	return "w-" + hex.EncodeToString(sum[:8])
}

func occurrenceFromError(err *model.CodedError) model.CodeOccurrence {
	return model.CodeOccurrence{
		Code:       err.Code,
		SessionID:  err.SessionID,
		StreamID:   err.StreamID,
		RequestSeq: err.RequestSeq,
		Offset:     err.Offset,
		Message:    err.Message,
	}
}

func requestKey(streamID string, requestSeq int64) string {
	return fmt.Sprintf("%s\x00%d", streamID, requestSeq)
}

func cloneDecision(input model.Decision) model.Decision {
	output := input
	output.Allocations = append([]model.Allocation(nil), input.Allocations...)
	return output
}

func hashRequests(requests []RequestAdjudication, interactive bool) string {
	type hashRequest struct {
		StreamID   string         `json:"stream_id"`
		RequestSeq int64          `json:"request_seq"`
		Decision   model.Decision `json:"decision"`
	}
	hashInput := make([]hashRequest, len(requests))
	for i, request := range requests {
		hashInput[i] = hashRequest{StreamID: request.StreamID, RequestSeq: request.RequestSeq}
		if interactive {
			hashInput[i].Decision = request.ReconciledInteractive
		} else {
			hashInput[i].Decision = request.Settlement
		}
	}
	encoded, _ := json.Marshal(hashInput)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
