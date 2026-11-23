package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"vpnbench/internal/engine"
	"vpnbench/internal/model"
	"vpnbench/internal/scheduler"
	"vpnbench/internal/store"
)

const Schema = "vpnbench.report/v1"

type ClockInfo struct {
	Mode   string `json:"mode"`
	Marker string `json:"marker"`
}

type Snapshots struct {
	Policy     ChainInfo `json:"policy"`
	Settlement ChainInfo `json:"settlement"`
}

type ChainInfo struct {
	LatestVersion   int64          `json:"latest_version"`
	MissingVersions []int64        `json:"missing_versions"`
	GapDetected     bool           `json:"gap_detected"`
	FallbackVersion int64          `json:"fallback_version,omitempty"`
	Entries         []SnapshotInfo `json:"entries"`
}

type SnapshotInfo struct {
	Kind             string        `json:"kind"`
	Version          int64         `json:"version"`
	ContentHash      string        `json:"content_hash"`
	ChangeType       string        `json:"change_type"`
	WholeReplacement bool          `json:"whole_replacement"`
	BeforeHash       string        `json:"before_hash,omitempty"`
	AfterHash        string        `json:"after_hash"`
	BeforeSummary    store.Summary `json:"before_summary"`
	AfterSummary     store.Summary `json:"after_summary"`
	PolicyVersion    int64         `json:"policy_version,omitempty"`
	WindowID         string        `json:"window_id,omitempty"`
	SessionID        string        `json:"session_id,omitempty"`
}

type SettledSession struct {
	Outcome           engine.Outcome
	SettlementVersion int64
}

type SessionSummary struct {
	OK             int `json:"ok"`
	NoTunnel       int `json:"no_tunnel"`
	FixedOverflow  int `json:"fixed_overflow"`
	RequestInvalid int `json:"request_invalid"`
	PolicyInvalid  int `json:"policy_invalid"`
	Corrections    int `json:"corrections"`
	LedgerEntries  int `json:"ledger_entries"`
	BusinessCodes  int `json:"business_codes"`
}

type SessionReport struct {
	SessionID         string                       `json:"session_id"`
	WindowID          string                       `json:"window_id"`
	Status            string                       `json:"status"`
	PolicyVersion     int64                        `json:"policy_version"`
	PolicyDigest      string                       `json:"policy_digest"`
	RequestBudget     int                          `json:"request_budget"`
	RequestsProcessed int                          `json:"requests_processed"`
	SettlementVersion int64                        `json:"settlement_version"`
	Summary           SessionSummary               `json:"summary"`
	Consistency       engine.Consistency           `json:"consistency"`
	Requests          []engine.RequestAdjudication `json:"requests"`
	ErrorLedger       []model.LedgerEntry          `json:"error_ledger"`
	ErrorByStream     []model.LedgerStreamSummary  `json:"error_by_stream"`
	LatencyMode       string                       `json:"latency_mode"`
	LatencyMarker     string                       `json:"latency_marker"`
	LatencySamples    []model.LatencySample        `json:"latency_samples"`
	Latency           []model.LatencyStat          `json:"latency"`
	Corrections       []model.Correction           `json:"corrections"`
	BusinessCodes     []model.CodeOccurrence       `json:"business_codes"`
	InteractiveHash   string                       `json:"interactive_hash"`
	SettlementHash    string                       `json:"settlement_hash"`
}

type Report struct {
	Schema        string                 `json:"schema"`
	Clock         ClockInfo              `json:"clock"`
	Concurrency   scheduler.Metrics      `json:"concurrency"`
	Snapshots     Snapshots              `json:"snapshots"`
	Sessions      []SessionReport        `json:"sessions"`
	BusinessCodes []model.CodeOccurrence `json:"business_codes"`
}

func Build(clock ClockInfo, concurrency scheduler.Metrics, policyChain, settlementChain store.Chain, settled []SettledSession, extraCodes []model.CodeOccurrence) Report {
	sort.SliceStable(settled, func(i, j int) bool {
		return settled[i].Outcome.SessionID < settled[j].Outcome.SessionID
	})
	result := Report{
		Schema:        Schema,
		Clock:         clock,
		Concurrency:   concurrency,
		Snapshots:     Snapshots{Policy: chainInfo(policyChain), Settlement: chainInfo(settlementChain)},
		Sessions:      make([]SessionReport, 0, len(settled)),
		BusinessCodes: append([]model.CodeOccurrence(nil), extraCodes...),
	}
	for _, item := range settled {
		outcome := item.Outcome
		report := SessionReport{
			SessionID:         outcome.SessionID,
			WindowID:          outcome.WindowID,
			Status:            outcome.Status,
			PolicyVersion:     outcome.PolicyVersion,
			PolicyDigest:      outcome.PolicyDigest,
			RequestBudget:     outcome.RequestBudget,
			RequestsProcessed: outcome.RequestsProcessed,
			SettlementVersion: item.SettlementVersion,
			Consistency:       outcome.Consistency,
			Requests:          append([]engine.RequestAdjudication(nil), outcome.Requests...),
			ErrorLedger:       append([]model.LedgerEntry(nil), outcome.ErrorLedger...),
			ErrorByStream:     append([]model.LedgerStreamSummary(nil), outcome.ErrorByStream...),
			LatencyMode:       outcome.LatencyMode,
			LatencyMarker:     outcome.LatencyMarker,
			LatencySamples:    append([]model.LatencySample(nil), outcome.LatencySamples...),
			Latency:           append([]model.LatencyStat(nil), outcome.Latency...),
			Corrections:       append([]model.Correction(nil), outcome.Corrections...),
			BusinessCodes:     append([]model.CodeOccurrence(nil), outcome.BusinessCodes...),
			InteractiveHash:   outcome.InteractiveHash,
			SettlementHash:    outcome.SettlementHash,
		}
		report.Summary = summarize(outcome)
		result.Sessions = append(result.Sessions, report)
		result.BusinessCodes = append(result.BusinessCodes, outcome.BusinessCodes...)
	}
	sort.SliceStable(result.BusinessCodes, func(i, j int) bool {
		left, right := result.BusinessCodes[i], result.BusinessCodes[j]
		if left.SessionID != right.SessionID {
			return left.SessionID < right.SessionID
		}
		if left.StreamID != right.StreamID {
			return left.StreamID < right.StreamID
		}
		if left.RequestSeq != right.RequestSeq {
			return left.RequestSeq < right.RequestSeq
		}
		if left.Code != right.Code {
			return left.Code < right.Code
		}
		return left.Offset < right.Offset
	})
	return result
}

func summarize(outcome engine.Outcome) SessionSummary {
	summary := SessionSummary{
		Corrections:   len(outcome.Corrections),
		LedgerEntries: len(outcome.ErrorLedger),
		BusinessCodes: len(outcome.BusinessCodes),
	}
	for _, request := range outcome.Requests {
		switch request.Settlement.Code {
		case model.CodeOK:
			summary.OK++
		case model.CodeNoTunnel:
			summary.NoTunnel++
		case model.CodeFixedOverflow:
			summary.FixedOverflow++
		case model.CodeRequestInvalid:
			summary.RequestInvalid++
		case model.CodePolicyInvalid:
			summary.PolicyInvalid++
		}
	}
	return summary
}

func chainInfo(chain store.Chain) ChainInfo {
	info := ChainInfo{
		LatestVersion:   chain.LatestVersion,
		MissingVersions: append([]int64(nil), chain.MissingVersions...),
		GapDetected:     len(chain.MissingVersions) > 0,
		Entries:         make([]SnapshotInfo, 0, len(chain.Snapshots)),
	}
	if info.MissingVersions == nil {
		info.MissingVersions = []int64{}
	}
	if chain.Fallback != nil {
		info.FallbackVersion = chain.Fallback.Version
	}
	for _, snapshot := range chain.Snapshots {
		info.Entries = append(info.Entries, SnapshotInfo{
			Kind:             snapshot.Kind,
			Version:          snapshot.Version,
			ContentHash:      snapshot.ContentHash,
			ChangeType:       snapshot.ChangeType,
			WholeReplacement: snapshot.WholeReplacement,
			BeforeHash:       snapshot.BeforeHash,
			AfterHash:        snapshot.AfterHash,
			BeforeSummary:    snapshot.BeforeSummary,
			AfterSummary:     snapshot.AfterSummary,
			PolicyVersion:    snapshot.PolicyVersion,
			WindowID:         snapshot.WindowID,
			SessionID:        snapshot.SessionID,
		})
	}
	return info
}

func (r Report) Bytes() ([]byte, error) {
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func (r Report) Write(path string) error {
	if path == "" {
		return fmt.Errorf("report path is empty")
	}
	encoded, err := r.Bytes()
	if err != nil {
		return err
	}
	if directory := filepath.Dir(path); directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return fmt.Errorf("create report directory: %w", err)
		}
	}
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}
