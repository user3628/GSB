package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"vpnbench/internal/model"
	"vpnbench/internal/report"
	"vpnbench/internal/scheduler"
	"vpnbench/internal/store"
)

func ReportFromDB(ctx context.Context, dbPath string) (report.Report, error) {
	database, err := store.Open(dbPath)
	if err != nil {
		return report.Report{}, err
	}
	defer database.Close()
	return reportFromStore(ctx, database)
}

func reportFromStore(ctx context.Context, database *store.Store) (report.Report, error) {
	policyChain, err := database.Chain(ctx, "policy")
	if err != nil {
		return report.Report{}, err
	}
	settlementChain, err := database.Chain(ctx, "settlement")
	if err != nil {
		return report.Report{}, err
	}
	windows, err := database.Windows(ctx)
	if err != nil {
		return report.Report{}, err
	}
	latestBySession := make(map[string]store.WindowRecord)
	for _, window := range windows {
		current, exists := latestBySession[window.SessionID]
		if !exists || window.SettlementVersion > current.SettlementVersion {
			latestBySession[window.SessionID] = window
		}
	}
	sessionIDs := make([]string, 0, len(latestBySession))
	for sessionID := range latestBySession {
		sessionIDs = append(sessionIDs, sessionID)
	}
	sort.Strings(sessionIDs)

	var settled []report.SettledSession
	var extraCodes []model.CodeOccurrence
	clock := report.ClockInfo{Mode: "monotonic-fixed-v1", Marker: "T_MONO_FIXED_250US_V1"}
	metrics := scheduler.Metrics{}
	var newestSettlementVersion int64 = -1
	extraSeen := make(map[string]struct{})
	for _, sessionID := range sessionIDs {
		window := latestBySession[sessionID]
		var persisted persistedSession
		if err := json.Unmarshal(window.OutcomeJSON, &persisted); err != nil {
			return report.Report{}, model.NewError(model.CodeInternal, "decode persisted session: "+err.Error(), 0)
		}
		if window.SettlementVersion > newestSettlementVersion {
			newestSettlementVersion = window.SettlementVersion
			clock = persisted.Clock
			metrics = persisted.Concurrency
		}
		for _, occurrence := range persisted.ExtraCodes {
			key := fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d\x00%s", occurrence.Code, occurrence.SessionID, occurrence.StreamID, occurrence.RequestSeq, occurrence.Offset, occurrence.Message)
			if _, exists := extraSeen[key]; exists {
				continue
			}
			extraSeen[key] = struct{}{}
			extraCodes = append(extraCodes, occurrence)
		}
		settled = append(settled, report.SettledSession{Outcome: persisted.Outcome, SettlementVersion: window.SettlementVersion})
	}
	if len(policyChain.MissingVersions) > 0 && !containsCode(extraCodes, model.CodeSnapshotVersion) {
		extraCodes = append(extraCodes, model.CodeOccurrence{
			Code:    model.CodeSnapshotVersion,
			Message: fmt.Sprintf("policy snapshot chain gap; fallback version %d", fallbackVersion(policyChain)),
		})
	}
	return report.Build(clock, metrics, policyChain, settlementChain, settled, extraCodes), nil
}

func containsCode(codes []model.CodeOccurrence, code model.Code) bool {
	for _, occurrence := range codes {
		if occurrence.Code == code {
			return true
		}
	}
	return false
}

type RecoverySnapshot struct {
	Kind             string   `json:"kind"`
	Version          int64    `json:"version"`
	ContentHash      string   `json:"content_hash"`
	ChangeType       string   `json:"change_type"`
	WholeReplacement bool     `json:"whole_replacement"`
	BeforeSummaryIDs []string `json:"before_summary_ids,omitempty"`
	AfterSummaryIDs  []string `json:"after_summary_ids,omitempty"`
}

type RecoveryChain struct {
	Kind            string             `json:"kind"`
	LatestVersion   int64              `json:"latest_version"`
	MissingVersions []int64            `json:"missing_versions"`
	FallbackVersion int64              `json:"fallback_version,omitempty"`
	Snapshots       []RecoverySnapshot `json:"snapshots"`
}

type RecoveryWindow struct {
	SessionID         string `json:"session_id"`
	WindowID          string `json:"window_id"`
	Status            string `json:"status"`
	PolicyVersion     int64  `json:"policy_version"`
	SettlementVersion int64  `json:"settlement_version"`
	RequestCount      int    `json:"request_count"`
}

type RecoveryReport struct {
	Schema        string                 `json:"schema"`
	Policy        RecoveryChain          `json:"policy"`
	Settlement    RecoveryChain          `json:"settlement"`
	Windows       []RecoveryWindow       `json:"windows"`
	BusinessCodes []model.CodeOccurrence `json:"business_codes"`
}

func Recover(ctx context.Context, dbPath string) (RecoveryReport, error) {
	database, err := store.Open(dbPath)
	if err != nil {
		return RecoveryReport{}, err
	}
	defer database.Close()
	policyChain, err := database.Chain(ctx, "policy")
	if err != nil {
		return RecoveryReport{}, err
	}
	settlementChain, err := database.Chain(ctx, "settlement")
	if err != nil {
		return RecoveryReport{}, err
	}
	windows, err := database.Windows(ctx)
	if err != nil {
		return RecoveryReport{}, err
	}
	reportValue := RecoveryReport{
		Schema:     "vpnbench.recovery/v1",
		Policy:     recoveryChain(policyChain),
		Settlement: recoveryChain(settlementChain),
		Windows:    make([]RecoveryWindow, 0, len(windows)),
	}
	if len(policyChain.MissingVersions) > 0 {
		reportValue.BusinessCodes = append(reportValue.BusinessCodes, model.CodeOccurrence{
			Code:    model.CodeSnapshotVersion,
			Message: "policy snapshot chain gap detected; latest contiguous fallback retained",
		})
	}
	if len(settlementChain.MissingVersions) > 0 {
		reportValue.BusinessCodes = append(reportValue.BusinessCodes, model.CodeOccurrence{
			Code:    model.CodeSnapshotVersion,
			Message: "settlement snapshot chain gap detected; latest contiguous fallback retained",
		})
	}
	for _, window := range windows {
		reportValue.Windows = append(reportValue.Windows, RecoveryWindow{
			SessionID:         window.SessionID,
			WindowID:          window.WindowID,
			Status:            window.Status,
			PolicyVersion:     window.PolicyVersion,
			SettlementVersion: window.SettlementVersion,
			RequestCount:      window.RequestCount,
		})
	}
	sort.SliceStable(reportValue.Windows, func(i, j int) bool {
		if reportValue.Windows[i].SessionID != reportValue.Windows[j].SessionID {
			return reportValue.Windows[i].SessionID < reportValue.Windows[j].SessionID
		}
		return reportValue.Windows[i].SettlementVersion < reportValue.Windows[j].SettlementVersion
	})
	return reportValue, nil
}

func (r RecoveryReport) Bytes() ([]byte, error) {
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func (r RecoveryReport) Write(path string) error {
	encoded, err := r.Bytes()
	if err != nil {
		return err
	}
	if directory := filepath.Dir(path); directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, encoded, 0o644)
}

func recoveryChain(chain store.Chain) RecoveryChain {
	result := RecoveryChain{
		Kind:            chain.Kind,
		LatestVersion:   chain.LatestVersion,
		MissingVersions: append([]int64(nil), chain.MissingVersions...),
		Snapshots:       make([]RecoverySnapshot, 0, len(chain.Snapshots)),
	}
	if result.MissingVersions == nil {
		result.MissingVersions = []int64{}
	}
	if chain.Fallback != nil {
		result.FallbackVersion = chain.Fallback.Version
	}
	for _, snapshot := range chain.Snapshots {
		result.Snapshots = append(result.Snapshots, RecoverySnapshot{
			Kind:             snapshot.Kind,
			Version:          snapshot.Version,
			ContentHash:      snapshot.ContentHash,
			ChangeType:       snapshot.ChangeType,
			WholeReplacement: snapshot.WholeReplacement,
			BeforeSummaryIDs: append([]string(nil), snapshot.BeforeSummary.IdentityKeys...),
			AfterSummaryIDs:  append([]string(nil), snapshot.AfterSummary.IdentityKeys...),
		})
	}
	return result
}

func fallbackVersion(chain store.Chain) int64 {
	if chain.Fallback == nil {
		return 0
	}
	return chain.Fallback.Version
}
