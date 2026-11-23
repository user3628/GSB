package mfa

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"sort"
)

type FactorReport struct {
	Factor
	RecoveryCodesTotal    int `json:"recovery_codes_total"`
	RecoveryCodesConsumed int `json:"recovery_codes_consumed"`
}

type ReportSummary struct {
	Total  int `json:"total"`
	Passed int `json:"passed"`
	Failed int `json:"failed"`
}

type ScenarioResult struct {
	Name           string   `json:"name"`
	ExpectedCode   Code     `json:"expected_code"`
	ActualCode     Code     `json:"actual_code"`
	Passed         bool     `json:"passed"`
	TenantID       string   `json:"tenant_id"`
	UserID         string   `json:"user_id"`
	FactorIDs      []string `json:"factor_ids"`
	SnapshotBefore int64    `json:"snapshot_before"`
	SnapshotAfter  int64    `json:"snapshot_after"`
	AttemptID      string   `json:"attempt_id,omitempty"`
	Detail         string   `json:"detail,omitempty"`
}

type FaultScenarioResult struct {
	Name            string         `json:"name"`
	Kind            FaultKind      `json:"kind"`
	ExpectedCode    Code           `json:"expected_code"`
	ActualCode      Code           `json:"actual_code"`
	Passed          bool           `json:"passed"`
	InvalidVersion  int64          `json:"invalid_version,omitempty"`
	FallbackVersion int64          `json:"fallback_version"`
	Reason          string         `json:"reason,omitempty"`
	SnapshotStatus  SnapshotStatus `json:"snapshot_status"`
}

type CrashRecoveryReport struct {
	BeforeCommitCode      Code   `json:"before_commit_code"`
	AfterCommitCode       Code   `json:"after_commit_code"`
	BeforeUsageCount      int    `json:"before_usage_count"`
	AfterUsageCount       int    `json:"after_usage_count"`
	BeforeSnapshotVersion int64  `json:"before_snapshot_version"`
	AfterSnapshotVersion  int64  `json:"after_snapshot_version"`
	Passed                bool   `json:"passed"`
	Detail                string `json:"detail,omitempty"`
}

type DiagnosticReport struct {
	SchemaVersion string                `json:"schema_version"`
	ClockUnix     int64                 `json:"clock_unix"`
	Summary       ReportSummary         `json:"summary"`
	Scenarios     []ScenarioResult      `json:"scenarios"`
	Snapshot      SnapshotStatus        `json:"snapshot_chain"`
	Factors       []FactorReport        `json:"factors"`
	UsageTraces   []UsageTrace          `json:"usage_traces"`
	RiskTraces    []RiskTrace           `json:"risk_traces"`
	RebindTraces  []RebindTrace         `json:"rebind_traces"`
	Faults        []FaultScenarioResult `json:"fault_injections"`
	Recovery      *CrashRecoveryReport  `json:"crash_recovery,omitempty"`
}

func (e *Engine) ListFactorReports(ctx context.Context) ([]FactorReport, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	factors, err := listFactorsTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	reports := make([]FactorReport, 0, len(factors))
	for _, factor := range factors {
		report := FactorReport{Factor: factor.Factor, RecoveryCodesTotal: len(factor.RecoveryCodes)}
		for _, code := range factor.RecoveryCodes {
			if code.Consumed {
				report.RecoveryCodesConsumed++
			}
		}
		reports = append(reports, report)
	}
	sort.Slice(reports, func(i, j int) bool {
		if reports[i].TenantID != reports[j].TenantID {
			return reports[i].TenantID < reports[j].TenantID
		}
		if reports[i].UserID != reports[j].UserID {
			return reports[i].UserID < reports[j].UserID
		}
		return reports[i].ID < reports[j].ID
	})
	return reports, nil
}

func (e *Engine) ListUsageTraces(ctx context.Context) ([]UsageTrace, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	rows, err := e.db.QueryContext(ctx, `
SELECT trace_id, tenant_id, user_id, factor_id, attempt_id, factor_type, outcome, code,
       challenge_prompt, response_sha256, snapshot_version, sequence
FROM usage_traces
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	traces := make([]UsageTrace, 0)
	for rows.Next() {
		var trace UsageTrace
		var factorType string
		if err := rows.Scan(
			&trace.TraceID, &trace.TenantID, &trace.UserID, &trace.FactorID,
			&trace.AttemptID, &factorType, &trace.Outcome, &trace.Code,
			&trace.ChallengePrompt, &trace.ResponseSHA, &trace.SnapshotVersion, &trace.Sequence,
		); err != nil {
			return nil, err
		}
		trace.FactorType = FactorType(factorType)
		traces = append(traces, trace)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(traces, func(i, j int) bool {
		return lessTraceIdentity(traces[i].TenantID, traces[i].UserID, traces[i].FactorID, traces[i].AttemptID, traces[i].Sequence,
			traces[j].TenantID, traces[j].UserID, traces[j].FactorID, traces[j].AttemptID, traces[j].Sequence)
	})
	return traces, nil
}

func (e *Engine) ListRiskTraces(ctx context.Context) ([]RiskTrace, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	rows, err := e.db.QueryContext(ctx, `
SELECT trace_id, tenant_id, user_id, factor_id, attempt_id, kind, reason,
       consecutive_failures, snapshot_version, sequence
FROM risk_traces
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	traces := make([]RiskTrace, 0)
	for rows.Next() {
		var trace RiskTrace
		if err := rows.Scan(&trace.TraceID, &trace.TenantID, &trace.UserID, &trace.FactorID,
			&trace.AttemptID, &trace.Kind, &trace.Reason, &trace.ConsecutiveFailures,
			&trace.SnapshotVersion, &trace.Sequence); err != nil {
			return nil, err
		}
		traces = append(traces, trace)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(traces, func(i, j int) bool {
		return lessTraceIdentity(traces[i].TenantID, traces[i].UserID, traces[i].FactorID, traces[i].AttemptID, traces[i].Sequence,
			traces[j].TenantID, traces[j].UserID, traces[j].FactorID, traces[j].AttemptID, traces[j].Sequence)
	})
	return traces, nil
}

func (e *Engine) ListRebindTraces(ctx context.Context) ([]RebindTrace, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	rows, err := e.db.QueryContext(ctx, `
SELECT trace_id, tenant_id, user_id, factor_type, old_factor_id, new_factor_id,
       attempt_id, status, code, snapshot_version, sequence
FROM rebind_traces
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	traces := make([]RebindTrace, 0)
	for rows.Next() {
		var trace RebindTrace
		var factorType string
		if err := rows.Scan(&trace.TraceID, &trace.TenantID, &trace.UserID, &factorType,
			&trace.OldFactorID, &trace.NewFactorID, &trace.AttemptID, &trace.Status,
			&trace.Code, &trace.SnapshotVersion, &trace.Sequence); err != nil {
			return nil, err
		}
		trace.FactorType = FactorType(factorType)
		traces = append(traces, trace)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(traces, func(i, j int) bool {
		return lessTraceIdentity(traces[i].TenantID, traces[i].UserID, traces[i].NewFactorID, traces[i].AttemptID, traces[i].Sequence,
			traces[j].TenantID, traces[j].UserID, traces[j].NewFactorID, traces[j].AttemptID, traces[j].Sequence)
	})
	return traces, nil
}

func lessTraceIdentity(at, au, af, aa string, as int64, bt, bu, bf, ba string, bs int64) bool {
	if at != bt {
		return at < bt
	}
	if au != bu {
		return au < bu
	}
	if af != bf {
		return af < bf
	}
	if aa != ba {
		return aa < ba
	}
	return as < bs
}

func (e *Engine) BuildDiagnosticReport(
	ctx context.Context,
	scenarios []ScenarioResult,
	faults []FaultScenarioResult,
	recovery *CrashRecoveryReport,
) (DiagnosticReport, error) {
	factors, err := e.ListFactorReports(ctx)
	if err != nil {
		return DiagnosticReport{}, err
	}
	usage, err := e.ListUsageTraces(ctx)
	if err != nil {
		return DiagnosticReport{}, err
	}
	risk, err := e.ListRiskTraces(ctx)
	if err != nil {
		return DiagnosticReport{}, err
	}
	rebinds, err := e.ListRebindTraces(ctx)
	if err != nil {
		return DiagnosticReport{}, err
	}
	status := e.SnapshotStatus(ctx)
	report := DiagnosticReport{
		SchemaVersion: "1.0",
		ClockUnix:     e.ClockUnix(),
		Scenarios:     append([]ScenarioResult(nil), scenarios...),
		Snapshot:      status,
		Factors:       factors,
		UsageTraces:   usage,
		RiskTraces:    risk,
		RebindTraces:  rebinds,
		Faults:        append([]FaultScenarioResult(nil), faults...),
		Recovery:      recovery,
	}
	sort.Slice(report.Scenarios, func(i, j int) bool { return report.Scenarios[i].Name < report.Scenarios[j].Name })
	sort.Slice(report.Faults, func(i, j int) bool { return report.Faults[i].Name < report.Faults[j].Name })
	report.Summary.Total = len(report.Scenarios)
	for _, scenario := range report.Scenarios {
		if scenario.Passed {
			report.Summary.Passed++
		} else {
			report.Summary.Failed++
		}
	}
	return report, nil
}

func WriteReport(writer io.Writer, report DiagnosticReport) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
