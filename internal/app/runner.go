package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"auditdrift/internal/drift"
	"auditdrift/internal/model"
	"auditdrift/internal/probe"
	"auditdrift/internal/sample"
	"auditdrift/internal/schedule"
	"auditdrift/internal/store"
)

const DefaultSession = "demo-baseline-audit"

func WriteSeedDirectory(dir string, set *sample.SampleSet) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	baselinesJSON, err := json.MarshalIndent(set.Baselines, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "baselines.json"), baselinesJSON, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "observations.jsonl"), set.JSONL, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "observations.legacy.lpb"), set.LegacyBlock, 0o644); err != nil {
		return err
	}
	manifest := map[string]any{
		"seed": set.Seed, "hosts": set.Hosts, "baseline_count": len(set.Baselines),
		"observation_count": len(set.Observations),
		"files":             []string{"baselines.json", "observations.jsonl", "observations.legacy.lpb"},
		"legacy_format":     "AUDITDRIFT-LPB/1 length-prefixed record and field frames; timestamp_us",
	}
	manifestJSON, _ := json.MarshalIndent(manifest, "", "  ")
	return os.WriteFile(filepath.Join(dir, "manifest.json"), manifestJSON, 0o644)
}

func InitializeDatabase(dbPath, sessionID string, quota int) (*store.InitializeResult, error) {
	set := sample.FixedSet()
	st, err := store.Open(dbPath)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	return st.InitializeSession(store.InitializeInput{
		SessionID: sessionID, Hosts: set.Hosts, Baselines: set.Baselines,
		Observations: set.Observations, PropagationMaxHops: 3, RepairQuota: quota,
	})
}

type AuditOptions struct {
	SessionID            string
	Concurrency          int
	QueueSize            int
	Budget               time.Duration
	ProbeTimeout         time.Duration
	ProbeBaseURL         string
	UseInProcessEndpoint bool
	EndpointDelayID      string
	EndpointDelay        time.Duration
}

type TaskReport struct {
	Key           string              `json:"key"`
	Host          string              `json:"host"`
	BaselineID    string              `json:"baseline_id"`
	Status        model.TaskStatus    `json:"status"`
	ErrorCode     model.ErrorCode     `json:"error_code,omitempty"`
	Reason        string              `json:"reason,omitempty"`
	DriftCategory model.DriftCategory `json:"drift_category,omitempty"`
	DriftLevel    int                 `json:"drift_level,omitempty"`
	RepairNeeded  bool                `json:"repair_needed"`
}

type AuditReport struct {
	SessionID      string                 `json:"session_id"`
	ProbeBaseURL   string                 `json:"probe_base_url"`
	OrderedKeys    []string               `json:"ordered_keys"`
	Tasks          []TaskReport           `json:"tasks"`
	Degradations   []schedule.Degradation `json:"degradations"`
	QuotaInitial   int                    `json:"quota_initial"`
	QuotaConsumed  int                    `json:"quota_consumed_by_scheduler"`
	QuotaRefunded  int                    `json:"quota_refunded"`
	QuotaRemaining int                    `json:"quota_remaining_scheduler"`
	PeakActive     int                    `json:"peak_active"`
	Summaries      []model.HostSummary    `json:"host_summaries"`
}

func RunAudit(ctx context.Context, dbPath string, opts AuditOptions) (*AuditReport, error) {
	if opts.SessionID == "" {
		opts.SessionID = DefaultSession
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.QueueSize < 0 {
		opts.QueueSize = 64
	}
	if opts.Budget <= 0 {
		opts.Budget = 2 * time.Second
	}
	if opts.ProbeTimeout <= 0 {
		opts.ProbeTimeout = 250 * time.Millisecond
	}
	set := sample.FixedSet()
	st, err := store.Open(dbPath)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	session, err := st.GetSession(opts.SessionID)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, fmt.Errorf("session %s not initialized", opts.SessionID)
	}

	baseURL := opts.ProbeBaseURL
	var endpoint *probe.Endpoint
	if baseURL == "" || opts.UseInProcessEndpoint {
		endpoint = probe.NewTestEndpoint()
		for _, obs := range set.Primary {
			endpoint.SetObservation(obs)
		}
		if opts.EndpointDelayID != "" && opts.EndpointDelay > 0 {
			endpoint.SetDelay(opts.EndpointDelayID, opts.EndpointDelay)
		}
		baseURL = endpoint.URL()
		defer endpoint.Close()
	}
	client := probe.NewClient(baseURL, opts.ProbeTimeout)

	obsByKey := make(map[drift.ConclusionKey][]model.Observation)
	for _, obs := range set.Observations {
		key := drift.ConclusionKey{Host: obs.Host, BaselineID: obs.BaselineID}
		obsByKey[key] = append(obsByKey[key], obs)
	}
	baselineByID := make(map[string]model.Baseline, len(set.Baselines))
	for _, b := range set.Baselines {
		baselineByID[b.ID] = b
	}

	tasks := make([]schedule.Task, 0, len(set.Hosts)*len(set.Baselines))
	for _, host := range set.Hosts {
		for _, b := range set.Baselines {
			host := host
			b := b
			key := host + "/" + b.ID
			tasks = append(tasks, schedule.Task{
				Key: key, Host: host, BaselineID: b.ID, Weight: b.SeverityWeight, RepairCost: b.SeverityWeight,
				Execute: func(taskCtx context.Context) (schedule.Outcome, error) {
					probeObs, probeErr := client.Probe(taskCtx, host, b.ID)
					var probeSource *drift.SourceValue
					if probeErr == nil {
						probeSource = &drift.SourceValue{Source: probeObs.Source, Value: probeObs.Actual}
					} else if !isParentCancellation(probeErr, taskCtx) {
						return schedule.Outcome{}, probeErr
					} else {
						return schedule.Outcome{}, probeErr
					}
					conclusion, evalErr := drift.EvaluateTarget(opts.SessionID, host, b, obsByKey[drift.ConclusionKey{Host: host, BaselineID: b.ID}], probeSource, session.ViewVersion)
					if evalErr != nil {
						return schedule.Outcome{}, evalErr
					}
					conclusion.RepairCost = b.SeverityWeight
					return schedule.Outcome{Conclusion: conclusion, RepairNeeded: conclusion.DriftLevel > 0}, nil
				},
			})
		}
	}

	report, err := schedule.RunRound(ctx, schedule.Config{
		MaxConcurrency: opts.Concurrency, QueueSize: opts.QueueSize,
		RoundBudget: opts.Budget, RepairQuota: session.RepairQuotaRemaining,
	}, tasks)
	if err != nil {
		return nil, err
	}

	taskReports := make([]TaskReport, 0, len(report.Results))
	for _, result := range report.Results {
		status := result.Status
		tr := TaskReport{
			Key: result.Task.Key, Host: result.Task.Host, BaselineID: result.Task.BaselineID,
			Status: status, ErrorCode: result.ErrorCode, Reason: result.DegradationReason,
		}
		var conclusion *model.Conclusion
		if result.Outcome != nil {
			c := result.Outcome.Conclusion
			c.Status = status
			conclusion = &c
			tr.DriftCategory = result.Outcome.Conclusion.DriftCategory
			tr.DriftLevel = result.Outcome.Conclusion.DriftLevel
			tr.RepairNeeded = result.QuotaCharged
		}
		code := result.ErrorCode
		if status != model.TaskDone {
			if code == "" {
				if status == model.TaskDeferred {
					code = "E_AUDIT_DEFERRED"
				} else {
					code = "E_AUDIT_EXECUTION"
				}
			}
			tr.ErrorCode = code
		}
		if _, err := st.CommitTaskResult(store.TaskCommit{
			SessionID: opts.SessionID, Host: result.Task.Host, BaselineID: result.Task.BaselineID,
			Status: status, Conclusion: conclusion, ErrorCode: code, Reason: result.DegradationReason,
			RepairNeeded: result.QuotaCharged && status == model.TaskDone,
		}); err != nil {
			return nil, err
		}
		taskReports = append(taskReports, tr)
	}
	summaries, err := st.ListHostSummaries(opts.SessionID)
	if err != nil {
		return nil, err
	}
	return &AuditReport{
		SessionID: opts.SessionID, ProbeBaseURL: baseURL, OrderedKeys: report.OrderedKeys,
		Tasks: taskReports, Degradations: report.Degradations, QuotaInitial: report.QuotaInitial,
		QuotaConsumed: report.QuotaConsumed, QuotaRefunded: report.QuotaRefunded,
		QuotaRemaining: report.QuotaRemaining, PeakActive: report.PeakActive, Summaries: summaries,
	}, nil
}

func isParentCancellation(err error, ctx context.Context) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil
}
