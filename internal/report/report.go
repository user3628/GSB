package report

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"keylife/internal/adjudicator"
	"keylife/internal/model"
	"keylife/internal/store"
)

type Options struct {
	NowTick   int64
	Threshold int
	Subject   string
}

type Report struct {
	Schema        string              `json:"schema"`
	Workspace     string              `json:"workspace"`
	TimeSample    string              `json:"time_sample"`
	AsOf          model.MonoTime      `json:"as_of"`
	Summary       Summary             `json:"summary"`
	Subjects      []SubjectReport     `json:"subjects"`
	Keys          []KeyReport         `json:"keys"`
	Transitions   []model.Audit       `json:"transitions"`
	Adjudication  AdjudicationReport  `json:"adjudication"`
	SnapshotChain SnapshotChainReport `json:"snapshot_chain"`
	Alerts        []Alert             `json:"alerts"`
}

type Summary struct {
	SubjectCount  int `json:"subject_count"`
	KeyCount      int `json:"key_count"`
	ActiveKeys    int `json:"active_keys"`
	AuditCount    int `json:"audit_count"`
	SnapshotCount int `json:"snapshot_count"`
	AlertCount    int `json:"alert_count"`
}

type SubjectReport struct {
	Subject           string              `json:"subject"`
	Policy            model.SubjectPolicy `json:"policy"`
	KeyCount          int                 `json:"key_count"`
	PhaseCount        map[model.Phase]int `json:"phase_count"`
	AlgorithmFamilies []string            `json:"algorithm_families"`
}

type KeyReport struct {
	Key          model.Key        `json:"key"`
	Metrics      model.KeyMetrics `json:"metrics"`
	LastAuditSeq int64            `json:"last_audit_seq"`
}

type AdjudicationReport struct {
	Threshold     int                         `json:"threshold"`
	InputCount    int                         `json:"input_count"`
	Paths         []adjudicator.PathSelection `json:"paths"`
	DecisionCodes []model.Code                `json:"decision_codes"`
	Digest        string                      `json:"digest"`
	Decisions     []model.Decision            `json:"decisions"`
	AuditTrail    []model.DecisionAudit       `json:"audit_trail"`
}

type SnapshotMeta struct {
	Version     int64          `json:"version"`
	ParentHash  string         `json:"parent_hash"`
	ContentHash string         `json:"content_hash"`
	ChainHash   string         `json:"chain_hash"`
	CreatedAt   model.MonoTime `json:"created_at"`
	TimeSample  string         `json:"time_sample"`
}

type SnapshotChainReport struct {
	Valid              bool           `json:"valid"`
	Code               model.Code     `json:"code"`
	Message            string         `json:"message,omitempty"`
	PhysicalMaxVersion int64          `json:"physical_max_version"`
	RecoveredThrough   int64          `json:"recovered_through"`
	PhysicalVersions   []int64        `json:"physical_versions"`
	Entries            []SnapshotMeta `json:"entries"`
}

type Alert struct {
	Code    model.Code `json:"code"`
	Subject string     `json:"subject,omitempty"`
	KeyID   string     `json:"key_id,omitempty"`
	Detail  string     `json:"detail"`
}

func Build(ctx context.Context, st *store.Store, opts Options) (Report, error) {
	if opts.Threshold < 1 {
		opts.Threshold = 4
	}
	now := model.T(opts.NowTick)
	chain, chainErr := st.LoadChain(ctx)
	physicalVersions, err := st.SnapshotChainVersions(ctx)
	if err != nil {
		return Report{}, err
	}
	policies, err := st.ListPolicies(ctx)
	if err != nil {
		return Report{}, err
	}
	keys, err := st.ListKeys(ctx, "")
	if err != nil {
		return Report{}, err
	}
	audits, err := st.ListAudits(ctx)
	if err != nil {
		return Report{}, err
	}
	if len(chain.Entries) > 0 {
		last := chain.Entries[len(chain.Entries)-1].State
		if last != nil {
			policies = append([]model.SubjectPolicy(nil), last.Policies...)
			keys = append([]model.Key(nil), last.Keys...)
			audits = append([]model.Audit(nil), last.Audits...)
		}
	}
	if opts.Subject != "" {
		keys = filterKeys(keys, opts.Subject)
		audits = filterAudits(audits, opts.Subject)
		policies = filterPolicies(policies, opts.Subject)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Subject == keys[j].Subject {
			return keys[i].ID < keys[j].ID
		}
		return keys[i].Subject < keys[j].Subject
	})
	sort.Slice(audits, func(i, j int) bool {
		if audits[i].Subject != audits[j].Subject {
			return audits[i].Subject < audits[j].Subject
		}
		if audits[i].KeyID != audits[j].KeyID {
			return audits[i].KeyID < audits[j].KeyID
		}
		if audits[i].Seq == audits[j].Seq {
			return audits[i].EventID < audits[j].EventID
		}
		return audits[i].Seq < audits[j].Seq
	})

	keyReports := make([]KeyReport, 0, len(keys))
	for _, key := range keys {
		var lastSeq int64
		for _, audit := range audits {
			if audit.KeyID == key.ID && audit.Seq > lastSeq {
				lastSeq = audit.Seq
			}
		}
		keyReports = append(keyReports, KeyReport{Key: key, Metrics: model.Metrics(key, now), LastAuditSeq: lastSeq})
	}
	subjects := buildSubjects(policies, keys)

	state := adjudicator.State{Now: now, Policies: make(map[string]model.SubjectPolicy), Keys: make(map[string]model.Key)}
	for _, policy := range policies {
		state.Policies[policy.Subject] = policy
	}
	for _, key := range keys {
		state.Keys[key.ID] = key
	}
	requests := make([]model.DecisionRequest, 0, len(keys))
	for _, key := range keys {
		if key.Phase != model.PhaseActive {
			continue
		}
		requests = append(requests, model.DecisionRequest{
			RequestID: "report-use-" + key.ID, Subject: key.Subject, KeyID: key.ID,
			Actor: key.Subject, Operation: "use",
		})
	}
	batch, err := adjudicator.Adjudicate(requests, state, opts.Threshold)
	if err != nil {
		return Report{}, err
	}

	alerts := buildAlerts(chainErr, chain, physicalVersions, keys, policies, now)
	chainReport := SnapshotChainReport{
		Valid: chainErr == nil, Code: model.CodeOf(chainErr), PhysicalMaxVersion: chain.PhysicalMaxVersion,
		RecoveredThrough: chain.RecoveredThrough, PhysicalVersions: physicalVersions,
		Entries: make([]SnapshotMeta, 0, len(chain.Entries)),
	}
	if chainErr != nil {
		chainReport.Message = chainErr.Error()
	}
	for _, entry := range chain.Entries {
		chainReport.Entries = append(chainReport.Entries, SnapshotMeta{
			Version: entry.Version, ParentHash: entry.ParentHash, ContentHash: entry.ContentHash,
			ChainHash: entry.ChainHash, CreatedAt: entry.CreatedAt, TimeSample: entry.TimeSample,
		})
	}
	active := 0
	for _, key := range keys {
		if key.Phase == model.PhaseActive {
			active++
		}
	}
	rep := Report{
		Schema: "keylife.diagnostic-report/v1", Workspace: "local-deterministic-sample/v1",
		TimeSample: model.TimeSample, AsOf: now,
		Summary: Summary{
			SubjectCount: len(subjects), KeyCount: len(keys), ActiveKeys: active,
			AuditCount: len(audits), SnapshotCount: len(chain.Entries), AlertCount: len(alerts),
		},
		Subjects: subjects, Keys: keyReports, Transitions: audits,
		Adjudication: AdjudicationReport{
			Threshold: opts.Threshold, InputCount: batch.InputCount, Paths: batch.Paths,
			DecisionCodes: batch.DecisionCodes, Digest: batch.Digest, Decisions: batch.Decisions,
			AuditTrail: batch.AuditTrail,
		},
		SnapshotChain: chainReport, Alerts: alerts,
	}
	return rep, nil
}

func Bytes(rep Report) ([]byte, error) {
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func buildSubjects(policies []model.SubjectPolicy, keys []model.Key) []SubjectReport {
	report := make([]SubjectReport, 0, len(policies))
	for _, policy := range policies {
		item := SubjectReport{Subject: policy.Subject, Policy: policy, PhaseCount: make(map[model.Phase]int)}
		families := make(map[string]struct{})
		for _, key := range keys {
			if key.Subject != policy.Subject {
				continue
			}
			item.KeyCount++
			item.PhaseCount[key.Phase]++
			families[key.Family] = struct{}{}
		}
		for family := range families {
			item.AlgorithmFamilies = append(item.AlgorithmFamilies, family)
		}
		sort.Strings(item.AlgorithmFamilies)
		report = append(report, item)
	}
	sort.Slice(report, func(i, j int) bool { return report[i].Subject < report[j].Subject })
	return report
}

func buildAlerts(chainErr error, chain store.ChainResult, physical []int64, keys []model.Key, policies []model.SubjectPolicy, now model.MonoTime) []Alert {
	alerts := make([]Alert, 0)
	if chainErr != nil {
		alerts = append(alerts, Alert{
			Code: model.CodeOf(chainErr), Detail: fmt.Sprintf("%s; recovered_through=%d physical_max=%d", chainErr.Error(), chain.RecoveredThrough, chain.PhysicalMaxVersion),
		})
	}
	policyBySubject := make(map[string]model.SubjectPolicy)
	for _, policy := range policies {
		policyBySubject[policy.Subject] = policy
	}
	for _, key := range keys {
		metrics := model.Metrics(key, now)
		if key.Phase == model.PhaseActive && metrics.Expired {
			alerts = append(alerts, Alert{Code: model.CodeKeyExpired, Subject: key.Subject, KeyID: key.ID, Detail: "active key is past its expiry tick"})
		}
		if key.Phase == model.PhaseActive && metrics.RotationOverdue {
			alerts = append(alerts, Alert{Code: "W_ROTATION_OVERDUE", Subject: key.Subject, KeyID: key.ID, Detail: "rotation window has elapsed"})
		}
		if policy, ok := policyBySubject[key.Subject]; ok && key.StrengthBits < policy.MinStrengthBits {
			alerts = append(alerts, Alert{Code: model.CodeWeakAlgo, Subject: key.Subject, KeyID: key.ID, Detail: "key strength is below current subject policy"})
		}
	}
	if len(physical) > 0 && len(chain.Entries) > 0 && physical[len(physical)-1] != chain.RecoveredThrough && chainErr == nil {
		alerts = append(alerts, Alert{Code: model.CodeSnapshotVersion, Detail: "physical tail differs from recovered chain"})
	}
	sort.Slice(alerts, func(i, j int) bool {
		if alerts[i].Subject != alerts[j].Subject {
			return alerts[i].Subject < alerts[j].Subject
		}
		if alerts[i].KeyID != alerts[j].KeyID {
			return alerts[i].KeyID < alerts[j].KeyID
		}
		if alerts[i].Code != alerts[j].Code {
			return alerts[i].Code < alerts[j].Code
		}
		return alerts[i].Detail < alerts[j].Detail
	})
	return alerts
}

func filterKeys(keys []model.Key, subject string) []model.Key {
	var out []model.Key
	for _, key := range keys {
		if key.Subject == subject {
			out = append(out, key)
		}
	}
	return out
}

func filterAudits(audits []model.Audit, subject string) []model.Audit {
	var out []model.Audit
	for _, audit := range audits {
		if audit.Subject == subject {
			out = append(out, audit)
		}
	}
	return out
}

func filterPolicies(policies []model.SubjectPolicy, subject string) []model.SubjectPolicy {
	var out []model.SubjectPolicy
	for _, policy := range policies {
		if policy.Subject == subject {
			out = append(out, policy)
		}
	}
	return out
}
