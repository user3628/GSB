package truststore

import (
	"context"
	"encoding/json"
	"sort"
)

// ReportSnapshot augments a snapshot reference with continuity information.
type ReportSnapshot struct {
	SnapshotRef
	Continuous bool `json:"continuous"`
}

// SnapshotChainReport describes the retained version chain and any detected
// continuity failure.
type SnapshotChainReport struct {
	Namespace         string           `json:"namespace"`
	StoredVersion     int64            `json:"stored_version"`
	ContinuousVersion int64            `json:"continuous_version"`
	Code              Code             `json:"code"`
	Versions          []ReportSnapshot `json:"versions"`
}

// ApplyDecision captures a rejected mutation without changing trust state. It
// allows a host or the example CLI to attach a failed attempt to an otherwise
// immutable snapshot report.
type ApplyDecision struct {
	Namespace       string    `json:"namespace"`
	RequestID       string    `json:"request_id"`
	Code            Code      `json:"code"`
	SnapshotVersion int64     `json:"snapshot_version"`
	Detail          string    `json:"detail,omitempty"`
	Changeset       Changeset `json:"changeset"`
}

// Report is the deterministic, reviewable output of the trust component.
type Report struct {
	SchemaVersion   string               `json:"schema_version"`
	Namespace       string               `json:"namespace"`
	SnapshotVersion int64                `json:"snapshot_version"`
	SnapshotChain   SnapshotChainReport  `json:"snapshot_chain"`
	LastApply       *ApplyResult         `json:"last_apply,omitempty"`
	Decision        *ApplyDecision       `json:"decision,omitempty"`
	Chains          []ChainDiff          `json:"chains"`
	Invalidation    *InvalidationClosure `json:"invalidation,omitempty"`
	Contentions     []ContentionRecord   `json:"contentions"`
	Codes           []string             `json:"codes"`
}

// ExportReport builds a report from the latest continuous snapshot. If a
// version gap exists it returns the fallback report together with an
// E_SNAPSHOT_VERSION error; callers may persist the fallback report for review
// but must refuse normal service.
func (s *Store) ExportReport(ctx context.Context, namespace string) (*Report, error) {
	loaded, err := s.loadNamespace(ctx, s.db, namespace)
	if err != nil {
		return nil, err
	}
	continuousVersions := make(map[int64]struct{}, len(loaded.Continuous))
	for _, stored := range loaded.Continuous {
		continuousVersions[stored.Snapshot.Version] = struct{}{}
	}
	versions := make([]ReportSnapshot, 0, len(loaded.Snapshots))
	for _, stored := range loaded.Snapshots {
		_, continuous := continuousVersions[stored.Snapshot.Version]
		versions = append(versions, ReportSnapshot{SnapshotRef: stored.RowRef, Continuous: continuous})
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i].Version < versions[j].Version })

	chainReport := SnapshotChainReport{
		Namespace:         namespace,
		StoredVersion:     loaded.StoredCurrent,
		ContinuousVersion: loaded.ContinuousVersion,
		Code:              CodeOK,
		Versions:          versions,
	}
	if loaded.Health != nil {
		chainReport.Code = loaded.Health.Code
	}
	latest := loaded.Latest.Ref()
	audit := loaded.LatestAudit
	lastApply := audit.Result()
	chains := append([]ChainDiff(nil), lastApply.Chains...)
	sort.Slice(chains, func(i, j int) bool {
		if chains[i].ChainID != chains[j].ChainID {
			return chains[i].ChainID < chains[j].ChainID
		}
		if chains[i].After.Code != chains[j].After.Code {
			return chains[i].After.Code < chains[j].After.Code
		}
		return chains[i].Before.Code < chains[j].Before.Code
	})
	if len(chains) == 0 {
		chains = chainDiffs(VerifyAll(loaded.Latest.Content, VerificationOptions{}), VerifyAll(loaded.Latest.Content, VerificationOptions{}))
	}
	codes := append([]string(nil), lastApply.Codes...)
	if len(codes) == 0 {
		codes = collectCodes(VerifyAll(loaded.Latest.Content, VerificationOptions{}), VerifyAll(loaded.Latest.Content, VerificationOptions{}), nil, CodeOK)
	}
	codeSet := make(map[string]struct{}, len(codes)+1)
	for _, code := range codes {
		codeSet[code] = struct{}{}
	}
	if chainReport.Code != CodeOK {
		codeSet[string(chainReport.Code)] = struct{}{}
	}
	contentions := s.contentionRecords()
	if contentions == nil {
		contentions = []ContentionRecord{}
	}
	for _, record := range contentions {
		codeSet[string(record.Code)] = struct{}{}
	}
	codes = codes[:0]
	for code := range codeSet {
		codes = append(codes, code)
	}
	sort.Strings(codes)

	report := &Report{
		SchemaVersion:   snapshotSchemaVersion,
		Namespace:       namespace,
		SnapshotVersion: latest.Version,
		SnapshotChain:   chainReport,
		LastApply:       &lastApply,
		Chains:          chains,
		Invalidation:    lastApply.Closure,
		Contentions:     contentions,
		Codes:           codes,
	}
	if loaded.Health != nil {
		return report, loaded.Health
	}
	return report, nil
}

// ReportJSON serializes a report with a stable indentation and trailing newline.
func ReportJSON(report *Report) ([]byte, error) {
	if report == nil {
		return nil, NewError(CodeInputInvalid, "encode report", "report is nil", nil)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, NewError(CodeStoreFailure, "encode report", "", err)
	}
	return append(data, '\n'), nil
}

// ExportReportJSON combines report construction and stable JSON encoding. When
// a snapshot gap is detected, both the fallback document and coded error are
// returned.
func (s *Store) ExportReportJSON(ctx context.Context, namespace string) ([]byte, error) {
	report, reportErr := s.ExportReport(ctx, namespace)
	if report == nil {
		return nil, reportErr
	}
	data, encodeErr := ReportJSON(report)
	if encodeErr != nil {
		return nil, encodeErr
	}
	return data, reportErr
}
