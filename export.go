package tokenforge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// WriteDeterministicJSON writes stable, indented JSON. Struct field order is
// fixed; map keys (when present) are sorted by encoding/json; HTML escaping is
// disabled; no wall-clock metadata is added by this function.
func WriteDeterministicJSON(w io.Writer, value any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func WriteJSONFile(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && filepath.Dir(path) != "." {
		return fmt.Errorf("create report directory: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	if err := WriteDeterministicJSON(f, value); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// TenantReport is the integrable export shape for one tenant. It is built from
// the latest contiguous snapshot and an adaptive family audit.
type TenantReport struct {
	Tenant              Tenant               `json:"tenant"`
	TokenSummaries      []SnapshotToken      `json:"token_summaries"`
	Families            []FamilyAuditEntry   `json:"family_trace"`
	FamilyPath          PathMode             `json:"family_query_path"`
	FamilyThreshold     int                  `json:"family_query_threshold"`
	FamilyIndexReady    bool                 `json:"family_index_ready"`
	FamilyIndexName     string               `json:"family_index_name,omitempty"`
	FamilyMembersDigest string               `json:"family_members_digest"`
	Charges             []ChargeRecord       `json:"quota_deductions"`
	Quota               QuotaState           `json:"quota"`
	Snapshot            SnapshotChainSummary `json:"snapshot_chain"`
}

func (s *Service) ExportTenantReport(ctx context.Context, tenantID string, opts FamilyQueryOptions) (TenantReport, error) {
	tenant, err := s.GetTenant(ctx, tenantID)
	if err != nil {
		return TenantReport{}, err
	}
	chain, replayErr := s.ReplaySnapshots(ctx, tenantID)
	if len(chain.Snapshots) == 0 {
		return TenantReport{}, replayErr
	}
	latest := chain.Snapshots[len(chain.Snapshots)-1]
	audit, err := s.AuditFamilies(ctx, tenantID, opts)
	if err != nil {
		return TenantReport{}, err
	}
	report := TenantReport{
		Tenant:              tenant,
		TokenSummaries:      append([]SnapshotToken(nil), latest.State.Tokens...),
		Families:            audit.Families,
		FamilyPath:          audit.Path,
		FamilyThreshold:     audit.Threshold,
		FamilyIndexReady:    audit.IndexReady,
		FamilyIndexName:     audit.IndexName,
		FamilyMembersDigest: audit.MembersDigest,
		Charges:             append([]ChargeRecord(nil), latest.State.Charges...),
		Quota:               latest.State.Quota,
		Snapshot:            SummarizeSnapshotChain(chain),
	}
	if report.TokenSummaries == nil {
		report.TokenSummaries = []SnapshotToken{}
	}
	if report.Families == nil {
		report.Families = []FamilyAuditEntry{}
	}
	if report.Charges == nil {
		report.Charges = []ChargeRecord{}
	}
	return report, replayErr
}
