package demo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"example.com/tokenforge"
)

const ReportVersion = 1

var injectionOrder = []string{"signature", "replay", "quota_overflow", "snapshot_gap", "contention"}

type InjectionSet map[string]bool

func ParseInjections(value string) (InjectionSet, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" || value == "all" {
		out := InjectionSet{}
		for _, name := range injectionOrder {
			out[name] = true
		}
		return out, nil
	}
	out := InjectionSet{}
	if value == "none" {
		return out, nil
	}
	for _, part := range strings.Split(value, ",") {
		name := strings.TrimSpace(part)
		valid := false
		for _, known := range injectionOrder {
			if name == known {
				valid = true
				break
			}
		}
		if !valid {
			return nil, fmt.Errorf("unknown injection %q (valid: signature,replay,quota_overflow,snapshot_gap,contention,none,all)", name)
		}
		out[name] = true
	}
	return out, nil
}

func (i InjectionSet) names() []string {
	out := make([]string, 0, len(i))
	for _, name := range injectionOrder {
		if i[name] {
			out = append(out, name)
		}
	}
	return out
}

type Options struct {
	WorkDir    string
	Threshold  int
	Injections InjectionSet
}

type Report struct {
	ReportVersion    int                   `json:"report_version"`
	Sample           tokenforge.SampleSpec `json:"sample"`
	Threshold        int                   `json:"family_scan_threshold"`
	Injections       []string              `json:"injections"`
	InjectionResults []InjectionResult     `json:"injection_results"`
	Main             MainReport            `json:"main"`
	PathParity       PathParityReport      `json:"path_parity"`
	SameTokenRace    SameTokenRaceReport   `json:"same_token_race"`
	Contention       *ContentionReport     `json:"contention,omitempty"`
	SnapshotGap      *SnapshotGapReport    `json:"snapshot_gap,omitempty"`
	RollbackRecovery RollbackReport        `json:"rollback_recovery"`
}

type InjectionResult struct {
	Name     string          `json:"name"`
	Observed bool            `json:"observed"`
	Code     tokenforge.Code `json:"code"`
}

type StepReport struct {
	Ordinal           int                 `json:"ordinal"`
	Operation         string              `json:"operation"`
	TenantID          string              `json:"tenant_id"`
	FamilyID          string              `json:"family_id"`
	Code              tokenforge.Code     `json:"code"`
	SnapshotVersion   int64               `json:"snapshot_version"`
	QueryPath         tokenforge.PathMode `json:"query_path,omitempty"`
	Threshold         int                 `json:"threshold,omitempty"`
	ChargedFixed      int64               `json:"charged_fixed"`
	RemainderUnits    int64               `json:"remainder_units"`
	BalanceAfterFixed int64               `json:"balance_after_fixed"`
}

type CodeCount struct {
	Code  tokenforge.Code `json:"code"`
	Count int             `json:"count"`
}

type MainReport struct {
	Steps          []StepReport                      `json:"steps"`
	BusinessCodes  []tokenforge.Code                 `json:"business_code_sequence"`
	CodeCounts     []CodeCount                       `json:"code_counts"`
	Tenants        []tokenforge.TenantReport         `json:"tenants"`
	SnapshotChains []tokenforge.SnapshotChainSummary `json:"snapshot_chains"`
}

type PathRun struct {
	RequestedPath       tokenforge.PathMode           `json:"requested_path"`
	ObservedAuditPath   tokenforge.PathMode           `json:"observed_audit_path"`
	Threshold           int                           `json:"threshold"`
	Codes               []tokenforge.Code             `json:"business_code_sequence"`
	RefreshChargesFixed []int64                       `json:"refresh_charges_fixed"`
	RefreshRemainders   []int64                       `json:"refresh_remainders"`
	TotalChargedFixed   int64                         `json:"total_charged_fixed"`
	TotalRemainderUnits int64                         `json:"total_remainder_units"`
	ErrorBoundFixed     int64                         `json:"error_bound_fixed"`
	FamilyCount         int                           `json:"family_count"`
	FamilyMembersDigest string                        `json:"family_members_digest"`
	Families            []tokenforge.FamilyAuditEntry `json:"family_trace"`
	SnapshotVersions    []int64                       `json:"snapshot_versions"`
	CanonicalSHA256     string                        `json:"canonical_conclusion_sha256"`
}

type PathParityReport struct {
	Precise                  PathRun `json:"precise"`
	Stream                   PathRun `json:"stream"`
	CodesEqual               bool    `json:"business_codes_equal"`
	QuotaDeductionsEqual     bool    `json:"quota_deductions_equal"`
	FamilyConclusionsEqual   bool    `json:"family_conclusions_equal"`
	ConclusionBytesIdentical bool    `json:"conclusion_bytes_identical"`
	CanonicalSHA256          string  `json:"canonical_conclusion_sha256"`
}

type SameTokenRaceReport struct {
	Workers                         int               `json:"workers"`
	CodesSorted                     []tokenforge.Code `json:"codes_sorted"`
	CodeCounts                      []CodeCount       `json:"code_counts"`
	WinnerSnapshotVersion           int64             `json:"winner_snapshot_version"`
	FamilySequenceBeforeReplay      int64             `json:"family_sequence_before_replay"`
	FamilyMembersDigestBeforeReplay string            `json:"family_members_digest_before_replay"`
	PostRaceReplayCode              tokenforge.Code   `json:"post_race_replay_code"`
	PostRaceReplaySnapshotVersion   int64             `json:"post_race_replay_snapshot_version"`
}

type ContentionReport struct {
	MaxInFlightConfigured     int               `json:"max_in_flight_configured"`
	AdmissionWaitNanos        int64             `json:"admission_wait_nanos"`
	CodesSorted               []tokenforge.Code `json:"codes_sorted"`
	ContendedObserved         bool              `json:"contended_observed"`
	QueueObservationObserved  bool              `json:"queue_observation_observed"`
	PeakInFlightWithinLimit   bool              `json:"peak_in_flight_within_limit"`
	QueueDurationExposedByAPI bool              `json:"queue_duration_exposed_by_api"`
	RuntimeDurationsInReport  bool              `json:"runtime_durations_in_report"`
	SnapshotVersions          []int64           `json:"snapshot_versions"`
}

type SnapshotGapReport struct {
	TenantID        string          `json:"tenant_id"`
	SkippedVersion  int64           `json:"skipped_version"`
	StoredVersion   int64           `json:"stored_version"`
	FallbackVersion int64           `json:"fallback_version"`
	DetectedCode    tokenforge.Code `json:"detected_code"`
	Versions        []int64         `json:"contiguous_versions"`
	ContentHashes   []string        `json:"content_hashes"`
	ComparisonCount int             `json:"comparison_count"`
}

type RollbackReport struct {
	Codes                  []tokenforge.Code `json:"codes"`
	VersionAfterAbort      int64             `json:"version_after_abort"`
	VersionAfterRetry      int64             `json:"version_after_retry"`
	ChargedAfterRetryFixed int64             `json:"charged_after_retry_fixed"`
}

type runner struct {
	opts  Options
	spec  tokenforge.SampleSpec
	clock func() time.Time
}

func Run(ctx context.Context, opts Options) (Report, error) {
	if opts.Threshold <= 0 {
		opts.Threshold = 4
	}
	if opts.WorkDir == "" {
		return Report{}, fmt.Errorf("work directory is required")
	}
	if opts.Injections == nil {
		parsed, err := ParseInjections("all")
		if err != nil {
			return Report{}, err
		}
		opts.Injections = parsed
	}
	if err := os.MkdirAll(opts.WorkDir, 0o755); err != nil {
		return Report{}, fmt.Errorf("create work directory: %w", err)
	}
	spec := tokenforge.DefaultSampleSpec()
	r := &runner{
		opts: opts, spec: spec,
		clock: func() time.Time { return time.Unix(0, spec.ClockUnixNano).UTC() },
	}
	report := Report{
		ReportVersion: ReportVersion,
		Sample:        spec,
		Threshold:     opts.Threshold,
		Injections:    opts.Injections.names(),
	}
	mainReport, err := r.runMain(ctx)
	if err != nil {
		return Report{}, err
	}
	report.Main = mainReport
	parity, err := r.runPathParity(ctx)
	if err != nil {
		return Report{}, err
	}
	report.PathParity = parity
	race, err := r.runSameTokenRace(ctx)
	if err != nil {
		return Report{}, err
	}
	report.SameTokenRace = race
	if opts.Injections["snapshot_gap"] {
		gap, err := r.runSnapshotGap(ctx)
		if err != nil {
			return Report{}, err
		}
		report.SnapshotGap = &gap
	}
	if opts.Injections["contention"] {
		contention, err := r.runContention(ctx)
		if err != nil {
			return Report{}, err
		}
		report.Contention = &contention
	}
	rollback, err := r.runRollbackRecovery(ctx)
	if err != nil {
		return Report{}, err
	}
	report.RollbackRecovery = rollback
	report.InjectionResults = buildInjectionResults(report)
	return report, nil
}

func EncodeReport(report Report) ([]byte, error) {
	var buf bytes.Buffer
	if err := tokenforge.WriteDeterministicJSON(&buf, report); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *runner) newService(ctx context.Context, filename string, mutate func(*tokenforge.Config)) (*tokenforge.Service, error) {
	path := filepath.Join(r.opts.WorkDir, filename)
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(path + suffix)
	}
	cfg := tokenforge.Config{
		DatabasePath:               path,
		SigningKey:                 r.spec.SigningKey(),
		AccessTTL:                  time.Duration(r.spec.AccessTTLNanos),
		RefreshTTL:                 time.Duration(r.spec.RefreshTTLNanos),
		DefaultFamilyScanThreshold: r.opts.Threshold,
		MaxInFlight:                16,
		AdmissionWait:              2 * time.Second,
		SQLiteBusyTimeout:          5 * time.Second,
		Clock:                      r.clock,
		IDs:                        tokenforge.NewSequentialIDSource(),
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return tokenforge.NewService(ctx, cfg)
}

func (r *runner) tenantSpec(tenantID string) (tokenforge.SampleTenant, bool) {
	for _, t := range r.spec.Tenants {
		if t.TenantID == tenantID {
			return t, true
		}
	}
	return tokenforge.SampleTenant{}, false
}

func (r *runner) createSampleTenant(ctx context.Context, svc *tokenforge.Service, tenantID string) error {
	t, ok := r.tenantSpec(tenantID)
	if !ok {
		return fmt.Errorf("sample tenant %s not found", tenantID)
	}
	return svc.CreateTenant(ctx, tokenforge.TenantSpec{
		TenantID: t.TenantID, DefaultUserID: t.UserID,
		InitialQuotaFixed: t.InitialQuotaFixed,
		UnitCostFixed:     t.UnitCostFixed,
		UsageFixed:        t.UsageFixed,
	})
}

type mainBuilder struct {
	r     *runner
	svc   *tokenforge.Service
	steps []StepReport
}

func (b *mainBuilder) add(step StepReport) {
	step.Ordinal = len(b.steps) + 1
	b.steps = append(b.steps, step)
}

func (b *mainBuilder) issueBatch(ctx context.Context, tenantID string, count int) ([]tokenforge.IssueResult, error) {
	out := make([]tokenforge.IssueResult, 0, count)
	for i := 0; i < count; i++ {
		res, err := b.svc.Issue(ctx, tokenforge.IssueRequest{TenantID: tenantID})
		code := tokenforge.CodeOf(err)
		step := StepReport{Operation: "issue", TenantID: tenantID, Code: code}
		if err == nil {
			step.FamilyID = res.FamilyID
			step.SnapshotVersion = res.SnapshotVersion
			out = append(out, res)
		}
		b.add(step)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (b *mainBuilder) refresh(ctx context.Context, operation, tenantID, familyID, token string) (tokenforge.RefreshResult, error) {
	res, err := b.svc.Refresh(ctx, tokenforge.RefreshRequest{RefreshToken: token})
	b.add(StepReport{
		Operation: operation, TenantID: tenantID, FamilyID: familyID,
		Code: tokenforge.CodeOf(err), SnapshotVersion: res.SnapshotVersion,
		QueryPath: res.QueryPath, Threshold: res.Threshold,
		ChargedFixed: res.ChargedFixed, RemainderUnits: res.RemainderUnits,
		BalanceAfterFixed: res.BalanceAfterFixed,
	})
	return res, err
}

func (b *mainBuilder) revoke(ctx context.Context, operation, tenantID, familyID string, req tokenforge.RevokeRequest) (tokenforge.RevokeResult, error) {
	res, err := b.svc.Revoke(ctx, req)
	b.add(StepReport{
		Operation: operation, TenantID: tenantID, FamilyID: familyID,
		Code: tokenforge.CodeOf(err), SnapshotVersion: res.SnapshotVersion,
		QueryPath: res.QueryPath, Threshold: res.Threshold,
	})
	return res, err
}

func (r *runner) runMain(ctx context.Context) (MainReport, error) {
	svc, err := r.newService(ctx, "main.db", nil)
	if err != nil {
		return MainReport{}, err
	}
	defer svc.Close()
	b := &mainBuilder{r: r, svc: svc}
	for _, tenantID := range []string{"tenant_small", "tenant_large"} {
		if err := r.createSampleTenant(ctx, svc, tenantID); err != nil {
			return MainReport{}, err
		}
	}
	overflowSelected := r.opts.Injections["quota_overflow"]
	if overflowSelected {
		if err := r.createSampleTenant(ctx, svc, "tenant_overflow"); err != nil {
			return MainReport{}, err
		}
	}
	small, err := b.issueBatch(ctx, "tenant_small", 3)
	if err != nil {
		return MainReport{}, err
	}
	large, err := b.issueBatch(ctx, "tenant_large", 6)
	if err != nil {
		return MainReport{}, err
	}
	var overflow []tokenforge.IssueResult
	if overflowSelected {
		overflow, err = b.issueBatch(ctx, "tenant_overflow", 1)
		if err != nil {
			return MainReport{}, err
		}
	}

	smallRotated1, err := b.refresh(ctx, "small_rotate_1", "tenant_small", small[0].FamilyID, small[0].Pair.Refresh.Value)
	if err != nil {
		return MainReport{}, err
	}
	if _, err := b.refresh(ctx, "small_rotate_2", "tenant_small", small[0].FamilyID, smallRotated1.Pair.Refresh.Value); err != nil {
		return MainReport{}, err
	}
	if _, err := b.revoke(ctx, "small_single_revoke", "tenant_small", small[1].FamilyID, tokenforge.RevokeRequest{
		Token: small[1].Pair.Refresh.Value, Scope: tokenforge.RevokeSingle,
	}); err != nil {
		return MainReport{}, err
	}
	if _, err := b.refresh(ctx, "small_single_revoked_use", "tenant_small", small[1].FamilyID, small[1].Pair.Refresh.Value); tokenforge.CodeOf(err) != tokenforge.CodeTokenRevoked {
		return MainReport{}, fmt.Errorf("expected single token revoked, got %v", err)
	}
	if _, err := b.revoke(ctx, "small_family_revoke", "tenant_small", small[2].FamilyID, tokenforge.RevokeRequest{
		Token: small[2].Pair.Access.Value, Scope: tokenforge.RevokeFamily,
	}); err != nil {
		return MainReport{}, err
	}
	if _, err := b.refresh(ctx, "small_family_revoked_use", "tenant_small", small[2].FamilyID, small[2].Pair.Refresh.Value); tokenforge.CodeOf(err) != tokenforge.CodeTokenRevoked {
		return MainReport{}, fmt.Errorf("expected family token revoked, got %v", err)
	}
	if _, err := b.refresh(ctx, "large_stream_rotate", "tenant_large", large[0].FamilyID, large[0].Pair.Refresh.Value); err != nil {
		return MainReport{}, err
	}
	if r.opts.Injections["replay"] {
		if _, err := b.refresh(ctx, "inject_replay_rotate", "tenant_large", large[1].FamilyID, large[1].Pair.Refresh.Value); err != nil {
			return MainReport{}, err
		}
		if _, err := b.refresh(ctx, "inject_replay", "tenant_large", large[1].FamilyID, large[1].Pair.Refresh.Value); tokenforge.CodeOf(err) != tokenforge.CodeTokenReuse {
			return MainReport{}, fmt.Errorf("expected replay reuse code, got %v", err)
		}
	}
	if r.opts.Injections["signature"] {
		if _, err := b.refresh(ctx, "inject_signature_tamper", "tenant_large", large[2].FamilyID, tamperToken(large[2].Pair.Refresh.Value)); tokenforge.CodeOf(err) != tokenforge.CodeTokenInvalid {
			return MainReport{}, fmt.Errorf("expected invalid signature code, got %v", err)
		}
	}
	if overflowSelected {
		if _, err := b.refresh(ctx, "inject_quota_overflow", "tenant_overflow", overflow[0].FamilyID, overflow[0].Pair.Refresh.Value); tokenforge.CodeOf(err) != tokenforge.CodeQuotaOverflow {
			return MainReport{}, fmt.Errorf("expected quota overflow code, got %v", err)
		}
	}

	codes := make([]tokenforge.Code, 0, len(b.steps))
	for _, step := range b.steps {
		codes = append(codes, step.Code)
	}
	main := MainReport{
		Steps:         b.steps,
		BusinessCodes: codes,
		CodeCounts:    sortedCodeCounts(codes),
	}
	tenantIDs := []string{"tenant_small", "tenant_large"}
	if overflowSelected {
		tenantIDs = append(tenantIDs, "tenant_overflow")
	}
	for _, tenantID := range tenantIDs {
		tenantReport, err := svc.ExportTenantReport(ctx, tenantID, tokenforge.FamilyQueryOptions{Threshold: r.opts.Threshold})
		if err != nil {
			return MainReport{}, fmt.Errorf("export tenant %s: %w", tenantID, err)
		}
		main.Tenants = append(main.Tenants, tenantReport)
		main.SnapshotChains = append(main.SnapshotChains, tenantReport.Snapshot)
	}
	return main, nil
}

func (r *runner) runPathParity(ctx context.Context) (PathParityReport, error) {
	precise, err := r.runPathParityOne(ctx, tokenforge.PathPrecise, "parity_precise.db")
	if err != nil {
		return PathParityReport{}, err
	}
	stream, err := r.runPathParityOne(ctx, tokenforge.PathStream, "parity_stream.db")
	if err != nil {
		return PathParityReport{}, err
	}
	out := PathParityReport{
		Precise: precise, Stream: stream,
		CodesEqual:             reflect.DeepEqual(precise.Codes, stream.Codes),
		QuotaDeductionsEqual:   reflect.DeepEqual(precise.RefreshChargesFixed, stream.RefreshChargesFixed) && precise.TotalRemainderUnits == stream.TotalRemainderUnits,
		FamilyConclusionsEqual: precise.FamilyMembersDigest == stream.FamilyMembersDigest,
		CanonicalSHA256:        precise.CanonicalSHA256,
	}
	out.ConclusionBytesIdentical = precise.CanonicalSHA256 == stream.CanonicalSHA256
	if !out.CodesEqual || !out.QuotaDeductionsEqual || !out.FamilyConclusionsEqual || !out.ConclusionBytesIdentical {
		return out, fmt.Errorf("family query paths disagree")
	}
	return out, nil
}

func (r *runner) runPathParityOne(ctx context.Context, path tokenforge.PathMode, filename string) (PathRun, error) {
	svc, err := r.newService(ctx, filename, nil)
	if err != nil {
		return PathRun{}, err
	}
	defer svc.Close()
	if err := r.createSampleTenant(ctx, svc, "tenant_parity"); err != nil {
		return PathRun{}, err
	}
	codes := make([]tokenforge.Code, 0, 16)
	issued := make([]tokenforge.IssueResult, 0, 5)
	for i := 0; i < 5; i++ {
		res, err := svc.Issue(ctx, tokenforge.IssueRequest{TenantID: "tenant_parity"})
		if err != nil {
			return PathRun{}, err
		}
		issued = append(issued, res)
		codes = append(codes, tokenforge.CodeOK)
	}
	charges := make([]int64, 0, 5)
	remainders := make([]int64, 0, 5)
	var totalCharged, totalRemainder int64
	rotated := make([]tokenforge.RefreshResult, 0, 5)
	for i := range issued {
		res, err := svc.Refresh(ctx, tokenforge.RefreshRequest{
			RefreshToken: issued[i].Pair.Refresh.Value,
			FamilyQuery:  tokenforge.FamilyQueryOptions{Threshold: r.opts.Threshold, ForcePath: path},
		})
		if err != nil {
			return PathRun{}, err
		}
		if res.QueryPath != path {
			return PathRun{}, fmt.Errorf("forced path %s was not observed: %s", path, res.QueryPath)
		}
		codes = append(codes, res.Code)
		charges = append(charges, res.ChargedFixed)
		remainders = append(remainders, res.RemainderUnits)
		totalCharged += res.ChargedFixed
		totalRemainder += res.RemainderUnits
		rotated = append(rotated, res)
	}
	revokeRes, err := svc.Revoke(ctx, tokenforge.RevokeRequest{
		Token: rotated[4].Pair.Refresh.Value, Scope: tokenforge.RevokeSingle,
		FamilyQuery: tokenforge.FamilyQueryOptions{Threshold: r.opts.Threshold, ForcePath: path},
	})
	if err != nil {
		return PathRun{}, err
	}
	codes = append(codes, revokeRes.Code)
	if _, err := svc.Refresh(ctx, tokenforge.RefreshRequest{
		RefreshToken: rotated[4].Pair.Refresh.Value,
		FamilyQuery:  tokenforge.FamilyQueryOptions{Threshold: r.opts.Threshold, ForcePath: path},
	}); tokenforge.CodeOf(err) != tokenforge.CodeTokenRevoked {
		return PathRun{}, fmt.Errorf("expected revoked code, got %v", err)
	} else {
		codes = append(codes, tokenforge.CodeTokenRevoked)
	}
	if _, err := svc.Refresh(ctx, tokenforge.RefreshRequest{
		RefreshToken: issued[3].Pair.Refresh.Value,
		FamilyQuery:  tokenforge.FamilyQueryOptions{Threshold: r.opts.Threshold, ForcePath: path},
	}); tokenforge.CodeOf(err) != tokenforge.CodeTokenReuse {
		return PathRun{}, fmt.Errorf("expected reuse code, got %v", err)
	} else {
		codes = append(codes, tokenforge.CodeTokenReuse)
	}
	audit, err := svc.AuditFamilies(ctx, "tenant_parity", tokenforge.FamilyQueryOptions{
		Threshold: r.opts.Threshold, ForcePath: path,
	})
	if err != nil {
		return PathRun{}, err
	}
	chain, err := svc.ReplaySnapshots(ctx, "tenant_parity")
	if err != nil {
		return PathRun{}, err
	}
	tenant, err := svc.GetTenant(ctx, "tenant_parity")
	if err != nil {
		return PathRun{}, err
	}
	versions := make([]int64, 0, len(chain.Snapshots))
	for _, snapshot := range chain.Snapshots {
		versions = append(versions, snapshot.Version)
	}
	canonical := struct {
		Codes               []tokenforge.Code             `json:"codes"`
		Charges             []int64                       `json:"charges"`
		Remainders          []int64                       `json:"remainders"`
		FamilyMembersDigest string                        `json:"family_members_digest"`
		Families            []tokenforge.FamilyAuditEntry `json:"families"`
		SnapshotVersions    []int64                       `json:"snapshot_versions"`
		Quota               tokenforge.QuotaState         `json:"quota"`
	}{
		Codes: codes, Charges: charges, Remainders: remainders,
		FamilyMembersDigest: audit.MembersDigest, Families: audit.Families,
		SnapshotVersions: versions, Quota: tenant.Quota,
	}
	canonicalBytes, err := json.Marshal(canonical)
	if err != nil {
		return PathRun{}, err
	}
	return PathRun{
		RequestedPath: path, ObservedAuditPath: audit.Path, Threshold: audit.Threshold,
		Codes: codes, RefreshChargesFixed: charges, RefreshRemainders: remainders,
		TotalChargedFixed: totalCharged, TotalRemainderUnits: totalRemainder,
		ErrorBoundFixed: tenant.Quota.ErrorBook.UpperBoundFixed,
		FamilyCount:     audit.FamilyCount, FamilyMembersDigest: audit.MembersDigest,
		Families: audit.Families, SnapshotVersions: versions,
		CanonicalSHA256: sha256Hex(canonicalBytes),
	}, nil
}

func (r *runner) runSameTokenRace(ctx context.Context) (SameTokenRaceReport, error) {
	var once sync.Once
	svc, err := r.newService(ctx, "race.db", func(cfg *tokenforge.Config) {
		cfg.BeforeCommit = func(_ context.Context, info tokenforge.CommitInfo) error {
			if info.Operation == "refresh" {
				once.Do(func() { time.Sleep(500 * time.Millisecond) })
			}
			return nil
		}
	})
	if err != nil {
		return SameTokenRaceReport{}, err
	}
	defer svc.Close()
	if err := r.createSampleTenant(ctx, svc, "tenant_concurrent"); err != nil {
		return SameTokenRaceReport{}, err
	}
	issued, err := svc.Issue(ctx, tokenforge.IssueRequest{TenantID: "tenant_concurrent"})
	if err != nil {
		return SameTokenRaceReport{}, err
	}
	const workers = 8
	codes := make([]tokenforge.Code, workers)
	versions := make([]int64, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			res, err := svc.Refresh(ctx, tokenforge.RefreshRequest{RefreshToken: issued.Pair.Refresh.Value})
			codes[index] = tokenforge.CodeOf(err)
			versions[index] = res.SnapshotVersion
		}(i)
	}
	close(start)
	wg.Wait()
	counts := map[tokenforge.Code]int{}
	var winnerVersion int64
	for i, code := range codes {
		counts[code]++
		if code == tokenforge.CodeOK {
			winnerVersion = versions[i]
		}
	}
	if counts[tokenforge.CodeOK] != 1 || counts[tokenforge.CodeTokenConsumed] != workers-1 {
		return SameTokenRaceReport{}, fmt.Errorf("same-token race was not isolated: %+v", counts)
	}
	audit, err := svc.AuditFamilies(ctx, "tenant_concurrent", tokenforge.FamilyQueryOptions{Threshold: r.opts.Threshold})
	if err != nil {
		return SameTokenRaceReport{}, err
	}
	sequence := int64(0)
	if len(audit.Families) == 1 {
		sequence = audit.Families[0].CurrentSequence
	}
	replay, err := svc.Refresh(ctx, tokenforge.RefreshRequest{RefreshToken: issued.Pair.Refresh.Value})
	if tokenforge.CodeOf(err) != tokenforge.CodeTokenReuse {
		return SameTokenRaceReport{}, fmt.Errorf("post-race replay code=%v", tokenforge.CodeOf(err))
	}
	return SameTokenRaceReport{
		Workers: workers, CodesSorted: sortedCodes(codes), CodeCounts: sortedCodeCounts(codes),
		WinnerSnapshotVersion: winnerVersion, FamilySequenceBeforeReplay: sequence,
		FamilyMembersDigestBeforeReplay: audit.MembersDigest,
		PostRaceReplayCode:              tokenforge.CodeTokenReuse, PostRaceReplaySnapshotVersion: replay.SnapshotVersion,
	}, nil
}

func (r *runner) runContention(ctx context.Context) (ContentionReport, error) {
	const wait = 40 * time.Millisecond
	entered := make(chan struct{})
	var once sync.Once
	svc, err := r.newService(ctx, "contention.db", func(cfg *tokenforge.Config) {
		cfg.MaxInFlight = 1
		cfg.AdmissionWait = wait
		cfg.BeforeCommit = func(_ context.Context, info tokenforge.CommitInfo) error {
			if info.Operation == "refresh" {
				once.Do(func() { close(entered) })
				time.Sleep(250 * time.Millisecond)
			}
			return nil
		}
	})
	if err != nil {
		return ContentionReport{}, err
	}
	defer svc.Close()
	if err := r.createSampleTenant(ctx, svc, "tenant_concurrent"); err != nil {
		return ContentionReport{}, err
	}
	first, err := svc.Issue(ctx, tokenforge.IssueRequest{TenantID: "tenant_concurrent"})
	if err != nil {
		return ContentionReport{}, err
	}
	second, err := svc.Issue(ctx, tokenforge.IssueRequest{TenantID: "tenant_concurrent"})
	if err != nil {
		return ContentionReport{}, err
	}
	firstErr := make(chan error, 1)
	go func() {
		_, err := svc.Refresh(ctx, tokenforge.RefreshRequest{RefreshToken: first.Pair.Refresh.Value})
		firstErr <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		return ContentionReport{}, fmt.Errorf("contention first refresh did not enter transaction")
	}
	_, contentionErr := svc.Refresh(ctx, tokenforge.RefreshRequest{RefreshToken: second.Pair.Refresh.Value})
	if tokenforge.CodeOf(contentionErr) != tokenforge.CodeQuotaContention {
		return ContentionReport{}, fmt.Errorf("expected contention code, got %v", contentionErr)
	}
	if err := <-firstErr; err != nil {
		return ContentionReport{}, err
	}
	metrics := svc.Metrics()
	chain, err := svc.ReplaySnapshots(ctx, "tenant_concurrent")
	if err != nil {
		return ContentionReport{}, err
	}
	versions := make([]int64, 0, len(chain.Snapshots))
	for _, snapshot := range chain.Snapshots {
		versions = append(versions, snapshot.Version)
	}
	codes := []tokenforge.Code{tokenforge.CodeOK, tokenforge.CodeQuotaContention}
	return ContentionReport{
		MaxInFlightConfigured: 1, AdmissionWaitNanos: wait.Nanoseconds(),
		CodesSorted:       sortedCodes(codes),
		ContendedObserved: metrics.Contended >= 1, QueueObservationObserved: metrics.QueueObservations > 0,
		PeakInFlightWithinLimit:   metrics.PeakInFlight <= 1,
		QueueDurationExposedByAPI: true, RuntimeDurationsInReport: false,
		SnapshotVersions: versions,
	}, nil
}

func (r *runner) runSnapshotGap(ctx context.Context) (SnapshotGapReport, error) {
	svc, err := r.newService(ctx, "snapshot_gap.db", nil)
	if err != nil {
		return SnapshotGapReport{}, err
	}
	defer svc.Close()
	if err := r.createSampleTenant(ctx, svc, "tenant_gap"); err != nil {
		return SnapshotGapReport{}, err
	}
	first, err := svc.Issue(ctx, tokenforge.IssueRequest{TenantID: "tenant_gap"})
	if err != nil {
		return SnapshotGapReport{}, err
	}
	second, err := svc.Refresh(ctx, tokenforge.RefreshRequest{RefreshToken: first.Pair.Refresh.Value})
	if err != nil {
		return SnapshotGapReport{}, err
	}
	third, err := svc.Refresh(ctx, tokenforge.RefreshRequest{RefreshToken: second.Pair.Refresh.Value})
	if err != nil {
		return SnapshotGapReport{}, err
	}
	svc.InjectNextSnapshotGap()
	gapped, err := svc.Refresh(ctx, tokenforge.RefreshRequest{RefreshToken: third.Pair.Refresh.Value})
	if err != nil || gapped.SnapshotVersion != 5 {
		return SnapshotGapReport{}, fmt.Errorf("gap refresh: version=%d err=%v", gapped.SnapshotVersion, err)
	}
	chain, err := svc.ReplaySnapshots(ctx, "tenant_gap")
	if tokenforge.CodeOf(err) != tokenforge.CodeSnapshotVersion {
		return SnapshotGapReport{}, fmt.Errorf("gap was not detected: %v", err)
	}
	versions := make([]int64, 0, len(chain.Snapshots))
	hashes := make([]string, 0, len(chain.Snapshots))
	for _, snapshot := range chain.Snapshots {
		versions = append(versions, snapshot.Version)
		hashes = append(hashes, snapshot.ContentHash)
	}
	return SnapshotGapReport{
		TenantID: "tenant_gap", SkippedVersion: 4, StoredVersion: chain.StoredVersion,
		FallbackVersion: chain.LatestContiguousVersion, DetectedCode: tokenforge.CodeSnapshotVersion,
		Versions: versions, ContentHashes: hashes, ComparisonCount: len(chain.Comparisons),
	}, nil
}

func (r *runner) runRollbackRecovery(ctx context.Context) (RollbackReport, error) {
	var fail atomic.Bool
	fail.Store(true)
	svc, err := r.newService(ctx, "rollback.db", func(cfg *tokenforge.Config) {
		cfg.BeforeCommit = func(_ context.Context, info tokenforge.CommitInfo) error {
			if info.Operation == "refresh" && fail.Load() {
				return fmt.Errorf("injected transaction abort")
			}
			return nil
		}
	})
	if err != nil {
		return RollbackReport{}, err
	}
	defer svc.Close()
	if err := r.createSampleTenant(ctx, svc, "tenant_rollback"); err != nil {
		return RollbackReport{}, err
	}
	first, err := svc.Issue(ctx, tokenforge.IssueRequest{TenantID: "tenant_rollback"})
	if err != nil {
		return RollbackReport{}, err
	}
	_, abortErr := svc.Refresh(ctx, tokenforge.RefreshRequest{RefreshToken: first.Pair.Refresh.Value})
	if tokenforge.CodeOf(abortErr) != tokenforge.CodeAborted {
		return RollbackReport{}, fmt.Errorf("expected abort, got %v", abortErr)
	}
	afterAbort, err := svc.ReplaySnapshots(ctx, "tenant_rollback")
	if err != nil {
		return RollbackReport{}, err
	}
	fail.Store(false)
	retry, err := svc.Refresh(ctx, tokenforge.RefreshRequest{RefreshToken: first.Pair.Refresh.Value})
	if err != nil {
		return RollbackReport{}, err
	}
	return RollbackReport{
		Codes:             []tokenforge.Code{tokenforge.CodeAborted, tokenforge.CodeOK},
		VersionAfterAbort: afterAbort.LatestContiguousVersion, VersionAfterRetry: retry.SnapshotVersion,
		ChargedAfterRetryFixed: retry.ChargedFixed,
	}, nil
}

func buildInjectionResults(report Report) []InjectionResult {
	find := func(op string) (bool, tokenforge.Code) {
		for _, step := range report.Main.Steps {
			if step.Operation == op {
				return true, step.Code
			}
		}
		return false, ""
	}
	out := make([]InjectionResult, 0, 5)
	for _, name := range injectionOrder {
		if !contains(report.Injections, name) {
			continue
		}
		result := InjectionResult{Name: name}
		switch name {
		case "signature":
			result.Observed, result.Code = find("inject_signature_tamper")
		case "replay":
			result.Observed, result.Code = find("inject_replay")
		case "quota_overflow":
			result.Observed, result.Code = find("inject_quota_overflow")
		case "snapshot_gap":
			if report.SnapshotGap != nil {
				result.Observed = true
				result.Code = report.SnapshotGap.DetectedCode
			}
		case "contention":
			if report.Contention != nil {
				result.Observed = true
				for _, c := range report.Contention.CodesSorted {
					if c == tokenforge.CodeQuotaContention {
						result.Code = c
					}
				}
			}
		}
		out = append(out, result)
	}
	return out
}

func contains(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}

func sortedCodeCounts(codes []tokenforge.Code) []CodeCount {
	counts := map[tokenforge.Code]int{}
	for _, code := range codes {
		counts[code]++
	}
	out := make([]CodeCount, 0, len(counts))
	for code, count := range counts {
		out = append(out, CodeCount{Code: code, Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out
}

func sortedCodes(codes []tokenforge.Code) []tokenforge.Code {
	out := append([]tokenforge.Code(nil), codes...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func tamperToken(raw string) string {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 || parts[2] == "" {
		return raw + "x"
	}
	repl := byte('A')
	if parts[2][0] == 'A' {
		repl = 'B'
	}
	parts[2] = string(repl) + parts[2][1:]
	return strings.Join(parts, ".")
}
