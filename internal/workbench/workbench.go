package workbench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"svcworkbench/internal/domain"
	"svcworkbench/internal/merge"
	"svcworkbench/internal/samplegen"
	"svcworkbench/internal/snapshot"
	"svcworkbench/internal/source"
)

type Options struct {
	StateDir       string
	TargetFormat   string
	Priorities     []domain.SourceKind
	MaxConcurrency int
	ServiceBudget  int
	Fault          string
	CrashPoint     string
	Crash          func(point string)
}

type Outcome struct {
	Report    *domain.Report
	StateDir  string
	DBPath    string
	Ephemeral bool
}

type ReplayOutput struct {
	Marker           string                 `json:"marker"`
	SchemaVersion    string                 `json:"schema_version"`
	RequestedVersion int                    `json:"requested_version"`
	Snapshot         *snapshot.SnapshotMeta `json:"snapshot,omitempty"`
	Chain            snapshot.ChainState    `json:"chain"`
	Report           json.RawMessage        `json:"report,omitempty"`
	Codes            []string               `json:"codes,omitempty"`
	Detail           string                 `json:"detail,omitempty"`
}

func Run(ctx context.Context, options Options) (*Outcome, error) {
	ephemeral := strings.TrimSpace(options.StateDir) == ""
	options, sources, store, cleanup, err := prepare(ctx, options)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	if options.Fault == samplegen.FaultSnapshotGap {
		if err := prepareSnapshotGap(ctx, store, sources, options); err != nil {
			return nil, err
		}
	}
	config := snapshotConfig(options)
	_, chainState, preflightErr := store.Load(ctx, 0, config)
	var snapshotIssue *domain.Issue
	if preflightErr != nil {
		var business *snapshot.SnapshotError
		if !errors.As(preflightErr, &business) {
			return nil, preflightErr
		}
		issue := domain.Issue{
			Code:     business.Code,
			SourceID: "snapshot-store",
			Offset:   0,
			End:      0,
			Field:    "snapshot_chain",
			Detail:   business.Detail,
		}
		snapshotIssue = &issue
	}
	round, err := executeRound(ctx, sources, options, true)
	if err != nil {
		return nil, err
	}
	report := buildBaseReport(round, options, chainState, snapshotIssue)
	var commitErr error
	if !domain.HasBlockingCode(report.BusinessCodes) {
		payload, marshalErr := json.Marshal(report)
		if marshalErr != nil {
			return nil, marshalErr
		}
		var hook func(string)
		if options.Crash != nil && options.CrashPoint == "commit" {
			hook = options.Crash
		}
		var meta snapshot.SnapshotMeta
		meta, chainState, commitErr = store.Commit(ctx, config, payload, progressFromResults(round.Results), hook)
		if commitErr == nil {
			report.Snapshot = committedSummary(meta, chainState, options)
		}
	}
	if commitErr != nil {
		var business *snapshot.SnapshotError
		if !errors.As(commitErr, &business) {
			return nil, commitErr
		}
		issue := domain.Issue{
			Code:     business.Code,
			SourceID: "snapshot-store",
			Offset:   0,
			End:      0,
			Field:    "snapshot_chain",
			Detail:   business.Detail,
		}
		report.Issues = append(report.Issues, issue)
		report = rebuildReportCodes(report)
		chainState = business.Chain
	}
	if report.Snapshot == nil {
		report.Snapshot = blockedSummary(chainState, options, report.BusinessCodes)
	}
	return &Outcome{
		Report:    report,
		StateDir:  options.StateDir,
		DBPath:    filepath.Join(options.StateDir, "workbench.db"),
		Ephemeral: ephemeral,
	}, nil
}

type roundResult struct {
	Results []domain.SourceResult
	Merged  merge.Adjudication
	Metrics source.BatchMetrics
}

func executeRound(ctx context.Context, sources []source.Input, options Options, crashMerge bool) (roundResult, error) {
	scheduler := source.NewScheduler(options.MaxConcurrency)
	results, metrics := scheduler.ParseAll(ctx, sources, source.Options{ServiceBudget: options.ServiceBudget})
	if crashMerge && options.Crash != nil && options.CrashPoint == "merge" {
		options.Crash("merge")
	}
	adjudication := merge.Adjudicate(results, merge.Options{Priorities: options.Priorities})
	return roundResult{Results: results, Merged: adjudication, Metrics: metrics}, nil
}

func prepare(ctx context.Context, options Options) (Options, []source.Input, *snapshot.Store, func(), error) {
	options.TargetFormat = strings.ToLower(strings.TrimSpace(options.TargetFormat))
	if options.TargetFormat == "" {
		options.TargetFormat = "v2"
	}
	if options.TargetFormat != "v1" && options.TargetFormat != "v2" {
		return options, nil, nil, nil, fmt.Errorf("target format must be v1 or v2")
	}
	if len(options.Priorities) == 0 {
		options.Priorities = append([]domain.SourceKind(nil), domain.DefaultSourcePriorities...)
	}
	seen := make(map[domain.SourceKind]bool)
	for _, kind := range options.Priorities {
		if seen[kind] {
			return options, nil, nil, nil, fmt.Errorf("duplicate source priority %s", kind)
		}
		seen[kind] = true
	}
	if options.MaxConcurrency < 1 {
		options.MaxConcurrency = 3
	}
	if options.ServiceBudget < 1 {
		options.ServiceBudget = 128
	}
	if options.Fault == "" {
		options.Fault = samplegen.FaultNone
	}
	if options.CrashPoint != "" && options.CrashPoint != "merge" && options.CrashPoint != "commit" {
		return options, nil, nil, nil, fmt.Errorf("crash point must be merge or commit")
	}
	if options.StateDir == "" {
		dir, err := os.MkdirTemp("", "svcworkbench-*")
		if err != nil {
			return options, nil, nil, nil, err
		}
		options.StateDir = dir
	}
	if err := os.MkdirAll(options.StateDir, 0o755); err != nil {
		return options, nil, nil, nil, err
	}
	sources, err := samplegen.Generate(filepath.Join(options.StateDir, "samples"), samplegen.Options{
		TargetFormat: options.TargetFormat,
		Fault:        options.Fault,
	})
	if err != nil {
		return options, nil, nil, nil, err
	}
	store, err := snapshot.Open(filepath.Join(options.StateDir, "workbench.db"))
	if err != nil {
		return options, nil, nil, nil, err
	}
	cleanup := func() {
		store.Close()
	}
	return options, sources, store, cleanup, nil
}

func snapshotConfig(options Options) snapshot.Config {
	priorities := make([]string, 0, len(options.Priorities))
	for _, kind := range options.Priorities {
		priorities = append(priorities, string(kind))
	}
	return snapshot.Config{
		SchemaVersion:  domain.SnapshotSchemaVersion,
		Marker:         domain.ReportMarker,
		TargetFormat:   options.TargetFormat,
		SourceVersions: samplegen.FormatVersions(options.TargetFormat),
		Priorities:     priorities,
	}
}

func buildBaseReport(round roundResult, options Options, chain snapshot.ChainState, snapshotIssue *domain.Issue) *domain.Report {
	priorities := make([]string, 0, len(options.Priorities))
	for _, kind := range options.Priorities {
		priorities = append(priorities, string(kind))
	}
	summaries := make([]domain.SourceSummary, 0, len(round.Results))
	issues := make([]domain.Issue, 0)
	for _, result := range round.Results {
		resultIssues := append([]domain.Issue(nil), result.Issues...)
		merge.SortIssues(resultIssues)
		codes := make([]string, 0, len(resultIssues))
		for _, issue := range resultIssues {
			codes = append(codes, string(issue.Code))
		}
		summaries = append(summaries, domain.SourceSummary{
			SourceID:           result.SourceID,
			Kind:               result.Kind,
			FormatVersion:      result.FormatVersion,
			Fingerprint:        result.Fingerprint,
			Bytes:              result.Bytes,
			RecordsSeen:        result.RecordsSeen,
			RecordsAccepted:    result.RecordsAccepted,
			RecordsSkipped:     result.RecordsSkipped,
			UsableDeclarations: result.UsableDeclarations,
			Partial:            result.Partial,
			Codes:              domain.SortedStrings(codes),
			Issues:             resultIssues,
			SkippableRanges:    append([]domain.ByteRange(nil), result.SkippableRanges...),
		})
		issues = append(issues, result.Issues...)
	}
	sort.SliceStable(summaries, func(i, j int) bool { return summaries[i].SourceID < summaries[j].SourceID })
	issues = append(issues, round.Merged.Issues...)
	if snapshotIssue != nil {
		issues = append(issues, *snapshotIssue)
	}
	issues = deduplicateIssues(issues)
	merge.SortIssues(issues)
	codes := make([]string, 0, len(issues))
	for _, issue := range issues {
		codes = append(codes, string(issue.Code))
	}
	report := &domain.Report{
		Marker:          domain.ReportMarker,
		SchemaVersion:   domain.ReportSchemaVersion,
		TargetFormat:    options.TargetFormat,
		Priorities:      priorities,
		SourceSummaries: summaries,
		Services:        round.Merged.Services,
		Issues:          issues,
		BusinessCodes:   domain.SortedStrings(codes),
		Metrics: domain.Metrics{
			MaxConcurrency:      round.Metrics.MaxConcurrency,
			ObservedMaxInFlight: round.Metrics.ObservedMaxInFlight,
			QueuedSources:       round.Metrics.QueuedSources,
			CompletedSources:    round.Metrics.CompletedSources,
			MergePartitions:     len(round.Merged.Services),
			MergeMode:           "serial-service-partition",
		},
	}
	return report
}

func rebuildReportCodes(report *domain.Report) *domain.Report {
	report.Issues = deduplicateIssues(report.Issues)
	merge.SortIssues(report.Issues)
	codes := make([]string, 0, len(report.Issues))
	for _, issue := range report.Issues {
		codes = append(codes, string(issue.Code))
	}
	report.BusinessCodes = domain.SortedStrings(codes)
	return report
}

func deduplicateIssues(issues []domain.Issue) []domain.Issue {
	seen := make(map[string]bool, len(issues))
	out := make([]domain.Issue, 0, len(issues))
	for _, issue := range issues {
		key := strings.Join([]string{
			string(issue.Code), issue.SourceID, string(issue.SourceKind), fmt.Sprint(issue.Offset), fmt.Sprint(issue.End), issue.ServiceName, issue.Field, issue.Detail,
		}, "\x00")
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, issue)
	}
	return out
}

func progressFromResults(results []domain.SourceResult) []snapshot.Progress {
	out := make([]snapshot.Progress, 0, len(results))
	for _, result := range results {
		out = append(out, snapshot.Progress{
			SourceID:      result.SourceID,
			Fingerprint:   result.Fingerprint,
			Kind:          string(result.Kind),
			FormatVersion: result.FormatVersion,
			Accepted:      result.RecordsAccepted,
			Status:        "committed",
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].SourceID < out[j].SourceID })
	return out
}

func committedSummary(meta snapshot.SnapshotMeta, chain snapshot.ChainState, options Options) *domain.SnapshotSummary {
	priorities := make([]string, 0, len(options.Priorities))
	for _, kind := range options.Priorities {
		priorities = append(priorities, string(kind))
	}
	return &domain.SnapshotSummary{
		Committed:      true,
		ActiveVersion:  meta.Version,
		HeadVersion:    meta.Version,
		ChainVersions:  snapshot.SortedVersions(chain.Versions),
		ChainID:        meta.ChainID,
		ContentHash:    meta.ContentHash,
		TargetFormat:   options.TargetFormat,
		FormatVersions: samplegen.FormatVersions(options.TargetFormat),
		Priorities:     priorities,
	}
}

func blockedSummary(chain snapshot.ChainState, options Options, businessCodes []string) *domain.SnapshotSummary {
	priorities := make([]string, 0, len(options.Priorities))
	for _, kind := range options.Priorities {
		priorities = append(priorities, string(kind))
	}
	codes := make([]string, 0)
	for _, value := range businessCodes {
		if value == string(domain.CodeSnapshotVersion) {
			codes = append(codes, value)
		}
	}
	detail := "not committed due to blocking business codes"
	if len(codes) > 0 {
		detail = chain.Detail
		if detail == "" {
			detail = "snapshot chain rejected"
		}
	}
	versions := snapshot.SortedVersions(chain.Versions)
	return &domain.SnapshotSummary{
		Committed:       false,
		ActiveVersion:   chain.ActiveVersion,
		FallbackVersion: chain.FallbackVersion,
		HeadVersion:     chain.HeadVersion,
		ChainVersions:   versions,
		ChainID:         chain.ChainID,
		ContentHash:     chain.HeadHash,
		TargetFormat:    options.TargetFormat,
		FormatVersions:  samplegen.FormatVersions(options.TargetFormat),
		Priorities:      priorities,
		Codes:           domain.SortedStrings(codes),
		Detail:          detail,
	}
}

func prepareSnapshotGap(ctx context.Context, store *snapshot.Store, sources []source.Input, options Options) error {
	config := snapshotConfig(options)
	load, _, err := store.Load(ctx, 0, config)
	if err != nil {
		var business *snapshot.SnapshotError
		if errors.As(err, &business) && business.Code == domain.CodeSnapshotVersion {
			return nil
		}
		return err
	}
	if load == nil {
		round, err := executeRound(ctx, sources, options, false)
		if err != nil {
			return err
		}
		report := buildBaseReport(round, options, snapshot.ChainState{}, nil)
		if domain.HasBlockingCode(report.BusinessCodes) {
			return fmt.Errorf("cannot prepare snapshot-gap baseline because the sample round is blocked")
		}
		payload, err := json.Marshal(report)
		if err != nil {
			return err
		}
		if _, _, err := store.Commit(ctx, config, payload, progressFromResults(round.Results), nil); err != nil {
			return err
		}
	}
	return store.InjectSnapshotGap(ctx)
}

func Replay(ctx context.Context, stateDir string, version int, targetFormat string, priorities []domain.SourceKind) (*ReplayOutput, error) {
	options, _, store, cleanup, err := openExisting(stateDir, targetFormat, priorities)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	config := snapshotConfig(options)
	result, chain, err := store.Load(ctx, version, config)
	if err != nil {
		var business *snapshot.SnapshotError
		if errors.As(err, &business) {
			return &ReplayOutput{
				Marker:           domain.ReportMarker,
				SchemaVersion:    domain.ReportSchemaVersion,
				RequestedVersion: version,
				Chain:            chain,
				Codes:            []string{string(business.Code)},
				Detail:           business.Detail,
			}, nil
		}
		return nil, err
	}
	if result == nil {
		return &ReplayOutput{
			Marker:           domain.ReportMarker,
			SchemaVersion:    domain.ReportSchemaVersion,
			RequestedVersion: version,
			Chain:            chain,
			Codes:            []string{string(domain.CodeSnapshotVersion)},
			Detail:           "no committed snapshot is available",
		}, nil
	}
	meta := result.Meta
	return &ReplayOutput{
		Marker:           domain.ReportMarker,
		SchemaVersion:    domain.ReportSchemaVersion,
		RequestedVersion: meta.Version,
		Snapshot:         &meta,
		Chain:            chain,
		Report:           append(json.RawMessage(nil), result.Payload...),
	}, nil
}

func Trace(ctx context.Context, stateDir string, version int, serviceName, targetFormat string, priorities []domain.SourceKind) (*domain.TraceOutput, error) {
	options, _, store, cleanup, err := openExisting(stateDir, targetFormat, priorities)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	result, chain, err := store.Load(ctx, version, snapshotConfig(options))
	if err != nil {
		var business *snapshot.SnapshotError
		if errors.As(err, &business) {
			return &domain.TraceOutput{
				Marker:        domain.ReportMarker,
				SchemaVersion: domain.ReportSchemaVersion,
				ServiceName:   domain.NormalizeServiceName(serviceName),
				ChainVersions: snapshot.SortedVersions(chain.Versions),
				Codes:         []string{string(business.Code)},
				Detail:        business.Detail,
			}, nil
		}
		return nil, err
	}
	if result == nil {
		return &domain.TraceOutput{
			Marker:        domain.ReportMarker,
			SchemaVersion: domain.ReportSchemaVersion,
			ServiceName:   domain.NormalizeServiceName(serviceName),
			ChainVersions: []int{},
			Codes:         []string{string(domain.CodeSnapshotVersion)},
			Detail:        "no committed snapshot is available",
		}, nil
	}
	var report domain.Report
	if err := json.Unmarshal(result.Payload, &report); err != nil {
		return nil, err
	}
	normalizedName := domain.NormalizeServiceName(serviceName)
	for index := range report.Services {
		if report.Services[index].ServiceName == normalizedName {
			service := report.Services[index]
			return &domain.TraceOutput{
				Marker:          domain.ReportMarker,
				SchemaVersion:   domain.ReportSchemaVersion,
				ServiceName:     normalizedName,
				SnapshotVersion: result.Meta.Version,
				ChainVersions:   snapshot.SortedVersions(chain.Versions),
				Service:         &service,
				Codes:           service.Codes,
			}, nil
		}
	}
	return &domain.TraceOutput{
		Marker:          domain.ReportMarker,
		SchemaVersion:   domain.ReportSchemaVersion,
		ServiceName:     normalizedName,
		SnapshotVersion: result.Meta.Version,
		ChainVersions:   snapshot.SortedVersions(chain.Versions),
		Codes:           []string{"E_SERVICE_NOT_FOUND"},
		Detail:          "service not present in requested snapshot",
	}, nil
}

func openExisting(stateDir, targetFormat string, priorities []domain.SourceKind) (Options, []source.Input, *snapshot.Store, func(), error) {
	if strings.TrimSpace(stateDir) == "" {
		return Options{}, nil, nil, nil, fmt.Errorf("state directory is required")
	}
	options := Options{StateDir: stateDir, TargetFormat: targetFormat, Priorities: priorities}
	options.TargetFormat = strings.ToLower(strings.TrimSpace(options.TargetFormat))
	if options.TargetFormat == "" {
		options.TargetFormat = "v2"
	}
	if options.TargetFormat != "v1" && options.TargetFormat != "v2" {
		return Options{}, nil, nil, nil, fmt.Errorf("target format must be v1 or v2")
	}
	if len(options.Priorities) == 0 {
		options.Priorities = append([]domain.SourceKind(nil), domain.DefaultSourcePriorities...)
	}
	store, err := snapshot.Open(filepath.Join(stateDir, "workbench.db"))
	if err != nil {
		return Options{}, nil, nil, nil, err
	}
	return options, nil, store, func() { store.Close() }, nil
}
