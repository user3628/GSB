package workbench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type ScanOptions struct {
	InputDir        string
	DBPath          string
	ReportPath      string
	TargetParser    string
	MaxConcurrency  int
	MaxEvents       int
	WindowTicks     uint64
	FlipWindowTicks uint64
	SnapshotJump    int
	NoCommit        bool
}

func Scan(ctx context.Context, options ScanOptions) (Report, error) {
	target := options.TargetParser
	if target == "" {
		target = "v2"
	}
	if target != "v1" && target != "v2" {
		return Report{}, NewBusinessError(CodeSnapshotVersion, "", 0, fmt.Sprintf("target parser version %q must be v1 or v2", target))
	}
	sources, err := DiscoverSources(options.InputDir)
	if err != nil {
		return Report{}, err
	}
	sessions, err := ParseWindow(ctx, sources, ParseConfig{
		MaxConcurrency: options.MaxConcurrency,
		MaxEvents:      options.MaxEvents,
	})
	if err != nil {
		return Report{}, err
	}
	report, err := BuildReport(sessions, target, CorrelateConfig{
		WindowTicks:     options.WindowTicks,
		FlipWindowTicks: options.FlipWindowTicks,
	})
	if err != nil {
		return Report{}, err
	}
	if !options.NoCommit {
		if options.DBPath == "" {
			return Report{}, fmt.Errorf("snapshot database path is required")
		}
		store, openErr := OpenStore(options.DBPath)
		if openErr != nil {
			return Report{}, openErr
		}
		defer store.Close()
		report, err = store.CommitReport(report, options.SnapshotJump)
		if err != nil {
			return Report{}, err
		}
	}
	if options.ReportPath != "" {
		encoded, encodeErr := PrettyReportBytes(report)
		if encodeErr != nil {
			return Report{}, encodeErr
		}
		if ensureErr := os.MkdirAll(filepath.Dir(options.ReportPath), 0o755); ensureErr != nil {
			return Report{}, ensureErr
		}
		if writeErr := os.WriteFile(options.ReportPath, encoded, 0o644); writeErr != nil {
			return Report{}, writeErr
		}
	}
	return report, nil
}

func BuildReport(sessions []SessionResult, target string, correlateConfig CorrelateConfig) (Report, error) {
	allEvents := make([]Event, 0)
	summaries := make([]SourceSummary, 0, len(sessions))
	for _, session := range sessions {
		allEvents = append(allEvents, session.Events...)
		summaries = append(summaries, session.Summary())
	}
	SortSourceSummaries(summaries)
	transactions, findings := Correlate(allEvents, correlateConfig)
	required, formats := SourceDependencies(sessions)
	compatible := ParserCompatible(target, required)
	compatibility := Compatibility{
		CurrentParserVersion:  target,
		RequiredParserVersion: required,
		Compatible:            compatible,
		FormatVersions:        formats,
	}
	if !compatible {
		compatibility.Code = CodeSnapshotVersion
		compatibility.Detail = fmt.Sprintf("target parser %s cannot load snapshots requiring parser %s", target, required)
	} else {
		compatibility.Detail = fmt.Sprintf("target parser %s is compatible with all observed format versions", target)
	}
	if transactions == nil {
		transactions = []TransactionReport{}
	}
	if findings == nil {
		findings = []Issue{}
	}
	return Report{
		SchemaVersion:       SchemaVersion,
		Tool:                ToolName,
		SampleClock:         SampleClock,
		TargetParserVersion: target,
		Compatibility:       compatibility,
		Sources:             summaries,
		Transactions:        transactions,
		Findings:            findings,
	}, nil
}

func SourceDependencies(sessions []SessionResult) (required string, formats []string) {
	required = "v1"
	formatSet := make(map[string]struct{})
	for _, session := range sessions {
		for _, version := range session.ParserVersions {
			formatSet[session.Format+":"+version] = struct{}{}
			if version == "v2" {
				required = "v2"
			}
		}
	}
	formats = make([]string, 0, len(formatSet))
	for format := range formatSet {
		formats = append(formats, format)
	}
	sort.Strings(formats)
	if formats == nil {
		formats = []string{}
	}
	return required, formats
}

func QuerySnapshotTransaction(store *Store, version int, currentParser string, txid uint32) (TransactionReport, error) {
	report, err := store.LoadSnapshot(version, currentParser)
	if err != nil {
		return TransactionReport{}, err
	}
	for _, transaction := range report.Transactions {
		if transaction.TxID == txid {
			return transaction, nil
		}
	}
	return TransactionReport{}, NewBusinessError(CodeParse, "", 0, fmt.Sprintf("txid %d is not present in snapshot version %d", txid, version))
}
