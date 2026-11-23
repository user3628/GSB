package report

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"baselineworkbench/internal/codes"
	"baselineworkbench/internal/engine"
	"baselineworkbench/internal/fixed"
	"baselineworkbench/internal/ledger"
	"baselineworkbench/internal/policy"
	"baselineworkbench/internal/snapshot"
)

type ConfigView struct {
	Mode                string `json:"mode"`
	MaxConcurrency      int    `json:"max_concurrency"`
	MaxItems            int    `json:"max_items"`
	MaxErrorBoundMicros int64  `json:"max_error_bound_micros"`
}

type SnapshotView struct {
	Chain           []snapshot.Snapshot `json:"chain"`
	ActiveVersion   int64               `json:"active_version"`
	RecoveryCode    codes.Code          `json:"recovery_code,omitempty"`
	RecoveryMessage string              `json:"recovery_message,omitempty"`
	Committed       bool                `json:"committed"`
	CommitErrorCode codes.Code          `json:"commit_error_code,omitempty"`
	CommitErrorText string              `json:"commit_error_text,omitempty"`
}

type Package struct {
	PackagePath          string              `json:"package_path"`
	SessionID            string              `json:"session_id"`
	Code                 codes.Code          `json:"code"`
	Codes                []codes.Code        `json:"codes"`
	Complete             bool                `json:"complete"`
	Issues               []policy.Issue      `json:"issues"`
	Items                []policy.Item       `json:"items"`
	BaselineItemCount    int                 `json:"baseline_item_count"`
	RepairItemCount      int                 `json:"repair_item_count"`
	EffectiveCount       int                 `json:"effective_item_count"`
	ProcessedItems       int                 `json:"processed_item_count"`
	UnmatchedItems       []string            `json:"unmatched_items"`
	Comparisons          []engine.Comparison `json:"comparisons"`
	ScoreMicros          int64               `json:"deviation_score_micros"`
	ErrorUpperMicros     int64               `json:"error_upper_micros"`
	ScoreInterval        engine.Interval     `json:"score_interval"`
	Deterministic        int                 `json:"deterministic_deviation_count"`
	Uncertain            int                 `json:"uncertain_interval_count"`
	Ledger               ledger.Ledger       `json:"ledger"`
	Snapshot             SnapshotView        `json:"snapshot"`
	RecoveredFromVersion int64               `json:"recovered_from_version,omitempty"`
	RecoveryCode         codes.Code          `json:"recovery_code,omitempty"`
}

type Rank struct {
	Rank             int        `json:"rank"`
	PackagePath      string     `json:"package_path"`
	DeviationScore   int64      `json:"deviation_score_micros"`
	ErrorUpperMicros int64      `json:"error_upper_micros"`
	Code             codes.Code `json:"code"`
}

type Summary struct {
	PackageCount       int    `json:"package_count"`
	CompleteCount      int    `json:"complete_count"`
	FailedCount        int    `json:"failed_count"`
	DeterministicTotal int    `json:"deterministic_deviation_total"`
	UncertainTotal     int    `json:"uncertain_interval_total"`
	Ranked             []Rank `json:"ranked_packages"`
}

type Report struct {
	Schema           string     `json:"schema"`
	SampleMarker     string     `json:"sample_marker"`
	ReferencePackage string     `json:"reference_package"`
	Config           ConfigView `json:"config"`
	Packages         []Package  `json:"packages"`
	Summary          Summary    `json:"summary"`
}

type BuildOptions struct {
	SampleMarker     string
	ReferencePackage string
	Config           engine.Config
	Results          []engine.Result
	Chains           map[string]snapshot.Chain
	CommitErrors     map[string]*codes.BusinessError
}

func Build(options BuildOptions) Report {
	cfg := options.Config.WithDefaults()
	report := Report{
		Schema:           "baseline.workbench.report/v1",
		SampleMarker:     options.SampleMarker,
		ReferencePackage: filepathToSlash(options.ReferencePackage),
		Config: ConfigView{
			Mode:                string(cfg.Mode),
			MaxConcurrency:      cfg.MaxConcurrency,
			MaxItems:            cfg.MaxItems,
			MaxErrorBoundMicros: cfg.MaxErrorBoundMicros,
		},
		Packages: []Package{},
	}
	results := append([]engine.Result{}, options.Results...)
	sort.Slice(results, func(i, j int) bool { return results[i].PackagePath < results[j].PackagePath })
	for _, result := range results {
		pkg := packageView(result, options.Chains[result.PackagePath], options.CommitErrors[result.PackagePath])
		report.Packages = append(report.Packages, pkg)
		if pkg.Complete {
			report.Summary.CompleteCount++
		} else {
			report.Summary.FailedCount++
		}
		report.Summary.DeterministicTotal += pkg.Deterministic
		report.Summary.UncertainTotal += pkg.Uncertain
	}
	report.Summary.PackageCount = len(report.Packages)
	report.Summary.Ranked = ranks(report.Packages)
	return report
}

func packageView(result engine.Result, chain snapshot.Chain, commitErr *codes.BusinessError) Package {
	codeList := append([]codes.Code{}, result.Codes...)
	complete := result.Complete
	if chain.ErrorCode != codes.None {
		codeList = append(codeList, chain.ErrorCode)
		complete = false
	}
	if commitErr != nil {
		codeList = append(codeList, commitErr.Code)
		complete = false
	}
	codeList = codes.SortUnique(codeList)
	items := append([]policy.Item{}, result.Items...)
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	comparisons := append([]engine.Comparison{}, result.Comparisons...)
	sort.Slice(comparisons, func(i, j int) bool { return comparisons[i].ItemID < comparisons[j].ItemID })
	issues := append([]policy.Issue{}, result.Issues...)
	sort.SliceStable(issues, func(i, j int) bool {
		if issues[i].Source != issues[j].Source {
			return issues[i].Source < issues[j].Source
		}
		if issues[i].Offset != issues[j].Offset {
			return issues[i].Offset < issues[j].Offset
		}
		return issues[i].Code < issues[j].Code
	})
	unmatched := append([]string{}, result.UnmatchedItems...)
	sort.Strings(unmatched)
	view := Package{
		PackagePath:          filepathToSlash(result.PackagePath),
		SessionID:            result.SessionID,
		Code:                 codes.Primary(codeList),
		Codes:                codeList,
		Complete:             complete,
		Issues:               issues,
		Items:                items,
		BaselineItemCount:    result.BaselineItemCount,
		RepairItemCount:      result.RepairItemCount,
		EffectiveCount:       result.EffectiveCount,
		ProcessedItems:       result.ProcessedItems,
		UnmatchedItems:       unmatched,
		Comparisons:          comparisons,
		ScoreMicros:          result.ScoreMicros,
		ErrorUpperMicros:     result.ErrorUpperMicros,
		ScoreInterval:        result.ScoreInterval,
		Deterministic:        result.Deterministic,
		Uncertain:            result.Uncertain,
		Ledger:               result.Ledger.Clone(),
		Snapshot:             snapshotView(chain),
		RecoveredFromVersion: result.RecoveredFromVersion,
		RecoveryCode:         result.RecoveryCode,
	}
	if view.Code == codes.None {
		view.Codes = []codes.Code{}
	}
	if view.Issues == nil {
		view.Issues = []policy.Issue{}
	}
	if view.Items == nil {
		view.Items = []policy.Item{}
	}
	if view.Comparisons == nil {
		view.Comparisons = []engine.Comparison{}
	}
	if view.UnmatchedItems == nil {
		view.UnmatchedItems = []string{}
	}
	if view.Ledger.Entries == nil {
		view.Ledger.Entries = []ledger.Entry{}
	}
	if view.ScoreInterval.Unit == "" {
		view.ScoreInterval.Unit = "score_1e-6"
	}
	if commitErr != nil {
		view.Snapshot.CommitErrorCode = commitErr.Code
		view.Snapshot.CommitErrorText = commitErr.Message
	}
	if view.RecoveryCode == codes.None && chain.ErrorCode != codes.None {
		view.RecoveryCode = chain.ErrorCode
	}
	return view
}

func snapshotView(chain snapshot.Chain) SnapshotView {
	view := SnapshotView{
		Chain:           append([]snapshot.Snapshot{}, chain.Snapshots...),
		RecoveryCode:    chain.ErrorCode,
		RecoveryMessage: chain.Message,
	}
	if view.Chain == nil {
		view.Chain = []snapshot.Snapshot{}
	}
	if chain.Active != nil {
		view.ActiveVersion = chain.Active.Version
		view.Committed = true
	}
	return view
}

func ranks(packages []Package) []Rank {
	ordered := append([]Package{}, packages...)
	sort.Slice(ordered, func(i, j int) bool {
		ai, _ := fixed.AbsInt64(ordered[i].ScoreMicros)
		aj, _ := fixed.AbsInt64(ordered[j].ScoreMicros)
		if ai != aj {
			return ai > aj
		}
		return ordered[i].PackagePath < ordered[j].PackagePath
	})
	out := make([]Rank, 0, len(ordered))
	for i, pkg := range ordered {
		out = append(out, Rank{
			Rank:             i + 1,
			PackagePath:      pkg.PackagePath,
			DeviationScore:   pkg.ScoreMicros,
			ErrorUpperMicros: pkg.ErrorUpperMicros,
			Code:             pkg.Code,
		})
	}
	return out
}

func JSONBytes(report Report) ([]byte, error) {
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func TerminalSummary(report Report) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "Baseline Workbench summary\n")
	fmt.Fprintf(&builder, "sample_marker=%s mode=%s reference=%s\n", report.SampleMarker, report.Config.Mode, report.ReferencePackage)
	fmt.Fprintf(&builder, "packages=%d complete=%d failed=%d deterministic=%d uncertain=%d\n\n",
		report.Summary.PackageCount, report.Summary.CompleteCount, report.Summary.FailedCount,
		report.Summary.DeterministicTotal, report.Summary.UncertainTotal)
	builder.WriteString("RANK  PACKAGE                              SCORE          ERROR_UPPER    SNAPSHOT  CODE\n")
	for _, rank := range report.Summary.Ranked {
		pkg := findPackage(report.Packages, rank.PackagePath)
		snapshotVersion := "-"
		if pkg.Snapshot.ActiveVersion > 0 {
			snapshotVersion = fmt.Sprintf("v%d", pkg.Snapshot.ActiveVersion)
		}
		code := string(rank.Code)
		if code == "" {
			code = "OK"
		}
		fmt.Fprintf(&builder, "%4d  %-35s %-14s %-14s %-9s %s\n",
			rank.Rank, rank.PackagePath, fixed.FormatMicros(rank.DeviationScore),
			fixed.FormatMicros(rank.ErrorUpperMicros), snapshotVersion, code)
	}
	return builder.String()
}

func findPackage(packages []Package, path string) Package {
	for _, pkg := range packages {
		if pkg.PackagePath == path {
			return pkg
		}
	}
	return Package{}
}

func filepathToSlash(path string) string {
	return strings.ReplaceAll(path, "\\", "/")
}
