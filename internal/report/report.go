package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"supplyaudit/internal/merge"
	"supplyaudit/internal/model"
	"supplyaudit/internal/pipeline"
	"supplyaudit/internal/snapshot"
)

const (
	ReportSchemaVersion = 1
	ToolVersion         = "0.1.0"
	SampleTimestamp     = "2000-01-01T00:00:00Z"
	TimePolicy          = "fixed-sample-marker"
)

type Configuration struct {
	FormatVersion          string                  `json:"format_version"`
	AcceptedFormats        []string                `json:"accepted_formats"`
	RequiredFormats        []string                `json:"required_formats"`
	Priority               []string                `json:"priority"`
	MaxConcurrency         int                     `json:"max_concurrency"`
	MaxComponentsPerSource int                     `json:"max_components_per_source"`
	Scheduler              pipeline.SchedulerStats `json:"scheduler"`
}

type SourceSummary struct {
	ID               string             `json:"id"`
	Path             string             `json:"path"`
	Format           model.SourceFormat `json:"format"`
	FormatVersion    string             `json:"format_version"`
	BytesRead        int64              `json:"bytes_read"`
	ParsedCount      int                `json:"parsed_count"`
	DeclarationCount int                `json:"declaration_count"`
	ComponentCount   int                `json:"component_count"`
	DigestAlgorithms []string           `json:"digest_algorithms"`
	BudgetStopped    bool               `json:"budget_stopped"`
	Codes            []string           `json:"codes"`
	IssueCount       int                `json:"issue_count"`
	Issues           []model.ParseIssue `json:"issues"`
}

type CodeCount struct {
	Code  string `json:"code"`
	Count int    `json:"count"`
}

type Counts struct {
	SourceFiles        int         `json:"source_files"`
	SourceDeclarations int         `json:"source_declarations"`
	MergedDeclarations int         `json:"merged_declarations"`
	Components         int         `json:"components"`
	MergeGroups        int         `json:"merge_groups"`
	SourceIssues       int         `json:"source_issues"`
	BusinessFindings   int         `json:"business_findings"`
	SelfConsistent     bool        `json:"self_consistent"`
	ConsistencyMessage string      `json:"consistency_message"`
	Codes              []CodeCount `json:"codes"`
}

type AuditData struct {
	ReportSchemaVersion int                    `json:"report_schema_version"`
	ToolVersion         string                 `json:"tool_version"`
	SampleTimestamp     string                 `json:"sample_timestamp"`
	TimePolicy          string                 `json:"time_policy"`
	Configuration       Configuration          `json:"configuration"`
	Counts              Counts                 `json:"counts"`
	Sources             []SourceSummary        `json:"sources"`
	Components          []merge.ComponentView  `json:"components"`
	Findings            []model.Finding        `json:"findings"`
	Conflicts           []model.Finding        `json:"digest_conflicts"`
	Downgrades          []model.Finding        `json:"version_downgrades"`
	WeakDigests         []model.Finding        `json:"weak_digests"`
	SourceCoverage      []merge.SourceCoverage `json:"source_coverage"`
}

type SnapshotSection struct {
	RequestedVersion int             `json:"requested_version,omitempty"`
	CommittedVersion int             `json:"committed_version,omitempty"`
	Status           string          `json:"status"`
	Error            string          `json:"error,omitempty"`
	Chain            []snapshot.Meta `json:"chain"`
}

type Report struct {
	AuditData
	Snapshot SnapshotSection `json:"snapshot"`
}

func BuildAuditData(result pipeline.Result) AuditData {
	config := Configuration{
		FormatVersion:          result.Config.BinaryFormatVersion,
		AcceptedFormats:        result.Config.AcceptedFormats(),
		RequiredFormats:        result.RequiredFormats,
		Priority:               result.Config.PriorityStrings(),
		MaxConcurrency:         result.Config.MaxConcurrency,
		MaxComponentsPerSource: result.Config.MaxComponentsPerSource,
		Scheduler:              result.Stats,
	}
	sources := make([]SourceSummary, 0, len(result.Sources))
	codeCounter := make(map[string]int)
	totalSourceDeclarations := 0
	totalSourceIssues := 0
	for _, source := range result.Sources {
		componentSet := make(map[string]struct{})
		codes := []string{}
		for _, declaration := range source.Declarations {
			componentSet[declaration.ComponentKey] = struct{}{}
		}
		for _, issue := range source.Issues {
			codes = model.AppendUniqueCode(codes, issue.Code)
			codeCounter[issue.Code]++
			totalSourceIssues++
		}
		issues := append([]model.ParseIssue(nil), source.Issues...)
		if issues == nil {
			issues = []model.ParseIssue{}
		}
		algorithms := append([]string(nil), source.DigestAlgorithms...)
		if algorithms == nil {
			algorithms = []string{}
		}
		sources = append(sources, SourceSummary{
			ID:               source.Spec.ID,
			Path:             source.Spec.ReportPath,
			Format:           source.Spec.Format,
			FormatVersion:    source.FormatVersion,
			BytesRead:        source.BytesRead,
			ParsedCount:      source.ParsedCount,
			DeclarationCount: len(source.Declarations),
			ComponentCount:   len(componentSet),
			DigestAlgorithms: algorithms,
			BudgetStopped:    source.BudgetStopped,
			Codes:            model.NormalizeCodes(codes),
			IssueCount:       len(source.Issues),
			Issues:           issues,
		})
		totalSourceDeclarations += len(source.Declarations)
	}
	sort.Slice(sources, func(i, j int) bool {
		if sources[i].Path != sources[j].Path {
			return sources[i].Path < sources[j].Path
		}
		return sources[i].ID < sources[j].ID
	})

	findings := append([]model.Finding(nil), result.Merge.Findings...)
	if findings == nil {
		findings = []model.Finding{}
	}
	conflicts := filterFindings(findings, model.CodeDigestConflict)
	downgrades := filterFindings(findings, model.CodeVersionDowngrade)
	weak := filterFindings(findings, model.CodeWeakDigest)
	for _, finding := range findings {
		codeCounter[finding.Code]++
	}
	codeCounts := make([]CodeCount, 0, len(codeCounter))
	for code, count := range codeCounter {
		codeCounts = append(codeCounts, CodeCount{Code: code, Count: count})
	}
	sort.Slice(codeCounts, func(i, j int) bool { return codeCounts[i].Code < codeCounts[j].Code })

	components := append([]merge.ComponentView(nil), result.Merge.Components...)
	if components == nil {
		components = []merge.ComponentView{}
	}
	sort.Slice(components, func(i, j int) bool { return components[i].ComponentKey < components[j].ComponentKey })
	coverage := append([]merge.SourceCoverage(nil), result.Merge.SourceCoverage...)
	if coverage == nil {
		coverage = []merge.SourceCoverage{}
	}

	return AuditData{
		ReportSchemaVersion: ReportSchemaVersion,
		ToolVersion:         ToolVersion,
		SampleTimestamp:     SampleTimestamp,
		TimePolicy:          TimePolicy,
		Configuration:       config,
		Counts: Counts{
			SourceFiles:        len(result.Sources),
			SourceDeclarations: totalSourceDeclarations,
			MergedDeclarations: result.Merge.DeclarationCount,
			Components:         result.Merge.ComponentCount,
			MergeGroups:        result.Merge.MergeGroupCount,
			SourceIssues:       totalSourceIssues,
			BusinessFindings:   len(findings),
			SelfConsistent:     result.Merge.SelfConsistent,
			ConsistencyMessage: result.Merge.ConsistencyMessage,
			Codes:              codeCounts,
		},
		Sources:        sources,
		Components:     components,
		Findings:       findings,
		Conflicts:      conflicts,
		Downgrades:     downgrades,
		WeakDigests:    weak,
		SourceCoverage: coverage,
	}
}

func Build(data AuditData, section SnapshotSection) Report {
	if section.Chain == nil {
		section.Chain = []snapshot.Meta{}
	}
	return Report{AuditData: data, Snapshot: section}
}

func filterFindings(findings []model.Finding, code string) []model.Finding {
	out := make([]model.Finding, 0)
	for _, finding := range findings {
		if finding.Code == code {
			out = append(out, finding)
		}
	}
	return out
}

func Marshal(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func MarshalAuditData(data AuditData) ([]byte, error) {
	return Marshal(data)
}

func Write(path string, value any) ([]byte, error) {
	data, err := Marshal(value)
	if err != nil {
		return nil, err
	}
	if directory := filepath.Dir(path); directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return nil, err
		}
	}
	temp := path + ".tmp"
	if err := os.WriteFile(temp, data, 0o644); err != nil {
		return nil, err
	}
	if err := os.Rename(temp, path); err != nil {
		_ = os.Remove(temp)
		return nil, err
	}
	return data, nil
}

func ParseAuditData(data []byte) (AuditData, error) {
	var audit AuditData
	if err := json.Unmarshal(data, &audit); err != nil {
		return AuditData{}, fmt.Errorf("snapshot payload is not valid audit data: %w", err)
	}
	return audit, nil
}
