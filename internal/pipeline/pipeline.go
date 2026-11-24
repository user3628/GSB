package pipeline

import (
	"fmt"
	"sort"
	"strings"

	"supplyaudit/internal/formats"
	"supplyaudit/internal/merge"
	"supplyaudit/internal/model"
)

type Config struct {
	BinaryFormatVersion    string               `json:"format_version"`
	Priority               []model.SourceFormat `json:"priority"`
	MaxConcurrency         int                  `json:"max_concurrency"`
	MaxComponentsPerSource int                  `json:"max_components_per_source"`
}

func DefaultConfig() Config {
	return Config{
		BinaryFormatVersion:    "v2",
		Priority:               []model.SourceFormat{model.FormatBinary, model.FormatJSONL, model.FormatTextBlock},
		MaxConcurrency:         3,
		MaxComponentsPerSource: 1000,
	}
}

func (c *Config) Normalize() error {
	if c.BinaryFormatVersion == "" {
		c.BinaryFormatVersion = "v2"
	}
	c.BinaryFormatVersion = strings.ToLower(strings.TrimSpace(c.BinaryFormatVersion))
	if c.BinaryFormatVersion != "v1" && c.BinaryFormatVersion != "v2" {
		return model.NewCodeError(model.CodeInvalidConfig, "format version must be v1 or v2", 0, 0)
	}
	if len(c.Priority) == 0 {
		c.Priority = []model.SourceFormat{model.FormatBinary, model.FormatJSONL, model.FormatTextBlock}
	}
	if len(c.Priority) != 3 {
		return model.NewCodeError(model.CodeInvalidConfig, "priority must contain binary, jsonl, and textblock exactly once", 0, 0)
	}
	seen := make(map[model.SourceFormat]bool)
	for index, format := range c.Priority {
		if !format.Valid() {
			return model.NewCodeError(model.CodeInvalidConfig, fmt.Sprintf("unknown priority format %q", format), 0, 0)
		}
		if seen[format] {
			return model.NewCodeError(model.CodeInvalidConfig, fmt.Sprintf("priority format %q is duplicated", format), 0, 0)
		}
		seen[format] = true
		c.Priority[index] = model.SourceFormat(strings.ToLower(string(format)))
	}
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = 3
	}
	if c.MaxComponentsPerSource <= 0 {
		c.MaxComponentsPerSource = 1000
	}
	return nil
}

func (c Config) AcceptedFormats() []string {
	accepted := []string{"json/v1", "textblock/v1"}
	if c.BinaryFormatVersion == "v1" {
		accepted = append(accepted, "binary/v1")
	} else {
		accepted = append(accepted, "binary/v1", "binary/v2")
	}
	sort.Strings(accepted)
	return accepted
}

func (c Config) PriorityStrings() []string {
	out := make([]string, len(c.Priority))
	for index, format := range c.Priority {
		out[index] = string(format)
	}
	return out
}

type SchedulerStats struct {
	SourcesStarted   int `json:"sources_started"`
	SourcesCompleted int `json:"sources_completed"`
	PeakInFlight     int `json:"peak_in_flight"`
	PeakQueueLength  int `json:"peak_queue_length"`
	FinalInFlight    int `json:"final_in_flight"`
	FinalQueueLength int `json:"final_queue_length"`
	MaxConcurrency   int `json:"max_concurrency"`
}

type Result struct {
	Sources         []*model.SourceResult `json:"sources"`
	Merge           merge.Result          `json:"merge"`
	Stats           SchedulerStats        `json:"scheduler"`
	Config          Config                `json:"config"`
	RequiredFormats []string              `json:"required_formats"`
}

func Run(specs []model.SourceSpec, config Config) (Result, error) {
	if err := config.Normalize(); err != nil {
		return Result{}, err
	}
	ordered := append([]model.SourceSpec(nil), specs...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].ReportPath != ordered[j].ReportPath {
			return ordered[i].ReportPath < ordered[j].ReportPath
		}
		if ordered[i].ID != ordered[j].ID {
			return ordered[i].ID < ordered[j].ID
		}
		return ordered[i].Path < ordered[j].Path
	})
	usedIDs := make(map[string]struct{}, len(ordered))
	for index := range ordered {
		if ordered[index].ID == "" {
			ordered[index].ID = string(ordered[index].Format) + ":" + ordered[index].ReportPath
		}
		base := ordered[index].ID
		candidate := base
		serial := 2
		for {
			if _, exists := usedIDs[candidate]; !exists {
				break
			}
			candidate = fmt.Sprintf("%s#%d", base, serial)
			serial++
		}
		ordered[index].ID = candidate
		usedIDs[candidate] = struct{}{}
	}

	type outcome struct {
		index  int
		result *model.SourceResult
	}
	parsed := make([]*model.SourceResult, len(ordered))
	stats := SchedulerStats{MaxConcurrency: config.MaxConcurrency}
	options := formats.ParseOptions{BinaryFormatVersion: config.BinaryFormatVersion, MaxComponents: config.MaxComponentsPerSource}
	results := make(chan outcome, len(ordered))
	next := 0
	inFlight := 0
	queueLength := len(ordered)
	completed := 0
	for completed < len(ordered) {
		for inFlight < config.MaxConcurrency && next < len(ordered) {
			index := next
			spec := ordered[index]
			next++
			inFlight++
			stats.SourcesStarted++
			if inFlight > stats.PeakInFlight {
				stats.PeakInFlight = inFlight
			}
			go func() {
				results <- outcome{index: index, result: formats.ParseSource(spec, options)}
			}()
		}
		queueLength = len(ordered) - next
		if queueLength > stats.PeakQueueLength {
			stats.PeakQueueLength = queueLength
		}
		finished := <-results
		inFlight--
		completed++
		stats.SourcesCompleted = completed
		parsed[finished.index] = finished.result
	}
	stats.FinalInFlight = 0
	stats.FinalQueueLength = 0

	for _, source := range parsed {
		if len(source.Declarations) > config.MaxComponentsPerSource {
			firstDropped := source.Declarations[config.MaxComponentsPerSource]
			source.Declarations = source.Declarations[:config.MaxComponentsPerSource]
			source.BudgetStopped = true
			source.AddIssue(model.CodeComponentBudget, fmt.Sprintf("source exceeded component budget %d; retained %d parsed declarations", config.MaxComponentsPerSource, len(source.Declarations)), firstDropped.Offset, source.BytesRead, config.MaxComponentsPerSource)
			source.Finalize()
		}
		for declarationIndex := range source.Declarations {
			declaration := &source.Declarations[declarationIndex]
			declaration.ID = fmt.Sprintf("%s#%d#%02d", declaration.SourceID, declaration.Offset, declarationIndex)
		}
	}

	merged := merge.Run(parsed, merge.Config{Priority: config.Priority})
	required := requiredFormats(parsed)
	return Result{
		Sources:         parsed,
		Merge:           merged,
		Stats:           stats,
		Config:          config,
		RequiredFormats: required,
	}, nil
}

func requiredFormats(sources []*model.SourceResult) []string {
	set := make(map[string]struct{})
	for _, source := range sources {
		if source.FormatVersion == "" {
			continue
		}
		formatName := string(source.Spec.Format)
		if source.Spec.Format == model.FormatJSONL {
			formatName = "json"
		}
		set[formatName+"/v"+source.FormatVersion] = struct{}{}
	}
	required := make([]string, 0, len(set))
	for value := range set {
		required = append(required, value)
	}
	sort.Strings(required)
	return required
}

func (r Result) AllCodes() []string {
	codes := make([]string, 0)
	for _, source := range r.Sources {
		for _, issue := range source.Issues {
			codes = model.AppendUniqueCode(codes, issue.Code)
		}
	}
	for _, finding := range r.Merge.Findings {
		codes = model.AppendUniqueCode(codes, finding.Code)
	}
	return model.NormalizeCodes(codes)
}

func (r Result) HasCode(code string) bool {
	return model.HasCode(r.AllCodes(), code)
}

// BlockingIssueCodes indicate an incomplete or untrustworthy parse. Business
// findings such as digest conflicts and downgrades are committed into snapshots;
// structural parse failures are not allowed to advance the canonical chain by
// default.
func (r Result) BlockingIssueCodes() []string {
	blockingSet := map[string]struct{}{
		model.CodeFormatVersion:   {},
		model.CodeTruncated:       {},
		model.CodeDigestInvalid:   {},
		model.CodeComponentBudget: {},
		model.CodeRecordInvalid:   {},
		model.CodeSourceIO:        {},
	}
	out := []string{}
	for _, source := range r.Sources {
		for _, issue := range source.Issues {
			if _, ok := blockingSet[issue.Code]; ok {
				out = model.AppendUniqueCode(out, issue.Code)
			}
		}
	}
	return model.NormalizeCodes(out)
}
